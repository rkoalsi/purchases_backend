"""Cash-aware purchase planning.

Two reports that sit between finance and purchase:

* **Working Capital** — how much cash is actually free to spend on inventory, and
  when. Cash on hand + expected collections − committed outflows, bucketed by
  horizon.
* **Brand Order Plan** — the master report's per-brand demand priced at COGS,
  ranked by GMROI, then funded from the working-capital envelope so each brand
  comes out as ORDER FULL / ORDER PARTIAL / DEFER.

Bank data (`bank_accounts`, `bank_transactions`) is written by `order_form_backend`
crons/webhooks into the same database, so it is read directly here.
"""

from fastapi import APIRouter, HTTPException, Depends, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import asyncio
import io
import logging

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from ..database import get_database
from ..services.master_report import _generate_master_report_data

logger = logging.getLogger(__name__)
router = APIRouter()

BANK_ACCOUNTS = "bank_accounts"
BANK_TRANSACTIONS = "bank_transactions"
INVOICES = "invoices"
BILLS = "bills"
PURCHASE_ORDERS = "purchase_orders"
BRAND_LOGISTICS = "brand_logistics"

# Zoho keeps voided invoices at their full original balance, and drafts are not
# yet receivable. Including either overstates AR by crores.
AR_STATUSES = ("sent", "overdue", "partially_paid")
AP_STATUSES = ("open", "overdue", "partially_paid")

# A credit card's balance is money owed, not money held.
LIABILITY_ACCOUNT_TYPES = ("credit_card",)
CASH_ACCOUNT_TYPES = ("bank", "cash", "payment_clearing", "other_current_asset")

# Internal movement between our own accounts — counting it inflates both sides
# of any in-vs-out view.
INTERNAL_TXN_TYPES = ("transfer_fund",)

AGEING_BUCKETS = [
    ("overdue", None, 0),
    ("0_30", 0, 30),
    ("31_60", 31, 60),
    ("61_90", 61, 90),
    ("90_plus", 91, None),
]

HORIZONS = (30, 60, 90)

# Last-resort fallbacks, used only when the live rate lookup fails and the user
# has not supplied a rate. Kept deliberately conservative.
DEFAULT_FX = {"USD": 86.0, "CNY": 12.0, "EUR": 93.0, "GBP": 110.0, "INR": 1.0}

FX_API_URL = "https://api.frankfurter.dev/v1/latest"
FX_CURRENCIES = ("USD", "CNY", "EUR", "GBP")
FX_CACHE_TTL_SECONDS = 3600

# Module-level cache so a page refresh (and the plan endpoint, which needs the
# same rates) doesn't hit the FX API on every request.
_fx_cache: Dict[str, object] = {"fetched_at": None, "rates": None, "source": None}


async def _fetch_live_fx() -> Dict:
    """Current INR rates for the currencies our POs are raised in.

    Returns ``{"rates": {...}, "source": "live"|"cache"|"fallback", "as_of": ...}``.
    A failure here must never break the report — purchase still needs the plan
    even if the FX host is down, so we degrade to the cached or default rate and
    say so in the payload.
    """
    now = datetime.now()
    fetched_at = _fx_cache.get("fetched_at")
    if (
        isinstance(fetched_at, datetime)
        and _fx_cache.get("rates")
        and (now - fetched_at).total_seconds() < FX_CACHE_TTL_SECONDS
    ):
        return {
            "rates": dict(_fx_cache["rates"]),  # type: ignore[arg-type]
            "source": "cache",
            "as_of": fetched_at.isoformat(),
        }

    try:
        import httpx

        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            resp = await client.get(
                FX_API_URL,
                params={"base": "INR", "symbols": ",".join(FX_CURRENCIES)},
            )
            resp.raise_for_status()
            body = resp.json()

        # The API quotes INR→foreign; we need foreign→INR, so invert.
        rates = {"INR": 1.0}
        for ccy, per_inr in (body.get("rates") or {}).items():
            if per_inr:
                rates[ccy] = round(1.0 / float(per_inr), 4)
        rates["RMB"] = rates.get("CNY", DEFAULT_FX["CNY"])

        if not rates.get("USD"):
            raise ValueError("FX response missing USD")

        _fx_cache["fetched_at"] = now
        _fx_cache["rates"] = rates
        _fx_cache["source"] = "live"
        return {
            "rates": dict(rates),
            "source": "live",
            "as_of": body.get("date") or now.date().isoformat(),
        }
    except Exception as e:
        logger.warning(f"Live FX lookup failed, falling back: {e}")
        cached = _fx_cache.get("rates")
        if cached:
            return {
                "rates": dict(cached),  # type: ignore[arg-type]
                "source": "stale_cache",
                "as_of": fetched_at.isoformat() if isinstance(fetched_at, datetime) else None,
            }
        return {"rates": dict(DEFAULT_FX), "source": "fallback", "as_of": None}


# ─── Helpers ────────────────────────────────────────────────────────────────────

def _f(val, default: float = 0.0) -> float:
    """Zoho returns numerics as strings often enough that this is unavoidable."""
    if val is None or val == "":
        return default
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def _parse_date(val) -> Optional[datetime]:
    """`due_date` is a 'YYYY-MM-DD' string on invoices but a datetime on bills."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val.replace(hour=0, minute=0, second=0, microsecond=0)
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:19] if " " in s else s, fmt)
        except ValueError:
            continue
    return None


def _bucket_for(due: Optional[datetime], today: datetime) -> str:
    """Which ageing bucket a due date falls into. No date = treat as due now,
    which is the conservative choice for a payable and the honest one for a
    receivable we cannot schedule."""
    if due is None:
        return "overdue"
    days = (due - today).days
    if days < 0:
        return "overdue"
    for name, lo, hi in AGEING_BUCKETS[1:]:
        if (lo is None or days >= lo) and (hi is None or days <= hi):
            return name
    return "90_plus"


def _empty_buckets() -> Dict[str, float]:
    return {name: 0.0 for name, _, _ in AGEING_BUCKETS}


def _within(buckets: Dict[str, float], horizon_days: int) -> float:
    """Sum of everything landing on or before `horizon_days`. Overdue is
    included in every horizon — it is already due."""
    total = buckets.get("overdue", 0.0)
    if horizon_days >= 30:
        total += buckets.get("0_30", 0.0)
    if horizon_days >= 60:
        total += buckets.get("31_60", 0.0)
    if horizon_days >= 90:
        total += buckets.get("61_90", 0.0)
    return round(total, 2)


async def _resolve_fx(
    usd_inr: Optional[float], cny_inr: Optional[float]
) -> Dict:
    """Build the currency→INR map, preferring a user-supplied rate over the live one.

    POs are raised in USD and CNY but every figure in these reports is rupees, so
    a rate is always needed. Finance often wants to plan at the rate they expect
    to actually pay at rather than today's spot, hence the override.
    """
    live = await _fetch_live_fx()
    rates = dict(DEFAULT_FX)
    rates.update(live["rates"])

    overrides = {}
    if usd_inr is not None:
        rates["USD"] = usd_inr
        overrides["USD"] = usd_inr
    if cny_inr is not None:
        rates["CNY"] = cny_inr
        rates["RMB"] = cny_inr
        overrides["CNY"] = cny_inr

    return {
        "rates": rates,
        "meta": {
            "source": "manual" if len(overrides) == 2 else live["source"],
            "as_of": live["as_of"],
            "live_rates": {c: live["rates"].get(c) for c in FX_CURRENCIES},
            "overrides": overrides,
            "applied": {"USD": rates.get("USD"), "CNY": rates.get("CNY")},
        },
    }


@router.get("/fx-rates")
async def get_fx_rates():
    """Current foreign-currency→INR rates, for pre-filling the rate inputs."""
    live = await _fetch_live_fx()
    return JSONResponse(status_code=status.HTTP_200_OK, content=live)


# ─── Cash position ──────────────────────────────────────────────────────────────

def _cash_position_sync(db) -> Dict:
    """Net cash from `bank_accounts`, split into holdings and liabilities.

    Zoho carries two different numbers per account and they are not the same
    thing:

    * ``balance`` — the **book** balance, i.e. what has been recorded in Books.
    * ``bank_balance`` — what the **bank feed** says is actually in the account.

    For "how much can we spend", the bank feed is the truth. Yes Bank, for
    example, holds ₹58.5L in the feed while Books shows ₹65 because the
    transactions have not been categorised yet. Using ``balance`` alone hides
    real money.

    Only fed accounts populate ``bank_balance`` (the rest sit at 0), so we take
    it per account when it is non-zero and fall back to the book balance
    otherwise — never zeroing out an unfed account like ICICI.

    Accounts flagged `deleted_in_zoho` are excluded — the cron flags rather than
    deletes them so their transactions aren't orphaned, but they are not live cash.
    """
    accounts = list(
        db.get_collection(BANK_ACCOUNTS).find(
            {"deleted_in_zoho": {"$ne": True}},
            {
                "account_id": 1, "account_name": 1, "account_type": 1,
                "balance": 1, "bank_balance": 1, "uncategorized_transactions": 1,
                "currency_code": 1, "is_active": 1, "_id": 0,
            },
        )
    )

    holdings, liabilities, unreconciled_total = 0.0, 0.0, 0.0
    rows = []
    for a in accounts:
        book = _f(a.get("balance"))
        feed = _f(a.get("bank_balance"))
        has_feed = feed != 0
        effective = feed if has_feed else book
        unreconciled = effective - book if has_feed else 0.0

        atype = a.get("account_type", "") or ""
        is_liability = atype in LIABILITY_ACCOUNT_TYPES
        if is_liability:
            liabilities += effective
        elif atype in CASH_ACCOUNT_TYPES:
            holdings += effective
        unreconciled_total += unreconciled

        rows.append({
            "account_id": a.get("account_id", ""),
            "account_name": a.get("account_name", "") or "",
            "account_type": atype,
            "book_balance": round(book, 2),
            "bank_balance": round(feed, 2),
            "has_feed": has_feed,
            "balance": round(effective, 2),
            "unreconciled": round(unreconciled, 2),
            "uncategorized_transactions": int(_f(a.get("uncategorized_transactions"))),
            "is_liability": is_liability,
            # What this account contributes to net cash.
            "net_contribution": round(-effective if is_liability else effective, 2),
            "currency_code": a.get("currency_code", "INR") or "INR",
            "is_active": bool(a.get("is_active", True)),
        })

    rows.sort(key=lambda r: (r["is_liability"], -abs(r["balance"])))

    return {
        "accounts": rows,
        "holdings": round(holdings, 2),
        "liabilities": round(liabilities, 2),
        "net_cash": round(holdings - liabilities, 2),
        "book_net_cash": round(
            sum(r["book_balance"] for r in rows if not r["is_liability"]
                and r["account_type"] in CASH_ACCOUNT_TYPES)
            - sum(r["book_balance"] for r in rows if r["is_liability"]), 2
        ),
        "unreconciled": round(unreconciled_total, 2),
    }


def _monthly_opex_sync(db, months: int = 3) -> Dict:
    """Trailing average monthly operating outflow.

    Uses `expense` transactions only. `signed_amount` is negative for cash out
    (a credit on the bank account), so the sum is negated to get a positive
    burn figure. Transfers are excluded as internal movement.
    """
    today = datetime.now()
    since = today - timedelta(days=30 * months)

    pipeline = [
        {"$match": {
            "transaction_date": {"$gte": since, "$lte": today},
            "transaction_type": {"$in": ["expense", "tds_payment"]},
        }},
        {"$group": {"_id": None, "net": {"$sum": "$signed_amount"}, "n": {"$sum": 1}}},
    ]
    res = list(db.get_collection(BANK_TRANSACTIONS).aggregate(pipeline))
    net_out = -(res[0]["net"] if res else 0.0)
    count = res[0]["n"] if res else 0

    return {
        "monthly_opex": round(max(0.0, net_out) / months, 2),
        "months_sampled": months,
        "transactions_sampled": count,
    }


def _collection_run_rate_sync(db, days: int = 90) -> Dict:
    """How fast money actually comes in, from `bank_transactions`.

    The invoice ledger tells us exactly *how much* is owed, but not how much of
    it will land inside a planning horizon — plenty of the ₹1.8 Cr overdue book
    has been overdue for months. Rather than asking someone to guess a
    percentage, we measure the rate at which customer payments have genuinely
    been hitting the bank and project that forward.
    """
    today = datetime.now()
    since = today - timedelta(days=days)

    pipeline = [
        {"$match": {
            "transaction_date": {"$gte": since, "$lte": today},
            "transaction_type": {"$in": ["customer_payment", "payment_refund"]},
        }},
        {"$group": {"_id": None, "net": {"$sum": "$signed_amount"}, "n": {"$sum": 1}}},
    ]
    res = list(db.get_collection(BANK_TRANSACTIONS).aggregate(pipeline))
    net_in = res[0]["net"] if res else 0.0
    count = res[0]["n"] if res else 0

    return {
        "daily_collection_rate": round(max(0.0, net_in) / days, 2),
        "window_days": days,
        "collected_in_window": round(net_in, 2),
        "transactions_sampled": count,
    }


def _cashflow_series_sync(db, days: int = 90) -> List[Dict]:
    """Daily cash in / cash out for a trend chart, transfers excluded."""
    today = datetime.now()
    since = today - timedelta(days=days)

    pipeline = [
        {"$match": {
            "transaction_date": {"$gte": since, "$lte": today},
            "transaction_type": {"$nin": list(INTERNAL_TXN_TYPES)},
        }},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$transaction_date"}},
            "inflow": {"$sum": {"$cond": [{"$gt": ["$signed_amount", 0]}, "$signed_amount", 0]}},
            "outflow": {"$sum": {"$cond": [{"$lt": ["$signed_amount", 0]}, "$signed_amount", 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]
    return [
        {
            "date": r["_id"],
            "inflow": round(r["inflow"], 2),
            "outflow": round(-r["outflow"], 2),
            "net": round(r["inflow"] + r["outflow"], 2),
        }
        for r in db.get_collection(BANK_TRANSACTIONS).aggregate(pipeline)
    ]


# ─── Receivables / payables ─────────────────────────────────────────────────────

def _receivables_sync(db) -> Dict:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    buckets = _empty_buckets()
    top: List[Dict] = []

    cursor = db.get_collection(INVOICES).find(
        {"status": {"$in": list(AR_STATUSES)}},
        {"customer_name": 1, "invoice_number": 1, "balance": 1,
         "due_date": 1, "status": 1, "_id": 0},
    )
    by_customer: Dict[str, float] = {}
    for inv in cursor:
        bal = _f(inv.get("balance"))
        if bal <= 0:
            continue
        bucket = _bucket_for(_parse_date(inv.get("due_date")), today)
        buckets[bucket] += bal
        name = inv.get("customer_name", "") or "Unknown"
        by_customer[name] = by_customer.get(name, 0.0) + bal

    top = sorted(
        ({"customer_name": k, "balance": round(v, 2)} for k, v in by_customer.items()),
        key=lambda r: -r["balance"],
    )[:20]

    return {
        "buckets": {k: round(v, 2) for k, v in buckets.items()},
        "total": round(sum(buckets.values()), 2),
        "top_customers": top,
    }


def _payables_sync(db) -> Dict:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    buckets = _empty_buckets()
    by_vendor: Dict[str, float] = {}

    cursor = db.get_collection(BILLS).find(
        {"status": {"$in": list(AP_STATUSES)}},
        {"vendor_name": 1, "bill_number": 1, "balance": 1,
         "due_date": 1, "status": 1, "_id": 0},
    )
    for bill in cursor:
        bal = _f(bill.get("balance"))
        if bal <= 0:
            continue
        buckets[_bucket_for(_parse_date(bill.get("due_date")), today)] += bal
        name = bill.get("vendor_name", "") or "Unknown"
        by_vendor[name] = by_vendor.get(name, 0.0) + bal

    top = sorted(
        ({"vendor_name": k, "balance": round(v, 2)} for k, v in by_vendor.items()),
        key=lambda r: -r["balance"],
    )[:20]

    return {
        "buckets": {k: round(v, 2) for k, v in buckets.items()},
        "total": round(sum(buckets.values()), 2),
        "top_vendors": top,
    }


def _open_po_commitment_sync(db, fx: Dict[str, float]) -> Dict:
    """Value still to be paid on issued purchase orders.

    Open POs are `order_status_formatted == "Issued"` — the same definition the
    master report uses for stock in transit, so the two always agree. Only the
    unreceived portion of each line counts.

    These are foreign-currency orders, so every row carries its own rate and the
    converted total is explicitly labelled rather than silently mixed.
    """
    pos = list(
        db.get_collection(PURCHASE_ORDERS).find(
            {"order_status_formatted": "Issued"},
            {"purchaseorder_number": 1, "vendor_name": 1, "vendor_id": 1,
             "currency_code": 1, "date": 1, "delivery_date": 1,
             "line_items": 1, "_id": 0},
        ).sort([("date", 1), ("purchaseorder_number", 1)])
    )

    rows = []
    total_inr = 0.0
    for po in pos:
        ccy = (po.get("currency_code") or "INR").upper()
        rate = fx.get(ccy, 1.0)
        open_val = 0.0
        for li in po.get("line_items", []):
            qty = _f(li.get("quantity"))
            recd = _f(li.get("quantity_received"))
            remaining = qty - recd
            if remaining > 0:
                open_val += remaining * _f(li.get("rate"))
        if open_val <= 0:
            continue
        inr = open_val * rate
        total_inr += inr
        rows.append({
            "purchaseorder_number": po.get("purchaseorder_number", ""),
            "vendor_name": po.get("vendor_name", "") or "",
            "currency_code": ccy,
            "fx_rate": rate,
            "open_value": round(open_val, 2),
            "open_value_inr": round(inr, 2),
            "date": str(po.get("date", ""))[:10],
            "delivery_date": str(po.get("delivery_date", ""))[:10],
        })

    rows.sort(key=lambda r: -r["open_value_inr"])
    return {"purchase_orders": rows, "total_inr": round(total_inr, 2), "count": len(rows)}


def _fallback_unit_costs_sync(db, fx: Dict[str, float]) -> Dict[str, float]:
    """Latest purchase-order rate per SKU, in INR.

    Needed because COGS comes from inventory valuation, which by construction
    has no unit cost for a SKU that is out of stock — and a stocked-out brand is
    exactly the one most likely to need ordering. Without this its capital
    requirement silently reads as zero and it drops out of the funding queue.

    `product_unit_prices` would be the better source but is not populated, so we
    fall back to what was actually last paid.
    """
    costs: Dict[str, float] = {}
    cursor = db.get_collection(PURCHASE_ORDERS).find(
        {}, {"line_items": 1, "currency_code": 1, "date": 1, "_id": 0}
    ).sort([("date", 1)])

    # Ascending date so later (more recent) POs overwrite earlier ones.
    for po in cursor:
        rate_fx = fx.get((po.get("currency_code") or "INR").upper(), 1.0)
        for li in po.get("line_items", []):
            sku = ""
            for cf in li.get("item_custom_fields", []) or []:
                if cf.get("api_name") == "cf_sku_code":
                    sku = cf.get("value", "") or ""
                    break
            if not sku:
                continue
            rate = _f(li.get("rate"))
            if rate > 0:
                costs[sku] = round(rate * rate_fx, 2)
    return costs


# ─── Working capital ────────────────────────────────────────────────────────────

def _build_working_capital(
    cash: Dict, receivables: Dict, payables: Dict,
    open_pos: Dict, opex: Dict, collection: Dict,
) -> Dict:
    """Assemble the cash bridge into 30/60/90-day projections.

    Expected collections are bounded by two facts, not by a guess: the amount
    actually owed and due inside the horizon (from the invoice ledger), and the
    rate at which money has genuinely been arriving (from the bank). We take the
    lower of the two and report which bound applied, so the number is always
    traceable to real data.
    """
    monthly_opex = opex["monthly_opex"]
    net_cash = cash["net_cash"]
    daily_rate = collection["daily_collection_rate"]
    projections = []

    for horizon in HORIZONS:
        ar_due = _within(receivables["buckets"], horizon)
        run_rate_ar = daily_rate * horizon
        expected_ar = min(ar_due, run_rate_ar)
        due_ap = _within(payables["buckets"], horizon)
        # Open POs have no reliable payment date on the record, so the whole
        # remaining commitment is charged to the nearest horizon. Conservative
        # by design: it never lets the plan overspend.
        po_due = open_pos["total_inr"]
        opex_due = monthly_opex * (horizon / 30.0)
        free = net_cash + expected_ar - due_ap - po_due - opex_due
        projections.append({
            "horizon_days": horizon,
            "opening_cash": round(net_cash, 2),
            "expected_collections": round(expected_ar, 2),
            "ar_due_in_window": round(ar_due, 2),
            "run_rate_capacity": round(run_rate_ar, 2),
            # Which constraint bound the estimate: are we short of invoices to
            # collect, or short of collection speed?
            "collections_limited_by": "invoices_due" if ar_due <= run_rate_ar else "collection_speed",
            "implied_realisation_pct": round(expected_ar / ar_due * 100, 1) if ar_due > 0 else 0.0,
            "bills_due": round(due_ap, 2),
            "open_po_commitment": round(po_due, 2),
            "operating_expenses": round(opex_due, 2),
            "free_cash": round(free, 2),
        })

    return {
        "cash": cash,
        "receivables": receivables,
        "payables": payables,
        "open_purchase_orders": open_pos,
        "opex": opex,
        "collection": collection,
        "projections": projections,
        # Default envelope. The order plan picks a horizon explicitly — orders
        # placed today are paid over a 60–96 day lead time, not within 30 days,
        # so planning against the 30-day figure understates what is affordable.
        "purchase_envelope": projections[-1]["free_cash"],
        "purchase_envelope_horizon": projections[-1]["horizon_days"],
        "generated_at": datetime.now().isoformat(),
    }


@router.get("/working-capital")
async def get_working_capital(
    usd_inr: Optional[float] = Query(
        None, gt=0, description="USD→INR override; omit to use the live rate"
    ),
    cny_inr: Optional[float] = Query(
        None, gt=0, description="CNY/RMB→INR override; omit to use the live rate"
    ),
    opex_months: int = Query(3, ge=1, le=12, description="Months sampled for the opex average"),
    collection_window_days: int = Query(
        90, ge=30, le=365,
        description="Trailing window used to measure the actual collection run rate",
    ),
    db=Depends(get_database),
):
    """Cash on hand + expected collections − committed outflows, per horizon.

    Sources: `bank_accounts` (net of credit-card liability), `invoices` (AR,
    excluding void/draft), `bills` (AP), issued `purchase_orders` converted to
    INR at the live or supplied rate, and an opex average from `bank_transactions`.
    """
    fx_resolved = await _resolve_fx(usd_inr, cny_inr)
    fx = fx_resolved["rates"]
    try:
        cash, receivables, payables, open_pos, opex, collection = await asyncio.gather(
            asyncio.to_thread(_cash_position_sync, db),
            asyncio.to_thread(_receivables_sync, db),
            asyncio.to_thread(_payables_sync, db),
            asyncio.to_thread(_open_po_commitment_sync, db, fx),
            asyncio.to_thread(_monthly_opex_sync, db, opex_months),
            asyncio.to_thread(_collection_run_rate_sync, db, collection_window_days),
        )
    except Exception as e:
        logger.error(f"working-capital failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build working capital view: {e}",
        )

    payload = _build_working_capital(
        cash, receivables, payables, open_pos, opex, collection
    )
    payload["fx"] = fx_resolved["meta"]
    return JSONResponse(status_code=status.HTTP_200_OK, content=payload)


@router.get("/cashflow")
async def get_cashflow(
    days: int = Query(90, ge=7, le=730, description="Trailing window in days"),
    db=Depends(get_database),
):
    """Daily cash in / out, excluding internal transfers."""
    series = await asyncio.to_thread(_cashflow_series_sync, db, days)
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "days": days,
        "series": series,
        "total_inflow": round(sum(s["inflow"] for s in series), 2),
        "total_outflow": round(sum(s["outflow"] for s in series), 2),
        "net": round(sum(s["net"] for s in series), 2),
    })


# ─── Brand order plan ───────────────────────────────────────────────────────────

def _classify_urgency(coverage: float, lead_time: float, drr: float) -> str:
    """Where a brand sits against its own lead time.

    The 3× lead-time ceiling matches the dead-SKU exclusion the dashboard already
    applies to weighted average cover, so the two pages never disagree about what
    counts as overstocked.
    """
    if drr <= 0:
        return "EXCESS"
    if coverage >= 3 * lead_time:
        return "EXCESS"
    if coverage < lead_time:
        return "CRITICAL"
    if coverage < lead_time + 30:
        return "ORDER"
    return "HEALTHY"


URGENCY_RANK = {"CRITICAL": 0, "ORDER": 1, "HEALTHY": 2, "EXCESS": 3}


def _unit_cost_for(
    item: Dict, fallback_costs: Dict[str, float], fx: Dict[str, float]
) -> tuple:
    """Best available cost to buy one more unit, and where it came from.

    Precedence, most to least authoritative for a *forward* purchase:

    1. **Managed unit price** (`product_unit_prices`, the Unit Prices page) —
       what the buyer says the next unit will actually cost. Empty today, but the
       moment it is populated it takes over from everything below.
    2. **COGS** — FIFO value of stock on hand. Accurate, but historical, and
       absent entirely for anything out of stock.
    3. **Last purchase-order rate** — what we last actually paid.

    Returns ``(unit_cost_inr, source)``.
    """
    managed = _f(item.get("unit_price"))
    if managed > 0:
        ccy = (item.get("unit_price_currency") or "INR").upper()
        return round(managed * fx.get(ccy, 1.0), 2), "unit_price"

    cogs = _f(item.get("cogs_unit_cost"))
    if cogs > 0:
        return cogs, "cogs"

    last_po = _f(fallback_costs.get(item.get("sku_code", "")))
    if last_po > 0:
        return last_po, "last_po"

    return 0.0, "none"


def _aggregate_brands(
    items: List[Dict], brand_logistics: Dict,
    fallback_costs: Dict[str, float], fx: Dict[str, float],
) -> List[Dict]:
    """Roll master-report SKUs into brand rows priced per `_unit_cost_for`.

    Every SKU in the brand is kept, not just the ones with an order quantity —
    the buyer needs to see the whole brand to judge the recommendation.
    """
    by_brand: Dict[str, List[Dict]] = {}
    for it in items:
        brand = (it.get("brand") or "Unassigned").strip() or "Unassigned"
        by_brand.setdefault(brand, []).append(it)

    rows = []
    for brand, group in by_brand.items():
        settings = brand_logistics.get(brand.lower(), {})
        lead_time = _f(settings.get("lead_time"), 60.0)

        stock_units = transit_units = 0.0
        stock_value = transit_value = 0.0
        order_units = capital_required = 0.0
        revenue = cogs_sold = 0.0
        drr = 0.0
        skus_to_order = 0
        missing_cost = 0
        estimated_cost = 0
        sku_rows = []

        for it in group:
            cm = it.get("combined_metrics", {}) or {}
            sku = it.get("sku_code", "")
            unit_cost, cost_source = _unit_cost_for(it, fallback_costs, fx)
            on_hand = _f(it.get("latest_total_stock"))
            transit = _f(it.get("total_stock_in_transit"))
            order_qty = _f(it.get("order_qty_plus_extra_qty_rounded"))
            net_units = _f(cm.get("total_sales"))
            sku_drr = _f(cm.get("avg_daily_run_rate"))

            drr += sku_drr
            stock_units += on_hand
            transit_units += transit
            stock_value += on_hand * unit_cost
            transit_value += transit * unit_cost
            revenue += _f(cm.get("total_amount"))
            cogs_sold += max(0.0, net_units) * unit_cost

            if order_qty > 0:
                order_units += order_qty
                capital_required += order_qty * unit_cost
                skus_to_order += 1
                if cost_source == "none":
                    missing_cost += 1
                elif cost_source == "last_po":
                    estimated_cost += 1

            sku_rows.append({
                "sku_code": sku,
                "product_name": (
                    it.get("item_name") or it.get("product_name") or it.get("name") or ""
                ),
                "unit_cost": round(unit_cost, 2),
                "cost_source": cost_source,
                "on_hand": round(on_hand, 2),
                "in_transit": round(transit, 2),
                "drr": round(sku_drr, 3),
                "current_days_coverage": round(_f(it.get("current_days_coverage")), 1),
                "order_qty": round(order_qty, 0),
                "capital_required": round(order_qty * unit_cost, 2),
                "excess_or_order": it.get("excess_or_order", ""),
            })

        coverage = round((stock_units + transit_units) / drr, 1) if drr > 0 else 0.0
        gross_margin = revenue - cogs_sold
        # Inventory at cost, including what is already on the water — that
        # capital is just as committed as what sits in the warehouse.
        avg_inventory = stock_value + transit_value
        gmroi = round(gross_margin / avg_inventory, 2) if avg_inventory > 0 else 0.0
        margin_pct = round(gross_margin / revenue * 100, 1) if revenue > 0 else 0.0
        urgency = _classify_urgency(coverage, lead_time, drr)

        # What freeing this brand down to a healthy 2× lead time would release.
        cash_trapped = 0.0
        if urgency == "EXCESS" and drr > 0 and coverage > 2 * lead_time:
            excess_units = (coverage - 2 * lead_time) * drr
            unit_val = avg_inventory / (stock_units + transit_units) if (stock_units + transit_units) > 0 else 0.0
            cash_trapped = excess_units * unit_val
        elif urgency == "EXCESS" and drr <= 0:
            cash_trapped = avg_inventory

        sku_rows.sort(key=lambda r: -r["capital_required"])

        rows.append({
            "brand": brand,
            "sku_count": len(group),
            "skus_to_order": skus_to_order,
            "skus_missing_cost": missing_cost,
            "skus_estimated_cost": estimated_cost,
            "lead_time": lead_time,
            "drr": round(drr, 3),
            "on_hand_units": round(stock_units, 0),
            "in_transit_units": round(transit_units, 0),
            "stock_value": round(stock_value, 2),
            "transit_value": round(transit_value, 2),
            "inventory_at_cost": round(avg_inventory, 2),
            "current_days_coverage": coverage,
            "revenue": round(revenue, 2),
            "cogs_sold": round(cogs_sold, 2),
            "gross_margin": round(gross_margin, 2),
            "margin_pct": margin_pct,
            "gmroi": gmroi,
            "order_units": round(order_units, 0),
            "capital_required": round(capital_required, 2),
            "urgency": urgency,
            "cash_trapped": round(cash_trapped, 2),
            "skus": sku_rows,
        })

    return rows


def _allocate(rows: List[Dict], envelope: float) -> Dict:
    """Fund brands from the cash envelope, best return on inventory first.

    Ranking by GMROI within urgency tier is the point of the whole report: it
    stops the last of a tight cash position going to a slow, thin-margin brand
    while a fast high-margin one stocks out.
    """
    fundable = [r for r in rows if r["urgency"] in ("CRITICAL", "ORDER") and r["capital_required"] > 0]
    fundable.sort(key=lambda r: (URGENCY_RANK[r["urgency"]], -r["gmroi"], -r["drr"]))

    remaining = max(0.0, envelope)
    total_requested = sum(r["capital_required"] for r in fundable)

    for rank, row in enumerate(fundable, start=1):
        need = row["capital_required"]
        row["priority"] = rank
        if remaining <= 0:
            row["funded_amount"] = 0.0
            row["funded_pct"] = 0.0
            row["recommendation"] = "DEFER"
        elif remaining >= need:
            row["funded_amount"] = round(need, 2)
            row["funded_pct"] = 100.0
            row["recommendation"] = "ORDER FULL"
            remaining -= need
        else:
            row["funded_amount"] = round(remaining, 2)
            row["funded_pct"] = round(remaining / need * 100, 1)
            row["recommendation"] = "ORDER PARTIAL"
            remaining = 0.0

        # Trim the brand's SKU list to what the funded amount actually buys,
        # highest-value lines first, so purchase has something to act on.
        budget = row["funded_amount"]
        for sku in row["skus"]:
            cost = sku["capital_required"]
            if cost <= 0 or budget <= 0:
                sku["funded_qty"] = 0
            elif budget >= cost:
                sku["funded_qty"] = sku["order_qty"]
                budget -= cost
            else:
                unit = sku["unit_cost"]
                sku["funded_qty"] = int(budget // unit) if unit > 0 else 0
                budget = 0.0

    for row in rows:
        if "recommendation" not in row:
            row["priority"] = None
            row["funded_amount"] = 0.0
            row["funded_pct"] = 0.0
            if row["urgency"] in ("CRITICAL", "ORDER"):
                # Needs stock but priced at nothing — never label this
                # "DO NOT ORDER", which is the opposite of the truth. It means
                # the master report suggested no quantity (usually because the
                # SKUs are inactive or discontinued) or we could not cost them.
                row["recommendation"] = "REVIEW"
                row["review_reason"] = (
                    "No costed order quantity — SKUs are likely inactive or "
                    "discontinued, or have no COGS and no purchase history."
                )
            else:
                row["recommendation"] = "HOLD" if row["urgency"] == "HEALTHY" else "DO NOT ORDER"
            for sku in row["skus"]:
                sku["funded_qty"] = 0

    # Display order must equal funding order, or the priority numbers read as
    # wrong. Anything unranked (REVIEW / HOLD / DO NOT ORDER) sorts after every
    # ranked brand, by urgency then GMROI.
    rows.sort(
        key=lambda r: (
            0 if r.get("priority") else 1,
            r.get("priority") or 0,
            URGENCY_RANK[r["urgency"]],
            -(r["gmroi"] or 0),
        )
    )

    return {
        "total_requested": round(total_requested, 2),
        "total_funded": round(sum(r["funded_amount"] for r in fundable), 2),
        "unfunded": round(max(0.0, total_requested - sum(r["funded_amount"] for r in fundable)), 2),
        "envelope": round(envelope, 2),
        "remaining_envelope": round(remaining, 2),
        "cash_trapped_in_excess": round(sum(r["cash_trapped"] for r in rows), 2),
    }


async def _brand_order_plan_data(
    start_date: str, end_date: str, db,
    usd_inr: Optional[float], cny_inr: Optional[float],
    envelope_override: Optional[float], horizon_days: int,
) -> Dict:
    fx_resolved = await _resolve_fx(usd_inr, cny_inr)
    fx = fx_resolved["rates"]

    # COGS is mandatory here — capital_required is meaningless without a unit
    # cost, and product_unit_prices is not populated.
    master_task = _generate_master_report_data(
        start_date, end_date, db, include_cogs=True
    )
    cash_task = asyncio.to_thread(_cash_position_sync, db)
    ar_task = asyncio.to_thread(_receivables_sync, db)
    ap_task = asyncio.to_thread(_payables_sync, db)
    po_task = asyncio.to_thread(_open_po_commitment_sync, db, fx)
    opex_task = asyncio.to_thread(_monthly_opex_sync, db, 3)
    collection_task = asyncio.to_thread(_collection_run_rate_sync, db, 90)
    fallback_task = asyncio.to_thread(_fallback_unit_costs_sync, db, fx)

    (master, cash, receivables, payables, open_pos, opex,
     collection, fallback_costs) = await asyncio.gather(
        master_task, cash_task, ar_task, ap_task, po_task, opex_task,
        collection_task, fallback_task
    )

    working_capital = _build_working_capital(
        cash, receivables, payables, open_pos, opex, collection
    )
    # Fund against the horizon the buyer is actually planning over. A container
    # ordered today lands in 60-96 days, so the cash that pays for it is the cash
    # available over that period, not what is spare this month.
    horizon_projection = next(
        (p for p in working_capital["projections"] if p["horizon_days"] == horizon_days),
        working_capital["projections"][-1],
    )
    envelope = (
        envelope_override
        if envelope_override is not None
        else horizon_projection["free_cash"]
    )

    def _load_logistics():
        return {
            (d.get("brand", "") or "").lower(): d
            for d in db.get_collection(BRAND_LOGISTICS).find({}, {"_id": 0})
        }

    brand_logistics = await asyncio.to_thread(_load_logistics)

    items = master.get("combined_data", []) or []
    rows = _aggregate_brands(items, brand_logistics, fallback_costs, fx)
    allocation = _allocate(rows, envelope)

    cogs_date = (master.get("latest_stock_dates") or {}).get("cogs", "")
    skus_missing_cost = sum(r["skus_missing_cost"] for r in rows)
    skus_estimated_cost = sum(r["skus_estimated_cost"] for r in rows)

    # The exact SKUs behind each warning, so the buyer can act on them rather
    # than just being told a count.
    cost_issues = [
        {
            "brand": r["brand"],
            "sku_code": s["sku_code"],
            "product_name": s["product_name"],
            "order_qty": s["order_qty"],
            "unit_cost": s["unit_cost"],
            "cost_source": s["cost_source"],
            "capital_required": s["capital_required"],
        }
        for r in rows
        for s in r["skus"]
        if s["order_qty"] > 0 and s["cost_source"] in ("last_po", "none")
    ]
    cost_issues.sort(key=lambda x: (x["cost_source"] != "none", -x["order_qty"]))

    warnings = []
    if skus_estimated_cost:
        warnings.append({
            "level": "info",
            "count": skus_estimated_cost,
            "cost_source": "last_po",
            "message": (
                f"{skus_estimated_cost} SKU(s) had no COGS (usually because they are out "
                f"of stock) and are costed at their most recent purchase-order rate instead."
            ),
        })
    if skus_missing_cost:
        warnings.append({
            "level": "warning",
            "count": skus_missing_cost,
            "cost_source": "none",
            "message": (
                f"{skus_missing_cost} SKU(s) with an order quantity have no managed unit "
                f"price, no COGS and no purchase history, so their capital requirement is "
                f"understated."
            ),
        })

    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "working_capital": working_capital,
        "horizon_days": horizon_projection["horizon_days"],
        "allocation": allocation,
        "brands": rows,
        "warnings": warnings,
        "cost_issues": cost_issues,
        "cogs_as_of": cogs_date,
        "fx": fx_resolved["meta"],
        "generated_at": datetime.now().isoformat(),
    }


@router.get("/brand-order-plan")
async def get_brand_order_plan(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    usd_inr: Optional[float] = Query(None, gt=0, description="USD→INR override; omit for live rate"),
    cny_inr: Optional[float] = Query(None, gt=0, description="CNY→INR override; omit for live rate"),
    envelope_override: Optional[float] = Query(
        None, description="Spend this instead of the computed free cash"
    ),
    horizon_days: int = Query(
        90, description="Planning horizon whose free cash funds the plan (30, 60 or 90)"
    ),
    db=Depends(get_database),
):
    """Which brands to order, ranked by GMROI and funded from available cash.

    Each brand is classified against **its own** lead time — CRITICAL below one
    lead time, ORDER below lead time + 30, EXCESS above 3× lead time — then the
    fundable ones are ranked by GMROI (gross margin per rupee of inventory) and
    paid for out of the working-capital envelope until it runs out.

    Runs the full master report with COGS enabled, so expect ~60–90s.
    """
    try:
        payload = await _brand_order_plan_data(
            start_date, end_date, db, usd_inr, cny_inr,
            envelope_override, horizon_days,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"brand-order-plan failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build brand order plan: {e}",
        )
    return JSONResponse(status_code=status.HTTP_200_OK, content=payload)


# ─── Excel ──────────────────────────────────────────────────────────────────────

_HDR_FILL = PatternFill("solid", fgColor="1F3864")
_HDR_FONT = Font(bold=True, color="FFFFFF", size=11)
_SECTION_FILL = PatternFill("solid", fgColor="D9E2F3")
_MONEY = '#,##0'
_THIN = Border(*(Side(style="thin", color="BFBFBF"),) * 4)

_URGENCY_FILL = {
    "CRITICAL": PatternFill("solid", fgColor="FF6B6B"),
    "ORDER": PatternFill("solid", fgColor="FFD966"),
    "HEALTHY": PatternFill("solid", fgColor="C6E0B4"),
    "EXCESS": PatternFill("solid", fgColor="F4B183"),
}


def _write_header(ws, headers: List[str], row: int = 1):
    for col, name in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col, value=name)
        cell.fill = _HDR_FILL
        cell.font = _HDR_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = _THIN
    ws.row_dimensions[row].height = 30


def _autosize(ws, widths: List[int]):
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w


def _build_plan_xlsx(data: Dict) -> bytes:
    wb = openpyxl.Workbook()

    # ── Sheet 1: cash bridge ────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Summary"
    wc = data["working_capital"]
    alloc = data["allocation"]

    ws["A1"] = "Cash-Aware Purchase Plan"
    ws["A1"].font = Font(bold=True, size=16, color="1F3864")
    ws["A2"] = (
        f"Period {data['period']['start_date']} to {data['period']['end_date']}  ·  "
        f"COGS as of {data.get('cogs_as_of') or 'n/a'}  ·  "
        f"generated {data['generated_at'][:16].replace('T', ' ')}"
    )
    ws["A2"].font = Font(italic=True, size=9, color="666666")

    fx_meta = data.get("fx") or {}
    applied = fx_meta.get("applied") or {}
    ws["A3"] = (
        f"Purchase orders converted to INR at USD {applied.get('USD', 'n/a')} / "
        f"CNY {applied.get('CNY', 'n/a')} "
        f"({fx_meta.get('source', 'unknown')}"
        f"{', as of ' + fx_meta['as_of'] if fx_meta.get('as_of') else ''})"
    )
    ws["A3"].font = Font(italic=True, size=9, color="666666")

    r = 4
    ws.cell(row=r, column=1, value="CASH POSITION").fill = _SECTION_FILL
    ws.cell(row=r, column=1).font = Font(bold=True)
    r += 1
    for label, val in (
        ("Cash & bank holdings", wc["cash"]["holdings"]),
        ("Credit card liabilities", -wc["cash"]["liabilities"]),
        ("Net cash", wc["cash"]["net_cash"]),
    ):
        ws.cell(row=r, column=1, value=label)
        c = ws.cell(row=r, column=2, value=val)
        c.number_format = _MONEY
        if label == "Net cash":
            ws.cell(row=r, column=1).font = Font(bold=True)
            c.font = Font(bold=True)
        r += 1

    r += 1
    ws.cell(row=r, column=1, value="PROJECTED FREE CASH").fill = _SECTION_FILL
    ws.cell(row=r, column=1).font = Font(bold=True)
    r += 1
    _write_header(ws, ["Horizon", "Opening cash", "Expected collections",
                       "Bills due", "Open PO commitment", "Operating expenses",
                       "Free cash"], row=r)
    r += 1
    for p in wc["projections"]:
        ws.cell(row=r, column=1, value=f"{p['horizon_days']} days")
        for col, key in enumerate(
            ["opening_cash", "expected_collections", "bills_due",
             "open_po_commitment", "operating_expenses", "free_cash"], start=2
        ):
            c = ws.cell(row=r, column=col, value=p[key])
            c.number_format = _MONEY
            if key == "free_cash":
                c.font = Font(bold=True)
        r += 1

    r += 1
    ws.cell(row=r, column=1, value="ALLOCATION").fill = _SECTION_FILL
    ws.cell(row=r, column=1).font = Font(bold=True)
    r += 1
    for label, val in (
        ("Purchase envelope", alloc["envelope"]),
        ("Total requested by brands", alloc["total_requested"]),
        ("Funded", alloc["total_funded"]),
        ("Unfunded shortfall", alloc["unfunded"]),
        ("Cash trapped in EXCESS brands", alloc["cash_trapped_in_excess"]),
    ):
        ws.cell(row=r, column=1, value=label)
        ws.cell(row=r, column=2, value=val).number_format = _MONEY
        r += 1

    if data.get("warnings"):
        r += 1
        ws.cell(row=r, column=1, value="WARNINGS").fill = PatternFill("solid", fgColor="FFF2CC")
        ws.cell(row=r, column=1).font = Font(bold=True)
        r += 1
        for w in data["warnings"]:
            ws.cell(row=r, column=1, value=w["message"] if isinstance(w, dict) else w)
            r += 1

    _autosize(ws, [34, 20, 22, 18, 22, 22, 18])

    # ── Sheet 2: brand plan ─────────────────────────────────────────────────
    ws2 = wb.create_sheet("Brand Plan")
    headers = [
        "Priority", "Brand", "Urgency", "Recommendation", "Lead Time",
        "DRR", "On Hand", "In Transit", "Days Coverage",
        "Inventory at Cost", "Revenue", "Gross Margin", "Margin %", "GMROI",
        "SKUs to Order", "Order Units", "Capital Required",
        "Funded Amount", "Funded %", "Cash Trapped",
    ]
    _write_header(ws2, headers)
    for i, b in enumerate(data["brands"], start=2):
        vals = [
            b.get("priority"), b["brand"], b["urgency"], b["recommendation"],
            b["lead_time"], b["drr"], b["on_hand_units"], b["in_transit_units"],
            b["current_days_coverage"], b["inventory_at_cost"], b["revenue"],
            b["gross_margin"], b["margin_pct"], b["gmroi"], b["skus_to_order"],
            b["order_units"], b["capital_required"], b["funded_amount"],
            b["funded_pct"], b["cash_trapped"],
        ]
        for col, v in enumerate(vals, start=1):
            c = ws2.cell(row=i, column=col, value=v)
            c.border = _THIN
            if col in (10, 11, 12, 17, 18, 20):
                c.number_format = _MONEY
        ws2.cell(row=i, column=3).fill = _URGENCY_FILL.get(b["urgency"], PatternFill())
    ws2.freeze_panes = "C2"
    _autosize(ws2, [9, 22, 11, 16, 11, 9, 11, 11, 14, 18, 16, 16, 10, 9, 13, 13, 18, 16, 11, 16])

    # ── Sheet 3: SKU detail for funded brands ───────────────────────────────
    ws3 = wb.create_sheet("SKU Detail")
    sku_headers = [
        "Brand", "Recommendation", "SKU Code", "Product Name", "Unit Cost",
        "Cost Source", "On Hand", "In Transit", "DRR", "Days Coverage",
        "Order Qty", "Funded Qty", "Capital Required", "Excess / Order",
    ]
    _COST_SOURCE_LABEL = {
        "unit_price": "Managed unit price",
        "cogs": "COGS",
        "last_po": "Last PO rate (est)",
        "none": "Unknown",
    }
    _write_header(ws3, sku_headers)
    row = 2
    for b in data["brands"]:
        if b["recommendation"] in ("DO NOT ORDER",):
            continue
        for s in b["skus"]:
            vals = [
                b["brand"], b["recommendation"], s["sku_code"], s["product_name"],
                s["unit_cost"], _COST_SOURCE_LABEL.get(s.get("cost_source", ""), ""),
                s["on_hand"], s["in_transit"], s["drr"],
                s["current_days_coverage"], s["order_qty"], s.get("funded_qty", 0),
                s["capital_required"], s["excess_or_order"],
            ]
            for col, v in enumerate(vals, start=1):
                c = ws3.cell(row=row, column=col, value=v)
                c.border = _THIN
                if col in (5, 13):
                    c.number_format = _MONEY
            row += 1
    ws3.freeze_panes = "C2"
    _autosize(ws3, [20, 16, 16, 44, 12, 18, 11, 11, 9, 14, 11, 12, 18, 15])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


@router.get("/brand-order-plan/download")
async def download_brand_order_plan(
    start_date: str = Query(...),
    end_date: str = Query(...),
    usd_inr: Optional[float] = Query(None, gt=0, description="USD→INR override; omit for live rate"),
    cny_inr: Optional[float] = Query(None, gt=0, description="CNY→INR override; omit for live rate"),
    envelope_override: Optional[float] = Query(None),
    horizon_days: int = Query(90),
    db=Depends(get_database),
):
    """Three-sheet workbook: cash bridge, brand plan, SKU detail."""
    try:
        data = await _brand_order_plan_data(
            start_date, end_date, db, usd_inr, cny_inr,
            envelope_override, horizon_days,
        )
        content = await asyncio.to_thread(_build_plan_xlsx, data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"brand-order-plan download failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build workbook: {e}",
        )

    filename = f"brand_order_plan_{start_date}_to_{end_date}.xlsx"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
