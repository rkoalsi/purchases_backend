# amazon_ads.py
"""
Amazon Ads (AMS) reporting — Sponsored Products / Brands / Display.

Data is pulled from the Amazon Ads v3 async reporting API and stored locally,
because Amazon only retains report data for ~60-95 days. Once that window
passes the history is gone for good, so the cron in helpers/scheduler.py must
keep running for trend data to exist at all.

Scoped to a single advertising profile (ADS_PROFILE_ID) by design.
"""
import os
import io
import re
import gzip
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta, timezone, date

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pymongo import UpdateOne, ASCENDING, DESCENDING

from ..database import get_database, serialize_mongo_document

logger = logging.getLogger(__name__)

router = APIRouter()

# --- Collections ---
CAMPAIGNS_COLLECTION = "amazon_ads_campaigns"
DAILY_COLLECTION = "amazon_ads_daily"
SYNC_RUNS_COLLECTION = "amazon_ads_sync_runs"
REPORT_JOBS_COLLECTION = "amazon_ads_report_jobs"

# --- Credentials ---
_ADS_CLIENT_ID = os.getenv("ADS_CLIENT_ID", "")
_ADS_CLIENT_SECRET = os.getenv("ADS_CLIENT_SECRET", "")
_ADS_REFRESH_TOKEN = os.getenv("ADS_REFRESH_TOKEN", "")
_ADS_PROFILE_ID = os.getenv("ADS_PROFILE_ID", "")
_ADS_API_URL = os.getenv("ADS_API_URL", "https://advertising-api-eu.amazon.com")
_LWA_TOKEN_URL = os.getenv("ADS_TOKEN_URL", "https://api.amazon.com/auth/o2/token")

_TOKEN_REFRESH_BUFFER = timedelta(minutes=5)
_access_token = None
_token_expires_at = None

# Amazon restates attribution for roughly 14 days after the fact, so a daily
# sync always re-pulls a trailing window rather than yesterday alone.
RESTATEMENT_WINDOW_DAYS = 14
# Amazon's retention ceiling; the first run backfills this far.
MAX_LOOKBACK_DAYS = 90

# A single report may not span more than 31 days, so longer windows are split
# into chunks and queued as separate reports.
MAX_REPORT_RANGE_DAYS = 31

# Retention is shorter than 90 days for some ad products and moves daily.
# These are conservative defaults; the real cut-off is read from Amazon's own
# error message and applied per product, so this only avoids a wasted call.
_DEFAULT_RETENTION_DAYS = {
    "SPONSORED_PRODUCTS": 90,
    "SPONSORED_BRANDS": 60,
    "SPONSORED_DISPLAY": 65,
}

# Amazon states the true cut-off in its 400 body, e.g.
# "must be equal to or after report type data retention start date (2026-06-07)"
_RETENTION_RE = re.compile(r"data retention start date \((\d{4}-\d{2}-\d{2})\)")
# Only a column complaint should trigger the drop-optional-columns retry.
_COLUMN_ERROR_RE = re.compile(r"column|field|metric", re.IGNORECASE)

# Amazon's report queue is slow and highly variable — reports for this account
# have been observed still PENDING after 50+ minutes. Holding a request or a
# worker open that long is not viable, so the daily path is split into
# initiate (create reports, store the ids) and collect (download whatever is
# ready), mirroring the existing Vendor Central cron/initiate + cron/collect
# split. The inline poll below is only for small manual runs.
_POLL_INTERVAL_SECONDS = 15
_POLL_MAX_ATTEMPTS = 240  # ~60 minutes, inline path only

AD_PRODUCTS = ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY")

# Column sets are split into required/optional. Amazon rejects the whole
# request with a 400 if any single column is invalid for a report type, so an
# unexpected rejection retries with the required set instead of failing the
# entire sync.
_REPORT_SPECS = {
    "SPONSORED_PRODUCTS": {
        "reportTypeId": "spCampaigns",
        "required": ["date", "campaignId", "campaignName", "impressions", "clicks", "cost"],
        "optional": ["campaignStatus", "purchases30d", "sales30d", "unitsSoldClicks30d"],
        "orders_field": "purchases30d",
        "sales_field": "sales30d",
        "units_field": "unitsSoldClicks30d",
    },
    "SPONSORED_BRANDS": {
        "reportTypeId": "sbCampaigns",
        "required": ["date", "campaignId", "campaignName", "impressions", "clicks", "cost"],
        "optional": ["campaignStatus", "purchases", "sales", "unitsSold"],
        "orders_field": "purchases",
        "sales_field": "sales",
        "units_field": "unitsSold",
    },
    "SPONSORED_DISPLAY": {
        "reportTypeId": "sdCampaigns",
        "required": ["date", "campaignId", "campaignName", "impressions", "clicks", "cost"],
        "optional": ["campaignStatus", "purchases", "sales", "unitsSold"],
        "orders_field": "purchases",
        "sales_field": "sales",
        "units_field": "unitsSold",
    },
}

_CREATE_REPORT_CT = "application/vnd.createasyncreportrequest.v3+json"


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------
def _get_ads_token() -> str:
    """LWA access token, cached until shortly before it expires (3600s life)."""
    global _access_token, _token_expires_at

    if (
        _access_token
        and _token_expires_at
        and datetime.now() < (_token_expires_at - _TOKEN_REFRESH_BUFFER)
    ):
        return _access_token

    if not (_ADS_CLIENT_ID and _ADS_CLIENT_SECRET and _ADS_REFRESH_TOKEN):
        raise HTTPException(
            status_code=503,
            detail="Amazon Ads credentials are not configured (ADS_CLIENT_ID / ADS_CLIENT_SECRET / ADS_REFRESH_TOKEN)",
        )

    r = requests.post(
        _LWA_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": _ADS_REFRESH_TOKEN,
            "client_id": _ADS_CLIENT_ID,
            "client_secret": _ADS_CLIENT_SECRET,
        },
        timeout=30,
    )
    if r.status_code != 200:
        # invalid_grant  = token revoked/expired -> needs a fresh consent round-trip
        # unauthorized_client = refresh token was issued to a different client id
        raise HTTPException(
            status_code=502,
            detail=f"Amazon Ads token refresh failed: {r.text[:300]}",
        )
    data = r.json()
    _access_token = data["access_token"]
    _token_expires_at = datetime.now() + timedelta(seconds=data.get("expires_in", 3600))
    return _access_token


def _headers(extra: dict | None = None) -> dict:
    h = {
        "Authorization": f"Bearer {_get_ads_token()}",
        "Amazon-Advertising-API-ClientId": _ADS_CLIENT_ID,
        "Amazon-Advertising-API-Scope": _ADS_PROFILE_ID,
    }
    if extra:
        h.update(extra)
    return h


def _request_with_backoff(method: str, url: str, *, max_attempts: int = 6, **kwargs):
    """Amazon throttles the reporting API aggressively; 429 needs real backoff."""
    delay = 5
    last = None
    for attempt in range(max_attempts):
        resp = requests.request(method, url, timeout=90, **kwargs)
        if resp.status_code != 429:
            return resp
        last = resp
        logger.warning(
            "Amazon Ads throttled (429) on %s, attempt %d/%d — sleeping %ds",
            url, attempt + 1, max_attempts, delay,
        )
        time.sleep(delay)
        delay = min(delay * 2, 120)
    return last


# --------------------------------------------------------------------------
# Report fetching
# --------------------------------------------------------------------------
def _create_report_sync(ad_product: str, start_date: str, end_date: str, columns: list[str]) -> str:
    spec = _REPORT_SPECS[ad_product]
    body = {
        "name": f"{spec['reportTypeId']}_{start_date}_{end_date}",
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": ad_product,
            "groupBy": ["campaign"],
            "columns": columns,
            "reportTypeId": spec["reportTypeId"],
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }
    resp = _request_with_backoff(
        "POST",
        f"{_ADS_API_URL}/reporting/reports",
        headers=_headers({"Content-Type": _CREATE_REPORT_CT, "Accept": _CREATE_REPORT_CT}),
        data=json.dumps(body),
    )
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"create report failed [{resp.status_code}]: {resp.text[:400]}")
    return resp.json()["reportId"]


def _poll_report_sync(report_id: str) -> str | None:
    """Poll until COMPLETED; returns the download URL, or None if it failed."""
    for attempt in range(_POLL_MAX_ATTEMPTS):
        resp = _request_with_backoff(
            "GET", f"{_ADS_API_URL}/reporting/reports/{report_id}", headers=_headers()
        )
        if resp.status_code != 200:
            raise RuntimeError(f"poll failed [{resp.status_code}]: {resp.text[:300]}")
        data = resp.json()
        status_ = data.get("status")
        if status_ == "COMPLETED":
            return data.get("url")
        if status_ == "FAILED":
            logger.error("Report %s FAILED: %s", report_id, data.get("failureReason"))
            return None
        time.sleep(_POLL_INTERVAL_SECONDS)
    logger.error("Report %s still pending after %d polls — giving up", report_id, _POLL_MAX_ATTEMPTS)
    return None


def _download_report_sync(url: str) -> list[dict]:
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))


def _chunk_ranges(start_date: str, end_date: str, max_days: int = MAX_REPORT_RANGE_DAYS) -> list[tuple[str, str]]:
    """Split a window into report-sized pieces (Amazon caps a report at 31 days)."""
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)
    if s > e:
        return []
    out = []
    while s <= e:
        chunk_end = min(s + timedelta(days=max_days - 1), e)
        out.append((s.isoformat(), chunk_end.isoformat()))
        s = chunk_end + timedelta(days=1)
    return out


def _clamp_start_to_retention(ad_product: str, start_date: str) -> str:
    """Apply the conservative per-product retention floor before calling out."""
    days = _DEFAULT_RETENTION_DAYS.get(ad_product, MAX_LOOKBACK_DAYS)
    floor = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    return max(start_date, floor)


def _create_report_with_retries(ad_product: str, start_date: str, end_date: str) -> tuple[str, str, str]:
    """
    Create one report, recovering from the two 400s Amazon actually returns.

    Returns (report_id, effective_start, effective_end) — the dates may differ
    from those requested when retention forced the start date forward.
    """
    spec = _REPORT_SPECS[ad_product]
    columns = spec["required"] + spec["optional"]

    for attempt in range(3):
        try:
            return _create_report_sync(ad_product, start_date, end_date, columns), start_date, end_date
        except RuntimeError as e:
            msg = str(e)
            if "400" not in msg:
                raise

            # Amazon names the exact retention cut-off — honour it rather than guess.
            m = _RETENTION_RE.search(msg)
            if m and m.group(1) > start_date:
                new_start = m.group(1)
                if new_start > end_date:
                    raise RuntimeError(
                        f"{ad_product}: entire window {start_date}..{end_date} is older than "
                        f"Amazon's retention cut-off ({new_start}) — this data no longer exists"
                    )
                logger.info(
                    "%s: clamping start %s -> %s (Amazon retention limit)",
                    ad_product, start_date, new_start,
                )
                start_date = new_start
                continue

            # Only drop optional columns when the complaint is about columns.
            if _COLUMN_ERROR_RE.search(msg) and columns != spec["required"]:
                logger.warning("%s rejected optional columns, retrying required-only: %s", ad_product, msg)
                columns = spec["required"]
                continue

            raise

    raise RuntimeError(f"{ad_product}: could not create report after retries")


def _fetch_report_sync(ad_product: str, start_date: str, end_date: str) -> list[dict]:
    """Create → poll → download one report (inline path; single chunk only)."""
    report_id, _, _ = _create_report_with_retries(ad_product, start_date, end_date)
    url = _poll_report_sync(report_id)
    if not url:
        return []
    return _download_report_sync(url)


# --------------------------------------------------------------------------
# Normalisation + persistence
# --------------------------------------------------------------------------
def _num(v):
    """0 must survive as 0 — `v or 0` would be fine here but None must stay distinct."""
    if v is None:
        return 0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0


def _normalize_row(row: dict, ad_product: str) -> dict | None:
    spec = _REPORT_SPECS[ad_product]
    campaign_id = row.get("campaignId")
    row_date = row.get("date")
    if not campaign_id or not row_date:
        return None

    cost = _num(row.get("cost"))
    sales = _num(row.get(spec["sales_field"]))
    clicks = _num(row.get("clicks"))
    impressions = _num(row.get("impressions"))
    orders = _num(row.get(spec["orders_field"]))

    return {
        "date": str(row_date),
        "campaign_id": str(campaign_id),
        "campaign_name": row.get("campaignName"),
        "campaign_status": row.get("campaignStatus"),
        "ad_product": ad_product,
        "profile_id": _ADS_PROFILE_ID,
        "impressions": impressions,
        "clicks": clicks,
        "cost": round(cost, 2),
        "orders": orders,
        "sales": round(sales, 2),
        "units": _num(row.get(spec["units_field"])),
        # Derived here so the UI and Excel never recompute them inconsistently.
        "ctr": round(clicks / impressions * 100, 4) if impressions else 0,
        "cpc": round(cost / clicks, 2) if clicks else 0,
        "acos": round(cost / sales * 100, 2) if sales else None,
        "roas": round(sales / cost, 2) if cost else None,
        "updated_at": datetime.now(timezone.utc),
    }


def _store_rows_sync(db, rows: list[dict]) -> int:
    """Upsert by (date, campaign_id, ad_product) so re-pulls are idempotent."""
    if not rows:
        return 0
    ops = [
        UpdateOne(
            {
                "date": r["date"],
                "campaign_id": r["campaign_id"],
                "ad_product": r["ad_product"],
                "profile_id": r["profile_id"],
            },
            {"$set": r},
            upsert=True,
        )
        for r in rows
    ]
    result = db[DAILY_COLLECTION].bulk_write(ops, ordered=False)
    return (result.upserted_count or 0) + (result.modified_count or 0)


def _store_campaign_meta_sync(db, rows: list[dict]) -> int:
    """One doc per campaign, carrying the most recent name/status seen."""
    latest: dict[str, dict] = {}
    for r in rows:
        cid = r["campaign_id"]
        if cid not in latest or r["date"] >= latest[cid]["date"]:
            latest[cid] = r
    if not latest:
        return 0
    ops = [
        UpdateOne(
            {"campaign_id": cid, "profile_id": _ADS_PROFILE_ID},
            {
                "$set": {
                    "campaign_id": cid,
                    "campaign_name": r.get("campaign_name"),
                    "campaign_status": r.get("campaign_status"),
                    "ad_product": r["ad_product"],
                    "profile_id": _ADS_PROFILE_ID,
                    "last_seen_date": r["date"],
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
        for cid, r in latest.items()
    ]
    db[CAMPAIGNS_COLLECTION].bulk_write(ops, ordered=False)
    return len(latest)


# --------------------------------------------------------------------------
# Console export import
#
# The Ads console retains data longer than the reporting API, so a console
# export is the only way to recover dates that have aged out (e.g. Sponsored
# Brands before its retention floor). Exports carry campaign NAMES but no
# campaign ids, and always use 14-day attribution.
# --------------------------------------------------------------------------
_CONSOLE_COLUMNS = {
    # canonical -> candidate header names, matched case-insensitively after
    # stripping whitespace (Amazon ships headers with stray trailing spaces).
    "date": ["date"],
    "campaign_name": ["campaign name"],
    "campaign_status": ["status"],
    "impressions": ["impressions"],
    "clicks": ["clicks"],
    "cost": ["spend - converted", "spend"],
    "sales": ["14 day total sales - converted", "14 day total sales"],
    "orders": ["14 day total orders (#)"],
    "units": ["14 day total units (#)"],
}


def _resolve_console_columns(df) -> dict:
    lookup = {str(c).strip().lower(): c for c in df.columns}
    resolved = {}
    for canon, candidates in _CONSOLE_COLUMNS.items():
        for cand in candidates:
            if cand in lookup:
                resolved[canon] = lookup[cand]
                break
    missing = [c for c in ("date", "campaign_name", "impressions", "clicks", "cost") if c not in resolved]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Console export is missing required columns: {missing}. Found: {list(df.columns)[:15]}",
        )
    return resolved


def _parse_console_export_sync(db, file_bytes: bytes, ad_product: str) -> tuple[list[dict], dict]:
    """Turn a console xlsx export into normalised daily rows."""
    import pandas as pd

    df = pd.read_excel(io.BytesIO(file_bytes))
    cols = _resolve_console_columns(df)

    # Exports have no campaign id, so names are mapped back to the ids already
    # known from the API. Unmatched names get a stable synthetic id that can
    # never collide with Amazon's numeric ids.
    known = {
        d["campaign_name"]: d["campaign_id"]
        for d in db[CAMPAIGNS_COLLECTION].find(
            {"profile_id": _ADS_PROFILE_ID}, {"campaign_name": 1, "campaign_id": 1}
        )
        if d.get("campaign_name")
    }

    rows, unmatched = [], set()
    for _, r in df.iterrows():
        name = str(r[cols["campaign_name"]]).strip()
        if not name or name == "nan":
            continue
        raw_date = pd.to_datetime(r[cols["date"]], errors="coerce")
        if pd.isna(raw_date):
            continue

        cid = known.get(name)
        if not cid:
            unmatched.add(name)
            cid = f"NAME:{name}"

        get = lambda k: _num(r[cols[k]]) if k in cols else 0
        cost, sales, clicks, impressions = get("cost"), get("sales"), get("clicks"), get("impressions")

        rows.append({
            "date": raw_date.date().isoformat(),
            "campaign_id": str(cid),
            "campaign_name": name,
            "campaign_status": (str(r[cols["campaign_status"]]).strip().upper()
                                if "campaign_status" in cols and str(r[cols["campaign_status"]]) != "nan" else None),
            "ad_product": ad_product,
            "profile_id": _ADS_PROFILE_ID,
            "impressions": impressions,
            "clicks": clicks,
            "cost": round(cost, 2),
            "orders": get("orders"),
            "sales": round(sales, 2),
            "units": get("units"),
            "ctr": round(clicks / impressions * 100, 4) if impressions else 0,
            "cpc": round(cost / clicks, 2) if clicks else 0,
            "acos": round(cost / sales * 100, 2) if sales else None,
            "roas": round(sales / cost, 2) if cost else None,
            # Provenance: console exports are 14-day attribution, API SP is 30-day.
            "source": "console_upload",
            "attribution": "14d",
            "updated_at": datetime.now(timezone.utc),
        })

    return rows, {"unmatched_campaign_names": sorted(unmatched)[:20], "unmatched_count": len(unmatched)}


_indexes_ready = False


def _ensure_indexes_sync(db):
    global _indexes_ready
    if _indexes_ready:
        return
    try:
        db[DAILY_COLLECTION].create_index(
            [("date", ASCENDING), ("campaign_id", ASCENDING), ("ad_product", ASCENDING)],
            name="ads_daily_unique_key",
        )
        db[DAILY_COLLECTION].create_index([("date", DESCENDING)], name="ads_daily_date")
        db[DAILY_COLLECTION].create_index([("ad_product", ASCENDING)], name="ads_daily_product")
        db[CAMPAIGNS_COLLECTION].create_index([("campaign_id", ASCENDING)], name="ads_campaign_id")
        db[SYNC_RUNS_COLLECTION].create_index([("started_at", DESCENDING)], name="ads_runs_started")
        _indexes_ready = True
    except Exception as e:  # index creation must never block a sync
        logger.warning("Amazon Ads index creation skipped: %s", e)


# --------------------------------------------------------------------------
# Sync orchestration
# --------------------------------------------------------------------------
def _run_sync_sync(db, start_date: str, end_date: str, ad_products: list[str]) -> dict:
    _ensure_indexes_sync(db)
    run_doc = {
        "started_at": datetime.now(timezone.utc),
        "start_date": start_date,
        "end_date": end_date,
        "ad_products": ad_products,
        "status": "running",
        "profile_id": _ADS_PROFILE_ID,
    }
    run_id = db[SYNC_RUNS_COLLECTION].insert_one(run_doc).inserted_id

    per_product, errors = {}, {}
    for ad_product in ad_products:
        try:
            raw = _fetch_report_sync(ad_product, start_date, end_date)
            rows = [n for n in (_normalize_row(r, ad_product) for r in raw) if n]
            stored = _store_rows_sync(db, rows)
            _store_campaign_meta_sync(db, rows)
            per_product[ad_product] = {"rows": len(rows), "stored": stored}
            logger.info("Amazon Ads %s: %d rows stored", ad_product, stored)
        except Exception as e:
            # One ad product failing must not lose the others.
            logger.error("Amazon Ads sync failed for %s: %s", ad_product, e)
            errors[ad_product] = str(e)[:500]
            per_product[ad_product] = {"rows": 0, "stored": 0}

    summary = {
        "status": "completed" if not errors else ("partial" if per_product else "failed"),
        "finished_at": datetime.now(timezone.utc),
        "per_product": per_product,
        "errors": errors,
        "total_rows": sum(v["rows"] for v in per_product.values()),
    }
    db[SYNC_RUNS_COLLECTION].update_one({"_id": run_id}, {"$set": summary})
    return {**summary, "run_id": str(run_id), "start_date": start_date, "end_date": end_date}


# --------------------------------------------------------------------------
# Two-phase sync: initiate (queue reports) -> collect (download when ready)
# --------------------------------------------------------------------------
def _initiate_reports_sync(db, start_date: str, end_date: str, ad_products: list[str]) -> dict:
    """
    Ask Amazon to build one report per ad product and record the ids.

    Amazon returns the *same* reportId for an identical configuration, so
    re-initiating an unchanged window is cheap and does not duplicate work.
    """
    _ensure_indexes_sync(db)
    queued, errors, skipped = [], {}, []

    for ad_product in ad_products:
        # Retention differs per ad product (Sponsored Brands is the shortest),
        # so each one gets its own clamped window before chunking.
        product_start = _clamp_start_to_retention(ad_product, start_date)
        if product_start > end_date:
            skipped.append({
                "ad_product": ad_product,
                "reason": f"window ends {end_date}, before retention start {product_start}",
            })
            continue
        if product_start != start_date:
            logger.info("%s: start clamped %s -> %s by retention", ad_product, start_date, product_start)

        for chunk_start, chunk_end in _chunk_ranges(product_start, end_date):
            try:
                report_id, eff_start, eff_end = _create_report_with_retries(ad_product, chunk_start, chunk_end)
                db[REPORT_JOBS_COLLECTION].update_one(
                    {"report_id": report_id},
                    {
                        "$set": {
                            "report_id": report_id,
                            "ad_product": ad_product,
                            "start_date": eff_start,
                            "end_date": eff_end,
                            "profile_id": _ADS_PROFILE_ID,
                            "status": "pending",
                            "updated_at": datetime.now(timezone.utc),
                        },
                        "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
                    },
                    upsert=True,
                )
                queued.append({"ad_product": ad_product, "report_id": report_id,
                               "start_date": eff_start, "end_date": eff_end})
                logger.info("Amazon Ads queued %s %s..%s -> %s", ad_product, eff_start, eff_end, report_id)
            except Exception as e:
                # One chunk failing must not abandon the rest of the window.
                logger.error("Failed to queue %s %s..%s: %s", ad_product, chunk_start, chunk_end, e)
                errors.setdefault(ad_product, []).append(f"{chunk_start}..{chunk_end}: {str(e)[:300]}")

    return {"queued": queued, "errors": errors, "skipped": skipped,
            "start_date": start_date, "end_date": end_date}


def _collect_reports_sync(db, max_jobs: int = 20) -> dict:
    """
    Download every pending report that Amazon has finished.

    Reports that are still building are left alone for the next collect pass,
    so a slow report delays its data by a cycle rather than losing it.
    """
    _ensure_indexes_sync(db)
    jobs = list(
        db[REPORT_JOBS_COLLECTION]
        .find({"status": "pending", "profile_id": _ADS_PROFILE_ID})
        .sort([("created_at", ASCENDING), ("_id", ASCENDING)])
        .limit(max_jobs)
    )

    collected, still_pending, failed, total_rows = [], [], [], 0

    for job in jobs:
        rid = job["report_id"]
        try:
            resp = _request_with_backoff(
                "GET", f"{_ADS_API_URL}/reporting/reports/{rid}", headers=_headers()
            )
            if resp.status_code != 200:
                logger.warning("Collect: poll failed for %s [%s]", rid, resp.status_code)
                still_pending.append(rid)
                continue

            data = resp.json()
            status_ = data.get("status")

            if status_ == "COMPLETED":
                raw = _download_report_sync(data["url"])
                rows = [n for n in (_normalize_row(r, job["ad_product"]) for r in raw) if n]
                stored = _store_rows_sync(db, rows)
                _store_campaign_meta_sync(db, rows)
                total_rows += len(rows)
                db[REPORT_JOBS_COLLECTION].update_one(
                    {"_id": job["_id"]},
                    {"$set": {
                        "status": "collected",
                        "rows": len(rows),
                        "stored": stored,
                        "collected_at": datetime.now(timezone.utc),
                    }},
                )
                collected.append({"ad_product": job["ad_product"], "report_id": rid, "rows": len(rows)})
                logger.info("Amazon Ads collected %s: %d rows", job["ad_product"], len(rows))

            elif status_ == "FAILED":
                db[REPORT_JOBS_COLLECTION].update_one(
                    {"_id": job["_id"]},
                    {"$set": {
                        "status": "failed",
                        "failure_reason": data.get("failureReason"),
                        "collected_at": datetime.now(timezone.utc),
                    }},
                )
                failed.append({"ad_product": job["ad_product"], "report_id": rid,
                               "reason": data.get("failureReason")})
            else:
                still_pending.append(rid)
        except Exception as e:
            logger.error("Collect failed for report %s: %s", rid, e)
            still_pending.append(rid)

    return {
        "collected": collected,
        "still_pending": len(still_pending),
        "failed": failed,
        "total_rows": total_rows,
    }


def _default_window(days: int | None = None) -> tuple[str, str]:
    """Amazon data lags ~12h, so 'yesterday' is the newest reliable end date."""
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=(days or RESTATEMENT_WINDOW_DAYS) - 1)
    return start.isoformat(), end.isoformat()


async def run_ads_sync(db, start_date: str | None = None, end_date: str | None = None,
                       ad_products: list[str] | None = None, days: int | None = None) -> dict:
    """Inline create+poll+store. Only for manual runs — can block up to an hour."""
    if not start_date or not end_date:
        start_date, end_date = _default_window(days)
    products = ad_products or list(AD_PRODUCTS)
    invalid = [p for p in products if p not in _REPORT_SPECS]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown ad products: {invalid}")
    return await asyncio.to_thread(_run_sync_sync, db, start_date, end_date, products)


async def initiate_ads_reports(db, start_date: str | None = None, end_date: str | None = None,
                               ad_products: list[str] | None = None, days: int | None = None) -> dict:
    """Phase 1 — queue reports with Amazon. Returns immediately."""
    if not start_date or not end_date:
        start_date, end_date = _default_window(days)
    products = ad_products or list(AD_PRODUCTS)
    return await asyncio.to_thread(_initiate_reports_sync, db, start_date, end_date, products)


async def collect_ads_reports(db, max_jobs: int = 20) -> dict:
    """Phase 2 — download whatever Amazon has finished building."""
    return await asyncio.to_thread(_collect_reports_sync, db, max_jobs)


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------
class SyncRequest(BaseModel):
    start_date: str | None = None
    end_date: str | None = None
    ad_products: list[str] | None = None
    days: int | None = None


@router.get("/config")
async def get_config():
    """Whether credentials are wired up — lets the UI show a clear error."""
    return {
        "configured": bool(_ADS_CLIENT_ID and _ADS_CLIENT_SECRET and _ADS_REFRESH_TOKEN and _ADS_PROFILE_ID),
        "profile_id": _ADS_PROFILE_ID,
        "api_url": _ADS_API_URL,
        "ad_products": list(AD_PRODUCTS),
        "max_lookback_days": MAX_LOOKBACK_DAYS,
        "restatement_window_days": RESTATEMENT_WINDOW_DAYS,
    }


@router.get("/profiles")
async def list_profiles():
    """Live profile list — useful for verifying the token still has access."""
    def _fetch():
        resp = _request_with_backoff("GET", f"{_ADS_API_URL}/v2/profiles", headers=_headers())
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"profiles failed: {resp.text[:300]}")
        return resp.json()

    return await asyncio.to_thread(_fetch)


@router.post("/sync")
async def trigger_sync(payload: SyncRequest, background_tasks: BackgroundTasks, db=Depends(get_database)):
    """
    Pull reports and store them. Runs in the background because a full
    three-product sync takes several minutes of polling.
    """
    start_date, end_date = payload.start_date, payload.end_date
    if not start_date or not end_date:
        start_date, end_date = _default_window(payload.days)

    earliest = (datetime.now(timezone.utc).date() - timedelta(days=MAX_LOOKBACK_DAYS)).isoformat()
    if start_date < earliest:
        # Amazon simply has no data before this; silently clamping would hide it.
        raise HTTPException(
            status_code=400,
            detail=f"start_date {start_date} exceeds Amazon's ~{MAX_LOOKBACK_DAYS}-day retention (earliest available: {earliest})",
        )

    products = payload.ad_products or list(AD_PRODUCTS)

    async def _task():
        try:
            await run_ads_sync(db, start_date, end_date, products)
        except Exception as e:
            logger.error("Background Amazon Ads sync failed: %s", e)

    background_tasks.add_task(_task)
    return {
        "status": "started",
        "start_date": start_date,
        "end_date": end_date,
        "ad_products": products,
        "message": "Sync running in background; poll GET /amazon_ads/sync-runs for status.",
    }


@router.post("/sync/initiate")
async def initiate_sync(payload: SyncRequest, db=Depends(get_database)):
    """
    Queue reports with Amazon without waiting for them.

    Preferred over /sync for anything large: Amazon's build times are highly
    variable, so collection happens separately.
    """
    start_date, end_date = payload.start_date, payload.end_date
    if not start_date or not end_date:
        start_date, end_date = _default_window(payload.days)

    earliest = (datetime.now(timezone.utc).date() - timedelta(days=MAX_LOOKBACK_DAYS)).isoformat()
    if start_date < earliest:
        raise HTTPException(
            status_code=400,
            detail=f"start_date {start_date} exceeds Amazon's ~{MAX_LOOKBACK_DAYS}-day retention (earliest available: {earliest})",
        )

    return await initiate_ads_reports(db, start_date, end_date, payload.ad_products)


@router.post("/sync/collect")
async def collect_sync(max_jobs: int = Query(20, le=100), db=Depends(get_database)):
    """Download and store any queued reports Amazon has finished."""
    return await collect_ads_reports(db, max_jobs)


@router.post("/upload")
async def upload_console_export(
    file: UploadFile = File(...),
    ad_product: str = Form(...),
    mode: str = Form("fill_missing"),
    uploaded_by: str | None = Form(None),
    db=Depends(get_database),
):
    """
    Import an Amazon Ads console campaign export (xlsx).

    The console retains data past the reporting API's retention floor, so this
    is the only route to dates the API refuses to serve.

    mode:
      fill_missing (default) — only writes dates with no existing rows for this
                               ad product, so API data is never disturbed.
      replace                — overwrites every date present in the file.

    Note the two sources are not interchangeable: console exports are 14-day
    attribution, while Sponsored Products from the API is 30-day. Replacing SP
    data therefore changes its basis and will not match previously stored rows.
    """
    if ad_product not in _REPORT_SPECS:
        raise HTTPException(status_code=400, detail=f"ad_product must be one of {list(_REPORT_SPECS)}")
    if mode not in ("fill_missing", "replace"):
        raise HTTPException(status_code=400, detail="mode must be 'fill_missing' or 'replace'")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    def _work():
        _ensure_indexes_sync(db)
        rows, meta = _parse_console_export_sync(db, content, ad_product)
        if not rows:
            return {"status": "no_rows", **meta}

        file_dates = sorted({r["date"] for r in rows})
        existing = set(
            db[DAILY_COLLECTION].distinct(
                "date",
                {"ad_product": ad_product, "profile_id": _ADS_PROFILE_ID,
                 "date": {"$gte": file_dates[0], "$lte": file_dates[-1]}},
            )
        )

        if mode == "fill_missing":
            target = [r for r in rows if r["date"] not in existing]
            skipped_dates = sorted(existing & set(file_dates))
        else:
            target = rows
            skipped_dates = []

        stored = _store_rows_sync(db, target)
        _store_campaign_meta_sync(db, target)

        db[SYNC_RUNS_COLLECTION].insert_one({
            "started_at": datetime.now(timezone.utc),
            "finished_at": datetime.now(timezone.utc),
            "status": "completed",
            "source": "console_upload",
            "mode": mode,
            "ad_products": [ad_product],
            "filename": file.filename,
            "uploaded_by": uploaded_by,
            "start_date": file_dates[0],
            "end_date": file_dates[-1],
            "total_rows": len(target),
            "profile_id": _ADS_PROFILE_ID,
        })

        return {
            "status": "ok",
            "ad_product": ad_product,
            "mode": mode,
            "file_dates": f"{file_dates[0]} .. {file_dates[-1]}",
            "rows_in_file": len(rows),
            "rows_written": len(target),
            "stored": stored,
            "dates_written": sorted({r["date"] for r in target}),
            "dates_skipped_already_present": skipped_dates,
            **meta,
        }

    return await asyncio.to_thread(_work)


@router.get("/report-jobs")
async def list_report_jobs(
    status_filter: str | None = Query(None, alias="status"),
    limit: int = Query(50, le=200),
    db=Depends(get_database),
):
    """Queued/collected/failed report jobs — shows what is still building."""
    def _fetch():
        q = {"profile_id": _ADS_PROFILE_ID}
        if status_filter:
            q["status"] = status_filter
        return list(
            db[REPORT_JOBS_COLLECTION].find(q).sort([("created_at", DESCENDING), ("_id", DESCENDING)]).limit(limit)
        )

    return {"jobs": serialize_mongo_document(await asyncio.to_thread(_fetch))}


@router.get("/sync-runs")
async def list_sync_runs(limit: int = Query(20, le=100), db=Depends(get_database)):
    def _fetch():
        return list(
            db[SYNC_RUNS_COLLECTION].find().sort([("started_at", DESCENDING), ("_id", DESCENDING)]).limit(limit)
        )

    return {"runs": serialize_mongo_document(await asyncio.to_thread(_fetch))}


def _date_match(start_date: str, end_date: str, ad_product: str | None) -> dict:
    m = {"date": {"$gte": start_date, "$lte": end_date}, "profile_id": _ADS_PROFILE_ID}
    if ad_product:
        m["ad_product"] = ad_product
    return m


_METRIC_SUMS = {
    "impressions": {"$sum": "$impressions"},
    "clicks": {"$sum": "$clicks"},
    "cost": {"$sum": "$cost"},
    "orders": {"$sum": "$orders"},
    "sales": {"$sum": "$sales"},
    "units": {"$sum": "$units"},
}


def _derive(d: dict) -> dict:
    """ACOS/ROAS/CTR/CPC must be computed from summed totals, never averaged."""
    imp, clk, cost, sales = d.get("impressions", 0), d.get("clicks", 0), d.get("cost", 0), d.get("sales", 0)
    d["ctr"] = round(clk / imp * 100, 2) if imp else 0
    d["cpc"] = round(cost / clk, 2) if clk else 0
    d["acos"] = round(cost / sales * 100, 2) if sales else None
    d["roas"] = round(sales / cost, 2) if cost else None
    d["cost"] = round(cost, 2)
    d["sales"] = round(sales, 2)
    return d


@router.get("/summary")
async def get_summary(
    start_date: str = Query(...),
    end_date: str = Query(...),
    ad_product: str | None = Query(None),
    db=Depends(get_database),
):
    """KPI totals plus a per-ad-product breakdown."""
    def _fetch():
        match = _date_match(start_date, end_date, ad_product)
        totals = list(db[DAILY_COLLECTION].aggregate([
            {"$match": match},
            {"$group": {"_id": None, **_METRIC_SUMS, "campaigns": {"$addToSet": "$campaign_id"}}},
        ]))
        by_product = list(db[DAILY_COLLECTION].aggregate([
            {"$match": match},
            {"$group": {"_id": "$ad_product", **_METRIC_SUMS, "campaigns": {"$addToSet": "$campaign_id"}}},
            {"$sort": {"cost": -1}},
        ]))
        return totals, by_product

    totals, by_product = await asyncio.to_thread(_fetch)

    t = totals[0] if totals else {}
    total_doc = _derive({k: t.get(k, 0) for k in _METRIC_SUMS})
    total_doc["campaign_count"] = len(t.get("campaigns", []))

    products = []
    for p in by_product:
        d = _derive({k: p.get(k, 0) for k in _METRIC_SUMS})
        d["ad_product"] = p["_id"]
        d["campaign_count"] = len(p.get("campaigns", []))
        products.append(d)

    return {"start_date": start_date, "end_date": end_date, "totals": total_doc, "by_ad_product": products}


@router.get("/daily")
async def get_daily(
    start_date: str = Query(...),
    end_date: str = Query(...),
    ad_product: str | None = Query(None),
    db=Depends(get_database),
):
    """Per-day time series for charting."""
    def _fetch():
        return list(db[DAILY_COLLECTION].aggregate([
            {"$match": _date_match(start_date, end_date, ad_product)},
            {"$group": {"_id": "$date", **_METRIC_SUMS}},
            {"$sort": {"_id": 1}},
        ]))

    rows = await asyncio.to_thread(_fetch)
    return {"series": [_derive({**{k: r.get(k, 0) for k in _METRIC_SUMS}, "date": r["_id"]}) for r in rows]}


@router.get("/campaigns")
async def get_campaigns(
    start_date: str = Query(...),
    end_date: str = Query(...),
    ad_product: str | None = Query(None),
    search: str | None = Query(None),
    sort_by: str = Query("cost"),
    sort_dir: int = Query(-1),
    page: int = Query(1, ge=1),
    limit: int = Query(50, le=500),
    db=Depends(get_database),
):
    """Per-campaign aggregation over the window, paginated."""
    allowed_sort = {"cost", "sales", "impressions", "clicks", "orders", "units", "campaign_name"}
    if sort_by not in allowed_sort:
        raise HTTPException(status_code=400, detail=f"sort_by must be one of {sorted(allowed_sort)}")

    def _fetch():
        match = _date_match(start_date, end_date, ad_product)
        if search:
            match["campaign_name"] = {"$regex": search, "$options": "i"}
        base = [
            {"$match": match},
            {"$group": {
                "_id": "$campaign_id",
                "campaign_name": {"$last": "$campaign_name"},
                "campaign_status": {"$last": "$campaign_status"},
                "ad_product": {"$last": "$ad_product"},
                **_METRIC_SUMS,
            }},
            # _id as tiebreaker keeps pagination stable across pages.
            {"$sort": {sort_by: sort_dir, "_id": -1}},
        ]
        total = list(db[DAILY_COLLECTION].aggregate(base + [{"$count": "n"}]))
        rows = list(db[DAILY_COLLECTION].aggregate(
            base + [{"$skip": (page - 1) * limit}, {"$limit": limit}]
        ))
        return (total[0]["n"] if total else 0), rows

    total, rows = await asyncio.to_thread(_fetch)
    out = []
    for r in rows:
        d = _derive({k: r.get(k, 0) for k in _METRIC_SUMS})
        d.update({
            "campaign_id": r["_id"],
            "campaign_name": r.get("campaign_name"),
            "campaign_status": r.get("campaign_status"),
            "ad_product": r.get("ad_product"),
        })
        out.append(d)
    return {"campaigns": out, "total": total, "page": page, "limit": limit,
            "total_pages": (total + limit - 1) // limit if limit else 0}


@router.get("/download")
async def download_report(
    start_date: str = Query(...),
    end_date: str = Query(...),
    ad_product: str | None = Query(None),
    db=Depends(get_database),
):
    """Excel export: Summary, Campaigns and Daily sheets."""
    import pandas as pd

    def _fetch():
        match = _date_match(start_date, end_date, ad_product)
        campaigns = list(db[DAILY_COLLECTION].aggregate([
            {"$match": match},
            {"$group": {
                "_id": "$campaign_id",
                "campaign_name": {"$last": "$campaign_name"},
                "campaign_status": {"$last": "$campaign_status"},
                "ad_product": {"$last": "$ad_product"},
                **_METRIC_SUMS,
            }},
            {"$sort": {"cost": -1, "_id": -1}},
        ]))
        daily = list(db[DAILY_COLLECTION].aggregate([
            {"$match": match},
            {"$group": {"_id": {"date": "$date", "ad_product": "$ad_product"}, **_METRIC_SUMS}},
            {"$sort": {"_id.date": 1}},
        ]))
        by_product = list(db[DAILY_COLLECTION].aggregate([
            {"$match": match},
            {"$group": {"_id": "$ad_product", **_METRIC_SUMS}},
            {"$sort": {"cost": -1}},
        ]))
        return campaigns, daily, by_product

    campaigns, daily, by_product = await asyncio.to_thread(_fetch)

    def _fmt(rows, id_map):
        out = []
        for r in rows:
            d = _derive({k: r.get(k, 0) for k in _METRIC_SUMS})
            out.append({**id_map(r), "Impressions": d["impressions"], "Clicks": d["clicks"],
                        "Spend": d["cost"], "Orders": d["orders"], "Sales": d["sales"],
                        "Units": d["units"], "CTR %": d["ctr"], "CPC": d["cpc"],
                        "ACOS %": d["acos"], "ROAS": d["roas"]})
        return out

    summary_rows = _fmt(by_product, lambda r: {"Ad Product": r["_id"]})
    campaign_rows = _fmt(campaigns, lambda r: {
        "Campaign ID": r["_id"], "Campaign Name": r.get("campaign_name"),
        "Ad Product": r.get("ad_product"), "Status": r.get("campaign_status")})
    daily_rows = _fmt(daily, lambda r: {"Date": r["_id"]["date"], "Ad Product": r["_id"]["ad_product"]})

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows or [{}]).to_excel(writer, sheet_name="Summary", index=False)
        pd.DataFrame(campaign_rows or [{}]).to_excel(writer, sheet_name="Campaigns", index=False)
        pd.DataFrame(daily_rows or [{}]).to_excel(writer, sheet_name="Daily", index=False)
    buf.seek(0)

    fname = f"amazon_ads_{start_date}_to_{end_date}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )
