"""Audit trail for Vendor Central POs.

Every mutating request on the `/vendor_po` router is recorded in the
`vendor_po_history` collection: who did it (JWT `sub` → purchase_users), what
changed (field, old → new), when, and whether it succeeded.

Wiring: `router = APIRouter(dependencies=[Depends(record_po_history)])` in
`vendor_po.py`. Endpoints whose PO number is not in the path (`/upload`,
`/bulk_update`, `/shipment_summary/upload`) log themselves via `log_po_history`.
"""

from datetime import datetime
from fastapi import Header, HTTPException, Request
from jose import jwt, JWTError
import asyncio
import logging
import os

logger = logging.getLogger(__name__)

HISTORY_COLLECTION = "vendor_po_history"
USERS_COLLECTION = "purchase_users"
PO_COLLECTION = "vendor_purchase_orders"

_MUTATING_METHODS = {"POST", "PATCH", "PUT", "DELETE"}

# Last path segment → item field actually stored on the PO item, so the audit row
# can show old → new (the URL name and the stored key often differ, e.g.
# open_qty → open_po_override).
_ITEM_FIELD_KEYS = {
    "accepted_qty": "accepted_qty",
    "received_qty": "received_qty",
    "open_qty": "open_po_override",
    "supply_qty": "supply_qty_override",
    "lead_time": "lead_time_override",
    "coverage_days": "coverage_days_override",
    "final_units": "final_units_override",
    "final_supply_fo": "final_supply_fo_override",
    "etrade_unit_cost": "etrade_unit_cost",
}

# Last path segment → PO-level field, for the same old → new capture.
_PO_FIELD_KEYS = {
    "status": "po_status",
    "received_qty": "received_qty",
    "sales_order_no": "sales_order_no",
    "package": "package_number",
    "transfer_order": "transfer_order_number",
}

# (method, last path segment) → human label shown in the History tab.
_ACTION_LABELS = {
    ("DELETE", ""): "Deleted PO",
    ("POST", "refresh-zoho-stock"): "Refreshed Zoho stock",
    ("PATCH", "status"): "Changed PO status",
    ("PATCH", "received_qty"): "Set PO received qty",
    ("PATCH", "shipment"): "Updated shipment details",
    ("POST", "upload_order"): "Uploaded order file",
    ("DELETE", "order_file"): "Deleted order file",
    ("POST", "upload_invoice"): "Uploaded invoice file",
    ("DELETE", "invoice_file"): "Deleted invoice file",
    ("POST", "estimate"): "Created estimate",
    ("PUT", "estimate"): "Updated estimate in Zoho",
    ("PATCH", "estimate"): "Linked estimate",
    ("DELETE", "estimate"): "Unlinked estimate",
    ("PATCH", "package"): "Linked package",
    ("DELETE", "package"): "Unlinked package",
    ("PATCH", "sales_order_no"): "Linked sales order",
    ("POST", "transfer_order"): "Created transfer order",
    ("PATCH", "transfer_order"): "Linked transfer order",
    ("DELETE", "transfer_order"): "Unlinked transfer order",
    ("POST", "assemblies"): "Created assemblies",
    ("PATCH", "assemblies"): "Linked assembly",
    ("DELETE", "assemblies"): "Unlinked assemblies",
}

_ITEM_FIELD_LABELS = {
    "accepted_qty": "Accepted Qty",
    "received_qty": "Received Qty",
    "open_qty": "Open PO Override",
    "supply_qty": "Supply Qty Override",
    "lead_time": "Lead Time",
    "coverage_days": "Coverage Days",
    "final_units": "Final Units",
    "final_supply_fo": "Final Supply FO",
    "etrade_unit_cost": "eTrade Unit Cost",
}

# Query params that carry the new value are echoed; nothing sensitive is expected
# on this router, but keep obvious secrets out of the audit row regardless.
_SKIP_PARAM_KEYS = {"token", "access_token", "authorization", "password"}


def extract_email_from_token(authorization: str | None) -> str | None:
    """Return email (`sub`) from a Bearer JWT, or None if missing/invalid."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ", 1)[1]
    try:
        secret = os.getenv("SECRET_KEY") or ""
        algo = os.getenv("ALGORITHM") or "HS256"
        payload = jwt.decode(
            token,
            secret,
            algorithms=[algo],
            options={"verify_aud": False, "verify_exp": False},
        )
        return payload.get("sub")
    except (JWTError, Exception):
        return None


def _actor_name(db, email: str | None) -> str | None:
    if not email:
        return None
    try:
        user = db[USERS_COLLECTION].find_one({"email": email}, {"name": 1})
    except Exception:
        return None
    return (user or {}).get("name")


_indexes_ready = False


def ensure_history_indexes(db) -> None:
    """Idempotent, runs once per process."""
    global _indexes_ready
    if _indexes_ready:
        return
    try:
        db[HISTORY_COLLECTION].create_index([("po_number", 1), ("created_at", -1)])
        db[HISTORY_COLLECTION].create_index([("created_at", -1)])
    except Exception:
        logger.warning("Could not create vendor_po_history indexes", exc_info=True)
    _indexes_ready = True


def log_po_history(
    db,
    po_number: str,
    action: str,
    label: str,
    *,
    actor_email: str | None = None,
    asin: str | None = None,
    field: str | None = None,
    old_value=None,
    new_value=None,
    details: dict | None = None,
    method: str | None = None,
    path: str | None = None,
    status: str = "success",
    error: str | None = None,
) -> None:
    """Insert one audit row. Never raises — auditing must not break the request."""
    try:
        ensure_history_indexes(db)
        doc = {
            "po_number": po_number,
            "action": action,
            "label": label,
            "method": method,
            "path": path,
            "actor_email": actor_email,
            "actor_name": _actor_name(db, actor_email),
            "asin": asin,
            "field": field,
            "old_value": old_value,
            "new_value": new_value,
            "details": details or {},
            "status": status,
            "error": error,
            "created_at": datetime.now(),
        }
        db[HISTORY_COLLECTION].insert_one(doc)
    except Exception:
        logger.warning("Failed to write vendor PO history for %s", po_number, exc_info=True)


def _read_old_value(db, po_number: str, asin: str | None, segment: str):
    """Best-effort read of the value being replaced, before the endpoint runs."""
    try:
        if asin and segment in _ITEM_FIELD_KEYS:
            key = _ITEM_FIELD_KEYS[segment]
            doc = db[PO_COLLECTION].find_one(
                {"po_number": po_number, "items.asin": asin},
                {"items": {"$elemMatch": {"asin": asin}}},
            )
            items = (doc or {}).get("items") or []
            return items[0].get(key) if items else None
        if not asin and segment in _PO_FIELD_KEYS:
            key = _PO_FIELD_KEYS[segment]
            doc = db[PO_COLLECTION].find_one({"po_number": po_number}, {key: 1})
            return (doc or {}).get(key)
    except Exception:
        logger.debug("Old-value read failed for %s/%s", po_number, segment, exc_info=True)
    return None


def _describe(method: str, segment: str, asin: str | None, new_value) -> tuple[str, str]:
    """Return (action, human label) for an audited request."""
    action = segment or ("po" if method == "DELETE" else "update")
    label = _ACTION_LABELS.get((method, segment))
    if label:
        return action, label
    if asin and segment in _ITEM_FIELD_LABELS:
        return action, f"Updated {_ITEM_FIELD_LABELS[segment]}"
    return action, f"{method} {segment}".strip()


def _coerce(value):
    """Query params arrive as strings — store numbers as numbers so old → new compares cleanly."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return value


def _serialisable(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _serialisable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialisable(v) for v in value][:50]
    return str(value)


async def record_po_history(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Router-level dependency: audit every mutating `/vendor_po/{po_number}/…` call.

    Requests without a `po_number` path param (`/upload`, `/bulk_update`,
    `/shipment_summary/upload`) are skipped here and logged inside the endpoint,
    since their PO numbers are only known after the workbook is parsed.
    """
    method = request.method.upper()
    po_number = (request.path_params or {}).get("po_number")
    if method not in _MUTATING_METHODS or not po_number:
        yield
        return

    from ..database import get_database  # local import: avoids a circular import

    asin = (request.path_params or {}).get("asin")
    route_path = getattr(request.scope.get("route"), "path", request.url.path)
    segment = route_path.rstrip("/").rsplit("/", 1)[-1]
    if segment.startswith("{"):  # e.g. /{po_number} or /package/{pkg_number}
        segment = "" if "po_number" in segment else route_path.rstrip("/").split("/")[-2]

    params = {
        k: v for k, v in request.query_params.items() if k.lower() not in _SKIP_PARAM_KEYS
    }
    body = None
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        try:
            body = await request.json()  # cached by Starlette; endpoint re-reads it
        except Exception:
            body = None
    elif "multipart/form-data" in content_type:
        body = {"file": "<uploaded file>"}

    new_value = None
    if segment and segment in params:
        new_value = params[segment]
    elif isinstance(body, dict) and segment in body:
        new_value = body.get(segment)
    elif len(params) == 1:
        # e.g. PATCH /{po}/status?po_status=packed — param name ≠ path segment
        new_value = next(iter(params.values()))
    new_value = _coerce(new_value)

    db = get_database()
    old_value = await asyncio.to_thread(_read_old_value, db, po_number, asin, segment)

    action, label = _describe(method, segment, asin, new_value)
    status, error = "success", None
    try:
        yield
    except HTTPException as exc:
        status, error = "failed", str(exc.detail)
        raise
    except Exception as exc:
        status, error = "failed", str(exc)
        raise
    finally:
        # Endpoints can enrich their audit row (e.g. uploaded filename) via request.state
        extra = getattr(request.state, "audit_details", None) or {}
        details = {k: v for k, v in {"params": params, "body": _serialisable(body)}.items() if v}
        details.update(_serialisable(extra))
        await asyncio.to_thread(
            log_po_history,
            db,
            po_number,
            action,
            label,
            actor_email=extract_email_from_token(authorization),
            asin=asin,
            field=_ITEM_FIELD_KEYS.get(segment) if asin else _PO_FIELD_KEYS.get(segment),
            old_value=_serialisable(old_value),
            new_value=_serialisable(new_value),
            details=details,
            method=method,
            path=request.url.path,
            status=status,
            error=error,
        )
