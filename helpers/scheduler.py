import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta, timezone
from ..database import get_database
import requests
from bson import ObjectId
from typing import Dict

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_URL = os.getenv("SLACK_URL")

# Per-department Slack webhook URLs — unmapped departments fall back to SLACK_URL_PURCHASE
SLACK_DEPT_URLS: dict = {
    "design": os.getenv("SLACK_URL_DESIGN"),
    "purchase": os.getenv("SLACK_URL_PURCHASE"),
}


def send_slack_notification(
    title: str, success: bool = True, details: Dict = None, error_msg: str = None
):
    """Send a Slack notification for cron job success or failure."""
    if not SLACK_URL:
        logger.warning("SLACK_URL not configured, skipping notification")
        return

    try:
        emoji = ":white_check_mark:" if success else ":x:"
        status = "✅ SUCCESS" if success else "❌ FAILED"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title} - {status}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Time:* {(datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d %H:%M:%S IST')}\n*Job:* {title}",
                },
            },
        ]

        if success and details:
            detail_text = ""
            if "processed" in details:
                detail_text += f"*Items Processed:* {details['processed']}\n"
            if "inserted" in details:
                detail_text += f"*New Records:* {details['inserted']}\n"
            if "duration" in details:
                detail_text += f"*Duration:* {details['duration']:.1f}s\n"
            if detail_text:
                blocks.append(
                    {"type": "section", "text": {"type": "mrkdwn", "text": detail_text.strip()}}
                )

        if not success and error_msg:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:* ```{error_msg[:500]}```",
                    },
                }
            )

        response = requests.post(SLACK_URL, json={"blocks": blocks}, timeout=10)
        if response.status_code != 200:
            logger.error(f"Slack notification failed: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error sending Slack notification: {e}")

def send_task_assignment_notification(
    task_title: str,
    created_by_name: str,
    assigned_by_name: str,
    new_assignees: list,  # [{"name": str, "department": str}]
):
    """Send Slack notification to per-department channels when a task is assigned."""
    if not new_assignees:
        return

    assignee_names = ", ".join(a["name"] for a in new_assignees)
    assignee_depts = ", ".join(
        filter(None, {(a.get("department") or "").title() for a in new_assignees})
    ) or "—"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":clipboard: Task Assigned: {task_title[:70]}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Created by:*\n{created_by_name}"},
                {"type": "mrkdwn", "text": f"*Assigned by:*\n{assigned_by_name}"},
                {"type": "mrkdwn", "text": f"*Assigned to:*\n{assignee_names}"},
                {"type": "mrkdwn", "text": f"*Department(s):*\n{assignee_depts}"},
            ],
        },
    ]

    # Collect unique department webhook URLs
    urls_to_notify: set = set()
    unmapped_depts = False
    for assignee in new_assignees:
        dept_key = (assignee.get("department") or "").lower()
        url = SLACK_DEPT_URLS.get(dept_key)
        if url:
            urls_to_notify.add(url)
        else:
            unmapped_depts = True

    # Fall back to purchase channel for any unmapped department
    if unmapped_depts:
        fallback = os.getenv("SLACK_URL_PURCHASE") or SLACK_URL
        if fallback:
            urls_to_notify.add(fallback)

    for url in urls_to_notify:
        try:
            requests.post(url, json={"blocks": blocks}, timeout=10)
        except Exception as e:
            logger.error(f"Task assignment Slack notification failed: {e}")


# Global scheduler instance
scheduler = AsyncIOScheduler()


class APIScheduler:
    def __init__(self, base_url: str = os.getenv("BASE_URL")):
        self.base_url = base_url
        self.client = None

        # Composite items configuration
        self.composite_items_url = os.getenv("COMPOSITE_ITEMS_URL")
        self.books_url = os.getenv("BOOKS_URL")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.org_id = os.getenv("ORGANIZATION_ID")
        self.books_refresh_token = os.getenv("BOOKS_REFRESH_TOKEN")
        self.access_token = None

    async def get_access_token(self):
        r = requests.post(
            self.books_url.format(
                clientId=self.client_id,
                clientSecret=self.client_secret,
                grantType="refresh_token",
                books_refresh_token=self.books_refresh_token,
            )
        )
        self.access_token = str(r.json()["access_token"])
        self.books_headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json",
        }

    async def initialize_client(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def close_client(self):
        if self.client:
            await self.client.aclose()

    async def get_sc_sales_traffic(self):
        try:
            logger.info("Executing Sales Traffic API call...")
            start_date = (datetime.now().date() - timedelta(days=2)).strftime(
                "%Y-%m-%d"
            )
            response = await self.client.post(
                f"/amazon/sync/sales-traffic?start_date={start_date}&end_date={start_date}"
            )
            response.raise_for_status()
            logger.info(f"Sales Traffic API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Sales Traffic API call failed: {str(e)}")
            raise

    async def get_sc_inventory(self):
        try:
            logger.info("Executing Inventory Ledger API call...")
            start_date = (datetime.now().date() - timedelta(days=2)).strftime(
                "%Y-%m-%d"
            )
            response = await self.client.post(
                f"/amazon/sync/ledger?start_date={start_date}&end_date={start_date}"
            )
            response.raise_for_status()
            logger.info(f"Inventory Ledger API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Inventory Ledger API call failed: {str(e)}")
            raise

    async def get_vc_initiate(self):
        """Phase 1: request VC sales + inventory reports; report IDs stored in vc_pending_reports."""
        try:
            logger.info("Executing VC Initiate Reports API call...")
            response = await self.client.post("/amazon/vendor/cron/initiate", timeout=120.0)
            response.raise_for_status()
            logger.info(f"VC Initiate Reports API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"VC Initiate Reports API call failed: {str(e)}")
            raise

    async def get_vc_collect(self):
        """Phase 2: collect completed VC reports, insert data, send Slack notification."""
        try:
            logger.info("Executing VC Collect Reports API call...")
            response = await self.client.post("/amazon/vendor/cron/collect", timeout=600.0)
            response.raise_for_status()
            data = response.json()
            logger.info(f"VC Collect Reports API call successful: {response.status_code}")

            completed = data.get("completed", 0)
            still_pending = data.get("still_pending", 0)
            failed = data.get("failed", 0)
            total_inserted = data.get("total_inserted", {})
            sales_inserted = total_inserted.get("sales", 0)
            inv_inserted = total_inserted.get("inventory", 0)

            lines = [
                f":white_check_mark: *VC Collect Reports* — done",
                f"  • Completed: {completed}  |  Still pending: {still_pending}  |  Failed: {failed}",
                f"  • Sales inserted: {sales_inserted}  |  Inventory inserted: {inv_inserted}",
            ]
            if failed:
                details = data.get("details", {})
                for f_item in details.get("failed", []):
                    lines.append(f"  :x: {f_item.get('type')} {f_item.get('date')}: `{f_item.get('error','')[:150]}`")

            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{'✅' if not failed else '⚠️'} VC Reports Collected",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Time:* {(datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d %H:%M:%S IST')}\n\n" + "\n".join(lines),
                    },
                },
            ]
            if SLACK_URL:
                import requests as _requests
                _requests.post(SLACK_URL, json={"blocks": blocks}, timeout=10)

            return data
        except Exception as e:
            logger.error(f"VC Collect Reports API call failed: {str(e)}")
            send_slack_notification("VC Collect Reports", success=False, error_msg=str(e))
            raise

    async def get_returns(self):
        try:
            logger.info("Executing Amazon Returns API call...")
            response = await self.client.post(f"/amazon/sync/daily-returns", timeout=700.0)
            response.raise_for_status()
            logger.info(
                f"returns Amazon Returns API call successful: {response.status_code}"
            )
            return response.json()
        except Exception as e:
            logger.error(f"returns Amazon Returns API call failed: {str(e)}")
            raise

    async def get_composite_items(self):
        """Get composite items from API and return structured data"""
        logger.info("Starting composite items fetch")

        # Validate required configuration
        if not all([self.composite_items_url, self.org_id, self.access_token]):
            logger.error("Missing required configuration for composite items")
            return []

        params = {"page": "1", "per_page": "200", "organization_id": self.org_id}
        try:
            # Create a separate client for Zoho API calls
            async with httpx.AsyncClient(timeout=30.0) as zoho_client:
                response = await zoho_client.get(
                    self.composite_items_url, params=params, headers=self.books_headers
                )
                response.raise_for_status()
                composite_items = response.json()["composite_items"]
                logger.info(f"Fetched {len(composite_items)} composite items")

                arr = []
                for i, item in enumerate(composite_items):
                    try:
                        logger.info(
                            f"Processing composite item {i+1}/{len(composite_items)}: {item['name']}"
                        )

                        item_response = await zoho_client.get(
                            f"{self.composite_items_url}/{item['composite_item_id']}",
                            params={"organization_id": self.org_id},
                            headers=self.books_headers,
                        )
                        item_response.raise_for_status()
                        item_data = item_response.json().get("composite_item", {})
                        components = item_data.get("composite_component_items", [])
                        db = get_database()
                        for c in components:
                            product = await asyncio.to_thread(db.products.find_one, {"name": c["name"]})
                            c["product_id"] = product.get("_id")
                            c["sku_code"] = product.get("cf_sku_code")
                        # Structure the data for MongoDB storage
                        sku_code = None
                        custom_fields = item_data.get("custom_fields", [])
                        for field in custom_fields:
                            if field.get("api_name") == "cf_sku_code":
                                sku_code = field.get("value", "")
                                break
                        composite_product = {
                            "composite_item_id": item["composite_item_id"],
                            "name": item["name"],
                            "sku_code": sku_code,
                            "components": [
                                {
                                    "name": c["name"],
                                    "quantity": c["quantity"],
                                    "item_id": c["item_id"],
                                    "sku_code": c["sku_code"],
                                    "product_id": ObjectId(c["product_id"]),
                                }
                                for c in components
                            ],
                            "rate":item['rate'],
                            "last_updated": datetime.now(),
                            "created_at": datetime.now(),
                        }

                        arr.append(composite_product)

                    except Exception as e:
                        logger.error(
                            f"Error processing composite item {item['composite_item_id']}: {e}"
                        )

                return arr

        except Exception as e:
            logger.error(f"Error fetching composite items: {e}")
            return []

    async def update_composite_products_db(self, composite_items):
        """Update composite products in MongoDB using database.py connection"""
        if not composite_items:
            logger.warning("No composite items to update in database")
            return

        try:
            # Use the database.py connection function
            db = get_database()
            collection = db.composite_products

            # Clear existing data and insert new data
            result = await asyncio.to_thread(collection.delete_many, {})
            logger.info(
                f"Deleted {result.deleted_count} existing composite product records"
            )

            # Insert new data
            result = await asyncio.to_thread(collection.insert_many, composite_items)
            logger.info(
                f"Inserted {len(result.inserted_ids)} new composite product records"
            )

            # Create index on composite_item_id for better performance
            await asyncio.to_thread(lambda: collection.create_index("composite_item_id", unique=True))
            logger.info("Created index on composite_item_id")

        except Exception as e:
            logger.error(f"Error updating composite products in MongoDB: {e}")
            raise

    async def sync_composite_items(self, notify: bool = False):
        """Main function to sync composite items to database"""
        try:
            logger.info("Starting composite items sync")
            await self.get_access_token()
            composite_items = await self.get_composite_items()

            if not composite_items:
                logger.warning("No composite items retrieved, skipping database update")
                return

            await self.update_composite_products_db(composite_items)

            logger.info(
                f"Composite items sync completed successfully. Processed {len(composite_items)} items"
            )
            if notify:
                send_slack_notification(
                    "Purchases Backend - Composite Items Sync",
                    success=True,
                    details={"processed": len(composite_items)},
                )

        except Exception as e:
            logger.error(f"Composite items sync failed: {e}")
            if notify:
                send_slack_notification(
                    "Purchases Backend - Composite Items Sync", success=False, error_msg=str(e)
                )
            raise
    async def sync_fba_shipments(self, days: int = 3650, notify: bool = False):
        """
        Sync recent FBA shipments + items from SP-API into amazon_fba_shipments.
        Default window is 90 days (catches new + recently-updated shipments).
        CLOSED shipments already synced within 7 days are skipped for efficiency.
        """
        try:
            logger.info(f"Starting FBA shipments sync (last {days} days)...")
            from ..routes.amazon_fba_shipment import _sync_shipments_to_db
            db = get_database()
            summary = await asyncio.to_thread(_sync_shipments_to_db, db, days)
            logger.info(f"FBA shipments sync complete: {summary}")
            if notify:
                send_slack_notification(
                    "FBA Shipments Sync",
                    success=True,
                    details={
                        "processed": summary.get("total", 0),
                        "inserted": summary.get("inserted", 0),
                    },
                )
            return summary
        except Exception as e:
            logger.error(f"FBA shipments sync failed: {e}")
            if notify:
                send_slack_notification("FBA Shipments Sync", success=False, error_msg=str(e))
            raise

    async def get_settlements(self):
        try:
            logger.info("Executing Amazon Settlements API call...")
            response = await self.client.post(f"/amazon/sync/settlements?days_back=30")
            response.raise_for_status()
            logger.info(f"Settlements API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Settlements API call failed: {str(e)}")
            raise

    async def daily_task_execution(self):
        logger.info(f"Starting daily task execution at {datetime.now()}")

        tasks = [
            ("SC Inventory Ledger", self.get_sc_inventory),
            ("VC Initiate Reports", self.get_vc_initiate),
            ("Amazon Returns", self.get_returns),
            ("Amazon Settlements", self.get_settlements),
            ("SC Sales Traffic", self.get_sc_sales_traffic),
        ]

        results = {}
        for name, fn in tasks:
            try:
                await fn()
                results[name] = ("success", None)
            except Exception as e:
                logger.error(f"{name} failed: {str(e)}")
                results[name] = ("failed", str(e))

        all_success = all(status == "success" for status, _ in results.values())
        logger.info(f"Daily task execution finished. All success: {all_success}")

        # Build detailed Slack message
        lines = []
        for name, (status, err) in results.items():
            if status == "success":
                lines.append(f":white_check_mark: *{name}* — completed")
            else:
                err_snippet = f": `{err[:200]}`" if err else ""
                lines.append(f":x: *{name}* — failed{err_snippet}")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{'✅' if all_success else '❌'} Daily Sync — {'All tasks completed' if all_success else 'Some tasks failed'}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n" + "\n".join(lines),
                },
            },
        ]

        if SLACK_URL:
            try:
                import requests as _requests
                _requests.post(SLACK_URL, json={"blocks": blocks}, timeout=10)
            except Exception as e:
                logger.error(f"Error sending Slack notification: {e}")

    async def weekly_task_execution(self):
        """Weekly tasks including composite items sync"""
        try:
            logger.info(f"Starting weekly task execution at {datetime.now()}")

            # Sync composite items
            await self.sync_composite_items()

            logger.info("Weekly task execution completed successfully")
            send_slack_notification("Purchases Backend - Weekly Composite Items Sync", success=True)

        except Exception as e:
            logger.error(f"Weekly task execution failed: {str(e)}")
            send_slack_notification("Purchases Backend - Weekly Composite Items Sync", success=False, error_msg=str(e))


api_scheduler = APIScheduler()


async def scheduled_daily_task():
    """Wrapper function for the scheduled daily task"""
    await api_scheduler.daily_task_execution()


async def scheduled_weekly_task():
    """Wrapper function for the scheduled weekly task"""
    await api_scheduler.weekly_task_execution()


async def scheduled_composite_items_task():
    """Wrapper function for composite items sync - can be scheduled separately"""
    await api_scheduler.sync_composite_items(notify=True)


async def scheduled_vc_collect_task():
    """Collect completed VC reports ~5 hours after midnight initiation."""
    await api_scheduler.get_vc_collect()


async def generate_and_send_draft_order_slack_report():
    """Aggregate draft order data by brand (last 90 days) and post a summary to Slack."""
    try:
        from ..routes.master import _generate_master_report_data

        db = get_database()
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)

        report_data = await _generate_master_report_data(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            db=db,
        )

        combined_data = report_data.get("combined_data", [])

        currency_symbols = {"USD": "$", "EUR": "€", "GBP": "£", "CNY": "¥", "INR": "₹"}

        # Aggregate by brand
        brand_totals: Dict[str, Dict] = {}
        for item in combined_data:
            if not isinstance(item, dict):
                continue
            brand = item.get("brand") or "Unknown"
            if brand in ("Dogfest", "Catfest"):
                brand = "Petfest"
            total_cbm = float(item.get("total_cbm") or 0)
            qty_rounded = float(item.get("order_qty_plus_extra_qty_rounded") or 0)
            unit_price = float(item.get("unit_price") or 0)
            currency = item.get("unit_price_currency") or "USD"

            if brand not in brand_totals:
                brand_totals[brand] = {"total_cbm": 0.0, "amounts": {}}

            brand_totals[brand]["total_cbm"] += total_cbm
            brand_totals[brand]["amounts"][currency] = (
                brand_totals[brand]["amounts"].get(currency, 0.0) + qty_rounded * unit_price
            )

        def fmt_amount(currency: str, amount: float) -> str:
            sym = currency_symbols.get(currency, currency)
            return f"{sym}{amount:,.2f}"

        today_label = datetime.now().strftime("%d %b %Y")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":package: Daily Draft Order Summary — {today_label}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Period:* {start_date.strftime('%d %b %Y')} → {today_label}  (90 days)",
                },
            },
            {"type": "divider"},
        ]

        for brand in sorted(brand_totals.keys()):
            data = brand_totals[brand]
            cbm = data["total_cbm"]
            amounts = {cur: amt for cur, amt in data["amounts"].items() if amt > 0}

            if cbm == 0 and not amounts:
                continue

            amount_str = "  |  ".join(fmt_amount(cur, amt) for cur, amt in amounts.items())
            if not amount_str:
                amount_str = "—"

            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{brand}*\n• CBM: `{cbm:.4f}`\n• Total: {amount_str}",
                    },
                }
            )

        blocks.append({"type": "divider"})

        if SLACK_URL:
            response = requests.post(SLACK_URL, json={"blocks": blocks}, timeout=10)
            if response.status_code != 200:
                logger.error(
                    f"Draft order Slack report failed: {response.status_code} - {response.text}"
                )
            else:
                logger.info("Draft order Slack report sent successfully")
        else:
            logger.warning("SLACK_URL not configured, skipping draft order report")

    except Exception as e:
        logger.error(f"Error generating draft order Slack report: {e}")
        send_slack_notification("Draft Order Daily Report", success=False, error_msg=str(e))


async def scheduled_draft_order_report():
    """Wrapper for the daily draft order Slack report (11:45 AM IST = 06:15 UTC)."""
    await generate_and_send_draft_order_slack_report()


async def scheduled_fba_shipments_sync():
    """Nightly FBA shipments sync — 10-year window, skips stable CLOSED shipments."""
    await api_scheduler.sync_fba_shipments(days=3650, notify=False)


async def scheduled_sheets_update():
    """Update master + Amazon combo Google Sheets — runs at 9:30 AM, 2:00 PM, 6:30 PM IST."""
    from ..routes.sheets_updater import (
        _fetch_live_zoho_stock,
        _run_master_stock_core,
        _run_amazon_combo_status_core,
        _resolve_master_sheet_id_from_db,
    )
    try:
        db = get_database()
        master_sheet_id = await asyncio.to_thread(_resolve_master_sheet_id_from_db, db, None)
        sku_stock_map, stock_date = await asyncio.to_thread(_fetch_live_zoho_stock, db)
        master_result, combo_result = await asyncio.gather(
            _run_master_stock_core(db, master_sheet_id, sku_stock_map, stock_date),
            _run_amazon_combo_status_core(db, master_sheet_id),
        )
        master_updated = master_result.get("updated", 0)
        combo_updated = combo_result.get("updated", 0)
        send_slack_notification(
            "Sheets Updater — Master + Amazon Combo",
            success=True,
            details={"processed": master_updated + combo_updated},
        )
        logger.info("Scheduled sheets update complete: %d master + %d combo rows", master_updated, combo_updated)
    except Exception as e:
        logger.error(f"Scheduled sheets update failed: {e}")
        send_slack_notification(
            "Sheets Updater — Master + Amazon Combo", success=False, error_msg=str(e)
        )


def setup_scheduler():
    """Configure and start the scheduler"""
    # Daily tasks
    scheduler.add_job(
        scheduled_daily_task,
        trigger=CronTrigger(hour=0, minute=0, timezone="UTC"),
        id="daily_api_calls",
        name="Daily API Calls",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Optional: Separate composite items sync (runs twice daily at 6 AM and 6 PM UTC)
    scheduler.add_job(
        scheduled_composite_items_task,
        trigger=CronTrigger(hour="22", minute=0, second=0),
        id="composite_items_sync",
        name="Composite Items Sync",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # VC collect: 3 hours after midnight initiation — reports are ready by then
    scheduler.add_job(
        scheduled_vc_collect_task,
        trigger=CronTrigger(hour=3, minute=0, timezone="UTC"),
        id="vc_collect_reports",
        name="VC Collect Reports",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Daily draft order report at 11 AM IST (05:30 UTC)
    scheduler.add_job(
        scheduled_draft_order_report,
        trigger=CronTrigger(hour=4, minute=0, timezone="UTC"),
        id="daily_draft_order_report",
        name="Daily Draft Order Slack Report",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Nightly FBA shipments sync — 1:30 AM UTC (7:00 AM IST)
    scheduler.add_job(
        scheduled_fba_shipments_sync,
        trigger=CronTrigger(hour=1, minute=30, timezone="UTC"),
        id="fba_shipments_sync",
        name="FBA Shipments Sync",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # Sheets updater — 9:30 AM IST (04:00 UTC), 2:00 PM IST (08:30 UTC), 6:30 PM IST (13:00 UTC)
    for job_hour, job_minute, job_suffix in [(4, 0, "0930ist"), (8, 30, "1400ist"), (13, 0, "1830ist")]:
        scheduler.add_job(
            scheduled_sheets_update,
            trigger=CronTrigger(hour=job_hour, minute=job_minute, timezone="UTC"),
            id=f"sheets_update_{job_suffix}",
            name=f"Sheets Updater ({job_suffix.upper()})",
            replace_existing=True,
            misfire_grace_time=300,
        )

    logger.info(
        "Scheduler configured successfully with daily, weekly, composite items, FBA shipments, and sheets updater tasks"
    )

