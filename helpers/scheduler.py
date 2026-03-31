from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from ..database import get_database
import requests
from bson import ObjectId
from typing import Dict

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_URL = os.getenv("SLACK_URL")


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
                    "text": f"*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n*Job:* {title}",
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

    async def get_vc_sales_traffic(self):
        try:
            logger.info("Executing Sales Traffic API call...")
            response = await self.client.post(f"/amazon/vendor/sync/cron/sales")
            response.raise_for_status()
            logger.info(f"Sales Traffic API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Sales Traffic API call failed: {str(e)}")
            raise

    async def get_vc_inventory(self):
        try:
            logger.info("Executing Inventory Ledger API call...")
            start_date = (datetime.now().date() - timedelta(days=2)).strftime(
                "%Y-%m-%d"
            )
            response = await self.client.post(
                f"/amazon/vendor/sync/inventory?start_date={start_date}&end_date={start_date}"
            )
            response.raise_for_status()
            logger.info(f"Inventory Ledger API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Inventory Ledger API call failed: {str(e)}")
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
                            product = db.products.find_one({"name": c["name"]})
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
            result = collection.delete_many({})
            logger.info(
                f"Deleted {result.deleted_count} existing composite product records"
            )

            # Insert new data
            result = collection.insert_many(composite_items)
            logger.info(
                f"Inserted {len(result.inserted_ids)} new composite product records"
            )

            # Create index on composite_item_id for better performance
            collection.create_index("composite_item_id", unique=True)
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
            ("VC Vendor Inventory", self.get_vc_inventory),
            ("VC Vendor Sales", self.get_vc_sales_traffic),
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
            include_zoho=True,
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

    # Daily draft order report at 11 AM IST (05:30 UTC)
    scheduler.add_job(
        scheduled_draft_order_report,
        trigger=CronTrigger(hour=6, minute=15, timezone="UTC"),
        id="daily_draft_order_report",
        name="Daily Draft Order Slack Report",
        replace_existing=True,
        misfire_grace_time=300,
    )

    logger.info(
        "Scheduler configured successfully with daily, weekly, and composite items tasks"
    )

