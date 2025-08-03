from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta

load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = AsyncIOScheduler()

class APIScheduler:
    def __init__(self, base_url: str = os.getenv('BASE_URL')):
        self.base_url = base_url
        self.client = None
    
    async def initialize_client(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
    
    async def close_client(self):
        if self.client:
            await self.client.aclose()
    
    async def get_sales_traffic(self):
        try:
            logger.info("Executing Sales Traffic API call...")
            start_date = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
            response = await self.client.post(f"/amazon/sync/sales-traffic?start_date={start_date}&end_date={start_date}")
            print(response.json())
            response.raise_for_status()
            logger.info(f"Sales Traffic API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Sales Traffic API call failed: {str(e)}")
            raise
    
    async def get_inventory_ledger(self):
        try:
            logger.info("Executing Inventory Ledger API call...")
            start_date = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
            response = await self.client.post(f"/amazon/sync/ledger?start_date={start_date}&end_date={start_date}")
            print(response.json())
            response.raise_for_status()
            logger.info(f"Inventory Ledger API call successful: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Inventory Ledger API call failed: {str(e)}")
            raise
    
    async def daily_task_execution(self):
        try:
            logger.info(f"Starting daily task execution at {datetime.now()}")
            
            await self.get_sales_traffic()
            
            await self.get_inventory_ledger()
            
            logger.info("Daily task execution completed successfully")
            
        except Exception as e:
            logger.error(f"Daily task execution failed: {str(e)}")

api_scheduler = APIScheduler()

async def scheduled_daily_task():
    """Wrapper function for the scheduled task"""
    await api_scheduler.daily_task_execution()

def setup_scheduler():
    """Configure and start the scheduler"""
    scheduler.add_job(
        scheduled_daily_task,
        trigger=CronTrigger(hour=0, minute=0, timezone='UTC'),
        id="daily_api_calls",
        name="Daily API Calls",
        replace_existing=True,
        misfire_grace_time=300  # Allow 5 minutes grace time if the system is busy
    )
    
    logger.info("Scheduler configured successfully")

