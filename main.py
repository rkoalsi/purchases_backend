from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.auth import router as auth_router
from .routes.users import router as user_router
from .routes.amazon import router as amazon_router
from .routes.blinkit import router as blinkit_router
from .routes.blinkit_ads import router as blinkit_ads_router
from .routes.dashboard import router as dashboard_router
from .routes.zoho import router as zoho_router
from .routes.master import router as master_router
from .routes.util import router as util_router
from .routes.workflow import router as workflow_router
from .database import connect_db, close_db
from contextlib import asynccontextmanager
from .helpers.scheduler import scheduler, api_scheduler, setup_scheduler
import logging
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info("Starting up application...")
    
    # Initialize database
    connect_db()
    
    # Initialize HTTP client for scheduler
    await api_scheduler.initialize_client()
    
    # Setup and start scheduler
    setup_scheduler()
    scheduler.start()
    logger.info("Scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    # Shutdown scheduler
    scheduler.shutdown()
    await api_scheduler.close_client()
    
    # Close database
    close_db()
    
    logger.info("Application shutdown complete")

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:8000",
    "https://127.0.0.1:8000",
    "http://purchase.pupscribe.in",
    "https://purchase.pupscribe.in",  
    "http://localhost",  
    "https://localhost", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return "Purchase Backend Running Successfully"


# --- Include Routers ---
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(user_router, prefix="/users", tags=["user"])
app.include_router(blinkit_router, prefix="/blinkit", tags=["blinkit"])
app.include_router(blinkit_ads_router, prefix="/blinkit_ads", tags=["blinkit_ads"])
app.include_router(amazon_router, prefix="/amazon", tags=["amazon"])
app.include_router(zoho_router, prefix="/zoho", tags=["zoho"])
app.include_router(master_router, prefix="/master", tags=["master"])
app.include_router(util_router, prefix="/util", tags=["util"])
app.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])
app.include_router(workflow_router, prefix="/workflows", tags=["workflow"])