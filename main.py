import os
import secrets
from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
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
from .routes.missed_sales import router as missed_sales_router
from .routes.seasonal import router as seasonal_router
from .routes.amazon_listing_validation import router as amazon_listing_validation_router
from .routes.bb_code_generator import router as bb_code_generator_router
from .routes.vendor_po import router as vendor_po_router
from .routes.inventory_aging import router as inventory_aging_router
from .routes.vendor import router as vendor_router
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

app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

_docs_security = HTTPBasic()


def _verify_docs_credentials(credentials: HTTPBasicCredentials = Depends(_docs_security)):
    docs_username = os.getenv("DOCS_USERNAME", "")
    docs_password = os.getenv("DOCS_PASSWORD", "")
    if not docs_username or not docs_password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Docs credentials not configured",
        )
    valid_username = secrets.compare_digest(credentials.username.encode(), docs_username.encode())
    valid_password = secrets.compare_digest(credentials.password.encode(), docs_password.encode())
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

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
app.include_router(missed_sales_router, prefix="/missed_sales", tags=["missed_sales"])
app.include_router(seasonal_router, prefix="/seasonal", tags=["seasonal"])
app.include_router(amazon_listing_validation_router, prefix="/amazon_listing_validation", tags=["amazon_listing_validation"])
app.include_router(bb_code_generator_router, prefix="/bb_code_generator", tags=["bb_code_generator"])
app.include_router(vendor_po_router, prefix="/vendor_po", tags=["vendor_po"])
app.include_router(inventory_aging_router, prefix="/inventory_aging", tags=["inventory_aging"])
app.include_router(vendor_router, prefix="/vendors", tags=["vendors"])


# --- Protected Swagger / ReDoc / OpenAPI schema ---
@app.get("/docs", include_in_schema=False, dependencies=[Depends(_verify_docs_credentials)])
async def swagger_ui():
    return get_swagger_ui_html(openapi_url="openapi.json", title="Pupscribe API Docs")


@app.get("/redoc", include_in_schema=False, dependencies=[Depends(_verify_docs_credentials)])
async def redoc_ui():
    return get_redoc_html(openapi_url="openapi.json", title="Pupscribe API Docs")


@app.get("/openapi.json", include_in_schema=False, dependencies=[Depends(_verify_docs_credentials)])
async def openapi_schema():
    return JSONResponse(app.openapi())