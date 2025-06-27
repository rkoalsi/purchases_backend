from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.auth import router as auth_router
from .routes.users import router as user_router
from .routes.amazon import router as amazon_router
from .routes.blinkit import router as blinkit_router
from .routes.dashboard import router as dashboard_router
from .routes.zoho import router as zoho_router
from .routes.util import router as util_router
from .database import connect_db, close_db


app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:8000",
    "https://127.0.0.1:8000",
    "http://purchase.pupscribe.in",
    "https://purchase.pupscribe.in",  # Add this for when you enable SSL
    "http://localhost",  # For nginx proxy
    "https://localhost",  # For nginx proxy with SSL
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    print("FastAPI app startup event triggered.")
    connect_db()


@app.on_event("shutdown")
async def shutdown_event():
    print("FastAPI app shutdown event triggered.")
    close_db()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# --- Include Routers (use distinct names) ---
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(user_router, prefix="/users", tags=["user"])
app.include_router(blinkit_router, prefix="/blinkit", tags=["blinkit"])
app.include_router(amazon_router, prefix="/amazon", tags=["amazon"])
app.include_router(zoho_router, prefix="/zoho", tags=["zoho"])
app.include_router(util_router, prefix="/util", tags=["util"])
app.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])
