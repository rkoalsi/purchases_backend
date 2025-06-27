from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, status, Depends
from dotenv import load_dotenv
from fastapi.responses import JSONResponse
from jose import jwt
import os
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
from ..database import get_database, serialize_mongo_document

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES")

# --- Password Hashing Setup ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

load_dotenv()  # Load environment variables


# Change this from FastAPI() to APIRouter()
router = APIRouter()


class User(BaseModel):
    email: str
    password: str


# --- Password Verification Helper ---
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password."""
    return pwd_context.verify(plain_password, hashed_password)


# --- JWT Creation Helper ---
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    """Creates a JWT access token."""
    if not SECRET_KEY or not ALGORITHM:
        raise ValueError("JWT SECRET_KEY or ALGORITHM not configured")

    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        # Default expiration if none provided (optional)
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)

    to_encode.update({"exp": expire})  # Add expiration claim
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@router.post("/login")
async def login(data: User, database=Depends(get_database)):
    """Handles user login and returns a JWT on success."""
    email = str(data.email).strip()
    password = data.password

    # 1. Find the user by email
    user = database["purchase_users"].find_one({"email": email})

    if not user:
        # Raise 401 Unauthorized for incorrect credentials
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Verify the password
    stored_hashed_password = user.get("password")
    if not stored_hashed_password or not verify_password(
        password, stored_hashed_password
    ):
        # Raise 401 Unauthorized for incorrect credentials
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 3. Password is correct, create JWT
    if not ACCESS_TOKEN_EXPIRE_MINUTES:
        raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES not configured")

    try:
        access_token_expires = timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES))
    except ValueError:
        raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES must be an integer")

    access_token_data = {"sub": user["email"]}

    access_token = create_access_token(
        data=access_token_data, expires_delta=access_token_expires
    )

    # Convert ObjectId to string before returning
    user["_id"] = str(user["_id"])

    # 4. Return the token and user info
    return JSONResponse(
        content={
            "access_token": access_token,
            "user": serialize_mongo_document(user),
            "token_type": "bearer",
        }
    )
