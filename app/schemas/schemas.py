from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=8, max_length=128)


class UserResponse(BaseModel):
    id: int
    username: str

    model_config = ConfigDict(from_attributes=True)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class PasswordResetRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32)


class PasswordResetConfirm(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    code: str = Field(min_length=6, max_length=16)
    new_password: str = Field(min_length=8, max_length=128)


class MessageOut(BaseModel):
    id: int
    username: str
    text: str
    file_url: str | None = None
    thumbnail_url: str | None = None
    file_name: str | None = None
    file_content_type: str | None = None
    file_size: int | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
