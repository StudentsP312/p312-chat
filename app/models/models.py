import re
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base
from app.core.security import hash_password, verify_password


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    messages: Mapped[list["Message"]] = relationship(
        "Message",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    @classmethod
    def create(cls, username: str, raw_password: str) -> "User":
        """Factory method to instantiate a new user with hashed password."""
        return cls(
            username=username.strip().lower(),
            hashed_password=hash_password(raw_password),
        )

    def set_password(self, new_password: str) -> None:
        """Update password hash."""
        self.hashed_password = hash_password(new_password)

    def check_password(self, password: str) -> bool:
        """Verify given raw password against stored hash."""
        return verify_password(password, self.hashed_password)

    def __repr__(self) -> str:
        return f"<User id={self.id} username={self.username!r}>"


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    username: Mapped[str] = mapped_column(String(32), nullable=False)
    text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    file_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    thumbnail_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
        nullable=False,
    )

    user: Mapped["User"] = relationship("User", back_populates="messages")

    @property
    def has_file(self) -> bool:
        """Whether this message contains a file attachment."""
        return bool(self.file_key)

    @property
    def has_thumbnail(self) -> bool:
        """Whether this message has an associated thumbnail."""
        return bool(self.thumbnail_key)

    @property
    def is_image(self) -> bool:
        """Whether the attached file is an image."""
        return bool(self.file_content_type and self.file_content_type.startswith("image/"))

    @property
    def normalized_text(self) -> str:
        """Normalized text with whitespace collapsed."""
        return re.sub(r"\s+", " ", self.text or "").strip().lower()

    def compute_fingerprint(self, file_sha256: str | None = None) -> str:
        """Generate fingerprint for anti-spam detection."""
        norm = self.normalized_text
        if file_sha256:
            return f"text={norm};file={file_sha256}"
        return f"text={norm}"

    def to_dict(self) -> dict[str, Any]:
        """Convert message entity to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "username": self.username,
            "text": self.text,
            "file_key": self.file_key,
            "thumbnail_key": self.thumbnail_key,
            "file_name": self.file_name,
            "file_content_type": self.file_content_type,
            "file_size": self.file_size,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self) -> str:
        return f"<Message id={self.id} user_id={self.user_id} text={self.text[:20]!r}>"
