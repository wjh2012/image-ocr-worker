import uuid

from sqlalchemy import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid_extensions import uuid7


class Base(DeclarativeBase):
    pass


class ImageValidationResult(Base):
    __tablename__ = "image_ocr_result"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid7
    )
    gid: Mapped[uuid.UUID] = mapped_column(default=None)

    def __repr__(self) -> str:
        pass