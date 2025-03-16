import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.common.database import Base


class Settings(Base):
    """Основные настройки дублей контактов."""

    __tablename__ = "settings"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    subdomain: Mapped[str] = mapped_column(sa.String(256), unique=True, nullable=False)
    merge_all: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    blocked_creation: Mapped[bool] = mapped_column(
        sa.Boolean, default=False, nullable=False
    )
    duplicate_start: Mapped[bool] = mapped_column(
        sa.Boolean, default=False, nullable=False
    )

    priority_fields: Mapped[list["PriorityField"]] = relationship(
        "PriorityField", back_populates="settings", cascade="all, delete-orphan"
    )
    blocks: Mapped[list["Block"]] = relationship(
        "Block", back_populates="settings", cascade="all, delete-orphan"
    )


class PriorityField(Base):
    """Приоритетные поля контактов."""

    __tablename__ = "priority_fields"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    field_name: Mapped[str] = mapped_column(sa.String(128), nullable=False)
    action: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)

    settings_id: Mapped[int] = mapped_column(
        sa.ForeignKey("settings.id", ondelete="CASCADE"), nullable=False
    )

    settings: Mapped["Settings"] = relationship(
        "Settings", back_populates="priority_fields"
    )

    __table_args__ = (
        sa.UniqueConstraint("field_name", "settings_id", name="uq_priority_fields"),
    )


class Block(Base):
    """Блоки настроек дублей контактов."""

    __tablename__ = "blocks"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    settings_id: Mapped[int] = mapped_column(
        sa.ForeignKey("settings.id", ondelete="CASCADE"), nullable=False
    )

    settings: Mapped["Settings"] = relationship("Settings", back_populates="blocks")

    fields: Mapped[list["BlockField"]] = relationship(
        "BlockField", back_populates="block", cascade="all, delete-orphan"
    )

    exclusion_fields: Mapped[list["ExclusionField"]] = relationship(
        "ExclusionField", back_populates="block", cascade="all, delete-orphan"
    )


class BlockField(Base):
    """Поля в блоках дублей."""

    __tablename__ = "block_fields"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    field_name: Mapped[str] = mapped_column(sa.String(128), nullable=False)

    block_id: Mapped[int] = mapped_column(
        sa.ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False
    )

    block: Mapped["Block"] = relationship("Block", back_populates="fields")


class ExclusionField(Base):
    """Исключённые поля в дублях контактов."""

    __tablename__ = "exclusion_fields"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    field_name: Mapped[str] = mapped_column(sa.String(128), nullable=False)

    block_id: Mapped[int] = mapped_column(
        sa.ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False
    )

    block: Mapped["Block"] = relationship("Block", back_populates="exclusion_fields")
