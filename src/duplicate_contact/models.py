from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import DateTime, func, Index
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
    merge_is_active: Mapped[bool] = mapped_column(
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
    block_id: Mapped[int] = mapped_column(nullable=False)
    settings_id: Mapped[int] = mapped_column(
        sa.ForeignKey("settings.id", ondelete="CASCADE"), nullable=False
    )

    settings: Mapped["Settings"] = relationship("Settings", back_populates="blocks")

    # Отношение к полям блока; через поля будут доступны исключения
    fields: Mapped[list["BlockField"]] = relationship(
        "BlockField", back_populates="block", cascade="all, delete-orphan"
    )

    merge_logs: Mapped[list["MergeBlockLog"]] = relationship(
        "MergeBlockLog", back_populates="block", cascade="all, delete-orphan"
    )

    __table_args__ = (
        sa.UniqueConstraint(
            "block_id", "settings_id", name="uq_block_blockid_settings"
        ),
    )


class BlockField(Base):
    """Поля в блоках настроек дублей с их исключениями."""

    __tablename__ = "block_fields"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    field_name: Mapped[str] = mapped_column(sa.String(128), nullable=False)
    block_id: Mapped[int] = mapped_column(
        sa.ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False
    )
    block: Mapped["Block"] = relationship("Block", back_populates="fields")

    # Связь с таблицей исключений для конкретного поля
    exclusion_values: Mapped[list["ExclusionField"]] = relationship(
        "ExclusionField", back_populates="block_field", cascade="all, delete-orphan"
    )


class ExclusionField(Base):
    """Исключённые поля в дублях контактов."""

    __tablename__ = "exclusion_fields"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    value: Mapped[str] = mapped_column(sa.String(128), nullable=False)
    field_name: Mapped[str] = mapped_column(sa.String(128), nullable=False)

    block_field_id: Mapped[int] = mapped_column(
        sa.ForeignKey("block_fields.id", ondelete="CASCADE"), nullable=False
    )
    block_field: Mapped["BlockField"] = relationship(
        "BlockField", back_populates="exclusion_values"
    )


class MergeBlockLog(Base):
    """Лог объединения контактов."""

    __tablename__ = "merge_block_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    subdomain: Mapped[str] = mapped_column(sa.String(256), nullable=False)
    block_id: Mapped[int] = mapped_column(
        sa.ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False
    )
    contact_id: Mapped[int] = mapped_column(
        sa.Integer, nullable=False
    )  # Итоговый контакт после склейки
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    block: Mapped["Block"] = relationship("Block", back_populates="merge_logs")

    __table_args__ = (
        Index("ix_merge_block_logs_subdomain_contact_id", "subdomain", "contact_id"),
    )
