"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    """Репозиторий для User."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(user: User) -> None
    # Используйте INSERT ... ON CONFLICT DO UPDATE
    async def save(self, user: User) -> None:
        raise NotImplementedError("TODO: Реализовать UserRepository.save")

    # TODO: Реализовать find_by_id(user_id: UUID) -> Optional[User]
    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        raise NotImplementedError("TODO: Реализовать UserRepository.find_by_id")

    # TODO: Реализовать find_by_email(email: str) -> Optional[User]
    async def find_by_email(self, email: str) -> Optional[User]:
        raise NotImplementedError("TODO: Реализовать UserRepository.find_by_email")

    # TODO: Реализовать find_all() -> List[User]
    async def find_all(self) -> List[User]:
        raise NotImplementedError("TODO: Реализовать UserRepository.find_all")


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(order: Order) -> None
    # Сохранить заказ, товары и историю статусов
    async def save(self, order: Order) -> None:
        raise NotImplementedError("TODO: Реализовать OrderRepository.save")

    # TODO: Реализовать find_by_id(order_id: UUID) -> Optional[Order]
    # Загрузить заказ со всеми товарами и историей
    # Используйте object.__new__(Order) чтобы избежать __post_init__
    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        raise NotImplementedError("TODO: Реализовать OrderRepository.find_by_id")

    # TODO: Реализовать find_by_user(user_id: UUID) -> List[Order]
    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        raise NotImplementedError("TODO: Реализовать OrderRepository.find_by_user")

    # TODO: Реализовать find_all() -> List[Order]
    async def find_all(self) -> List[Order]:
        raise NotImplementedError("TODO: Реализовать OrderRepository.find_all")
