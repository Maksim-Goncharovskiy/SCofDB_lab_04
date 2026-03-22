"""Доменные сущности заказа."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List

from .exceptions import (
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError
)


# TODO: Реализовать OrderStatus (str, Enum)
# Значения: CREATED, PAID, CANCELLED, SHIPPED, COMPLETED
class OrderStatus(str, Enum):
    CREATED = "created"
    PAID = "paid"
    CANCELLED = "cancelled"
    SHIPPED = "shipped"
    COMPLETED = "completed"



# TODO: Реализовать OrderItem (dataclass)
# Поля: product_name, price, quantity, id, order_id
# Свойство: subtotal (price * quantity)
# Валидация: quantity > 0, price >= 0
@dataclass
class OrderItem:
    product_name: str
    price: Decimal
    quantity: int
    order_id: uuid.UUID | None = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    @property
    def subtotal(self) -> Decimal:
        return self.price * Decimal(self.quantity)
    

    def __post_init__(self):
        if self.quantity <= 0:
            raise InvalidQuantityError(quantity=self.quantity)
        if self.price < 0:
            raise InvalidPriceError(price=self.price)



# TODO: Реализовать OrderStatusChange (dataclass)
# Поля: order_id, status, changed_at, id
@dataclass 
class OrderStatusChange:
    order_id: uuid.UUID
    status: OrderStatus
    changed_at: datetime = field(default_factory=datetime.now)
    id: uuid.UUID = field(default_factory=uuid.uuid4)



# TODO: Реализовать Order (dataclass)
# Поля: user_id, id, status, total_amount, created_at, items, status_history
# Методы:
#   - add_item(product_name, price, quantity) -> OrderItem
#   - pay() -> None  [КРИТИЧНО: нельзя оплатить дважды!]
#   - cancel() -> None
#   - ship() -> None
#   - complete() -> None
@dataclass
class Order:
    user_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: OrderStatus = field(default=OrderStatus.CREATED)
    total_amount: Decimal = field(default=Decimal("0"))
    created_at: datetime = field(default_factory=datetime.now)
    items: List[OrderItem] = field(default_factory=list)
    status_history: List[OrderStatusChange] = field(default_factory=list)

    def __post_init__(self):
        self._update_status_history(self.status)


    def _update_status_history(self, new_status: OrderStatus):
        status_change = OrderStatusChange(order_id=self.id, status=new_status)
        self.status_history.append(status_change)


    def add_item(self, product_name: str, price: Decimal, quantity: int) -> OrderItem:
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        
        item = OrderItem(product_name=product_name, price=price, quantity=quantity, order_id=self.id)

        self.items.append(item)
        self.total_amount += item.subtotal

        return item


    def pay(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        
        if self.status == OrderStatus.CANCELLED:
            raise OrderCancelledError(self.id)
        
        self.status = OrderStatus.PAID
        self._update_status_history(self.status)


    def cancel(self) -> None:
        if self.status == OrderStatus.PAID:
            raise OrderAlreadyPaidError(self.id)
        
        if self.status == OrderStatus.SHIPPED:
            raise ValueError("Shipped order cannote be canceled")
        
        if self.status == OrderStatus.COMPLETED:
            raise ValueError("Completed order cannot be canceled")
        
        self.status = OrderStatus.CANCELLED
        self._update_status_history(self.status)


    def ship(self) -> None:
        if self.status != OrderStatus.PAID:
            raise ValueError("Order cannot be shipped without paying")
        
        self.status = OrderStatus.SHIPPED
        self._update_status_history(self.status)


    def complete(self) -> None:
        if self.status != OrderStatus.SHIPPED:
            raise ValueError("Order must be shipped before it can be completed")
        
        self.status = OrderStatus.COMPLETED
        self._update_status_history(self.status)