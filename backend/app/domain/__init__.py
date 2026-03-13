# Domain layer exports
# Students must implement these classes

from .user import User
from .order import Order, OrderItem, OrderStatus, OrderStatusChange
from .exceptions import (
    DomainException,
    InvalidEmailError,
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError,
    UserNotFoundError,
    OrderNotFoundError,
    EmailAlreadyExistsError,
)

__all__ = [
    "User",
    "Order",
    "OrderItem",
    "OrderStatus",
    "OrderStatusChange",
    "DomainException",
    "InvalidEmailError",
    "OrderAlreadyPaidError",
    "OrderCancelledError",
    "InvalidQuantityError",
    "InvalidPriceError",
    "InvalidAmountError",
    "UserNotFoundError",
    "OrderNotFoundError",
    "EmailAlreadyExistsError",
]
