"""Domain exceptions for business rule violations."""


class DomainException(Exception):
    """Base exception for domain errors."""

    pass


class InvalidEmailError(DomainException):
    """Raised when email format is invalid."""

    def __init__(self, email: str):
        self.email = email
        super().__init__(f"Invalid email format: {email}")


class OrderAlreadyPaidError(DomainException):
    """Raised when attempting to pay an already paid order."""

    def __init__(self, order_id):
        self.order_id = order_id
        super().__init__(f"Order {order_id} is already paid")


class OrderCancelledError(DomainException):
    """Raised when attempting to modify a cancelled order."""

    def __init__(self, order_id):
        self.order_id = order_id
        super().__init__(f"Order {order_id} is cancelled")


class InvalidQuantityError(DomainException):
    """Raised when quantity is not positive."""

    def __init__(self, quantity: int):
        self.quantity = quantity
        super().__init__(f"Quantity must be positive, got: {quantity}")


class InvalidPriceError(DomainException):
    """Raised when price is negative."""

    def __init__(self, price):
        self.price = price
        super().__init__(f"Price cannot be negative, got: {price}")


class InvalidAmountError(DomainException):
    """Raised when amount is negative."""

    def __init__(self, amount):
        self.amount = amount
        super().__init__(f"Amount cannot be negative, got: {amount}")


class UserNotFoundError(DomainException):
    """Raised when user is not found."""

    def __init__(self, user_id):
        self.user_id = user_id
        super().__init__(f"User {user_id} not found")


class OrderNotFoundError(DomainException):
    """Raised when order is not found."""

    def __init__(self, order_id):
        self.order_id = order_id
        super().__init__(f"Order {order_id} not found")


class EmailAlreadyExistsError(DomainException):
    """Raised when email is already registered."""

    def __init__(self, email: str):
        self.email = email
        super().__init__(f"Email already exists: {email}")
