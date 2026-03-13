"""
Tests for domain layer invariants.

These tests verify that students correctly implemented domain invariants.
All tests must pass for the lab to be accepted.

DO NOT MODIFY THIS FILE!
"""

import pytest
import uuid
from decimal import Decimal

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus
from app.domain.exceptions import (
    InvalidEmailError,
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
    InvalidAmountError,
)


class TestUserInvariants:
    """Tests for User domain invariants."""

    def test_create_user_with_valid_email(self):
        """User can be created with valid email."""
        user = User(email="test@example.com")
        assert user.email == "test@example.com"
        assert user.id is not None

    def test_create_user_with_name(self):
        """User can be created with name."""
        user = User(email="test@example.com", name="John Doe")
        assert user.name == "John Doe"

    def test_create_user_with_empty_email_fails(self):
        """INVARIANT: User cannot be created with empty email."""
        with pytest.raises(InvalidEmailError):
            User(email="")

    def test_create_user_with_whitespace_email_fails(self):
        """INVARIANT: User cannot be created with whitespace-only email."""
        with pytest.raises(InvalidEmailError):
            User(email="   ")

    def test_create_user_with_invalid_email_no_at(self):
        """INVARIANT: Email must contain @."""
        with pytest.raises(InvalidEmailError):
            User(email="invalid")

    def test_create_user_with_invalid_email_no_domain(self):
        """INVARIANT: Email must have domain."""
        with pytest.raises(InvalidEmailError):
            User(email="invalid@")

    def test_create_user_with_invalid_email_no_local(self):
        """INVARIANT: Email must have local part."""
        with pytest.raises(InvalidEmailError):
            User(email="@example.com")


class TestOrderItemInvariants:
    """Tests for OrderItem domain invariants."""

    def test_create_order_item_with_valid_data(self):
        """OrderItem can be created with valid data."""
        item = OrderItem(
            product_name="Test Product",
            price=Decimal("99.99"),
            quantity=2,
        )
        assert item.product_name == "Test Product"
        assert item.price == Decimal("99.99")
        assert item.quantity == 2

    def test_order_item_subtotal_calculation(self):
        """OrderItem calculates subtotal correctly."""
        item = OrderItem(
            product_name="Test Product",
            price=Decimal("10.00"),
            quantity=3,
        )
        assert item.subtotal == Decimal("30.00")

    def test_order_item_with_zero_quantity_fails(self):
        """INVARIANT: Quantity must be > 0."""
        with pytest.raises(InvalidQuantityError):
            OrderItem(
                product_name="Test",
                price=Decimal("10.00"),
                quantity=0,
            )

    def test_order_item_with_negative_quantity_fails(self):
        """INVARIANT: Quantity must be > 0."""
        with pytest.raises(InvalidQuantityError):
            OrderItem(
                product_name="Test",
                price=Decimal("10.00"),
                quantity=-1,
            )

    def test_order_item_with_negative_price_fails(self):
        """INVARIANT: Price must be >= 0."""
        with pytest.raises(InvalidPriceError):
            OrderItem(
                product_name="Test",
                price=Decimal("-10.00"),
                quantity=1,
            )

    def test_order_item_with_zero_price_succeeds(self):
        """Zero price is allowed (free items)."""
        item = OrderItem(
            product_name="Free Item",
            price=Decimal("0.00"),
            quantity=1,
        )
        assert item.price == Decimal("0.00")


class TestOrderInvariants:
    """Tests for Order domain invariants."""

    def test_create_order(self):
        """Order can be created."""
        user_id = uuid.uuid4()
        order = Order(user_id=user_id)
        
        assert order.user_id == user_id
        assert order.status == OrderStatus.CREATED
        assert order.total_amount == Decimal("0")

    def test_add_item_to_order(self):
        """Items can be added to order."""
        order = Order(user_id=uuid.uuid4())
        item = order.add_item("Product", Decimal("100.00"), 2)
        
        assert len(order.items) == 1
        assert order.total_amount == Decimal("200.00")

    def test_order_total_recalculates_on_add_item(self):
        """Total amount recalculates when items are added."""
        order = Order(user_id=uuid.uuid4())
        order.add_item("Product 1", Decimal("100.00"), 1)
        order.add_item("Product 2", Decimal("50.00"), 2)
        
        assert order.total_amount == Decimal("200.00")

    def test_pay_order(self):
        """Order can be paid."""
        order = Order(user_id=uuid.uuid4())
        order.add_item("Product", Decimal("100.00"), 1)
        order.pay()
        
        assert order.status == OrderStatus.PAID

    def test_cannot_pay_order_twice(self):
        """CRITICAL INVARIANT: Order cannot be paid twice!"""
        order = Order(user_id=uuid.uuid4())
        order.add_item("Product", Decimal("100.00"), 1)
        order.pay()
        
        with pytest.raises(OrderAlreadyPaidError):
            order.pay()

    def test_cannot_pay_cancelled_order(self):
        """INVARIANT: Cancelled order cannot be paid."""
        order = Order(user_id=uuid.uuid4())
        order.cancel()
        
        with pytest.raises(OrderCancelledError):
            order.pay()

    def test_cancel_order(self):
        """Order can be cancelled."""
        order = Order(user_id=uuid.uuid4())
        order.cancel()
        
        assert order.status == OrderStatus.CANCELLED

    def test_cannot_cancel_paid_order(self):
        """INVARIANT: Paid order cannot be cancelled."""
        order = Order(user_id=uuid.uuid4())
        order.pay()
        
        with pytest.raises(OrderAlreadyPaidError):
            order.cancel()

    def test_cannot_add_items_to_cancelled_order(self):
        """INVARIANT: Items cannot be added to cancelled order."""
        order = Order(user_id=uuid.uuid4())
        order.cancel()
        
        with pytest.raises(OrderCancelledError):
            order.add_item("Product", Decimal("100.00"), 1)

    def test_ship_order_requires_paid_status(self):
        """INVARIANT: Order must be paid before shipping."""
        order = Order(user_id=uuid.uuid4())
        
        with pytest.raises(ValueError):
            order.ship()

    def test_complete_order_requires_shipped_status(self):
        """INVARIANT: Order must be shipped before completing."""
        order = Order(user_id=uuid.uuid4())
        order.pay()
        
        with pytest.raises(ValueError):
            order.complete()


class TestCriticalPaymentInvariant:
    """
    Special test class for the CRITICAL invariant: cannot pay twice.
    
    This is the main requirement of the lab!
    """

    def test_cannot_pay_twice_basic(self):
        """Basic test: second pay() call must raise OrderAlreadyPaidError."""
        order = Order(user_id=uuid.uuid4())
        order.pay()
        
        with pytest.raises(OrderAlreadyPaidError):
            order.pay()

    def test_cannot_pay_twice_with_items(self):
        """Test with items: second pay() call must raise OrderAlreadyPaidError."""
        order = Order(user_id=uuid.uuid4())
        order.add_item("Product", Decimal("100.00"), 1)
        order.pay()
        
        with pytest.raises(OrderAlreadyPaidError):
            order.pay()

    def test_order_remains_paid_after_failed_double_payment(self):
        """Order status must remain PAID after failed double payment."""
        order = Order(user_id=uuid.uuid4())
        order.pay()
        
        try:
            order.pay()
        except OrderAlreadyPaidError:
            pass
        
        assert order.status == OrderStatus.PAID

    def test_exception_contains_order_id(self):
        """OrderAlreadyPaidError should contain order ID."""
        order = Order(user_id=uuid.uuid4())
        order.pay()
        
        with pytest.raises(OrderAlreadyPaidError) as exc_info:
            order.pay()
        
        assert str(order.id) in str(exc_info.value)
