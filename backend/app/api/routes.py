"""API routes for the marketplace."""

import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db import get_db
from app.infrastructure.repositories import UserRepository, OrderRepository
from app.application.user_service import UserService
from app.application.order_service import OrderService
from app.domain.exceptions import (
    DomainException,
    InvalidEmailError,
    EmailAlreadyExistsError,
    UserNotFoundError,
    OrderNotFoundError,
    OrderAlreadyPaidError,
    OrderCancelledError,
    InvalidQuantityError,
    InvalidPriceError,
)

from .schemas import (
    CreateUser,
    UserResponse,
    CreateOrder,
    AddOrderItem,
    OrderResponse,
    OrderDetailResponse,
    OrderItemResponse,
    OrderStatusChangeResponse,
)

router = APIRouter()


def get_user_service(db: AsyncSession = Depends(get_db)) -> UserService:
    """Dependency to get UserService."""
    repo = UserRepository(db)
    return UserService(repo)


def get_order_service(db: AsyncSession = Depends(get_db)) -> OrderService:
    """Dependency to get OrderService."""
    user_repo = UserRepository(db)
    order_repo = OrderRepository(db)
    return OrderService(order_repo, user_repo)


# User endpoints
@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(data: CreateUser, service: UserService = Depends(get_user_service)):
    """Register a new user."""
    try:
        user = await service.register(data.email, data.name)
        return UserResponse(
            id=user.id,
            email=user.email,
            name=user.name,
            created_at=user.created_at,
        )
    except InvalidEmailError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except EmailAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.get("/users", response_model=List[UserResponse])
async def list_users(service: UserService = Depends(get_user_service)):
    """List all users."""
    users = await service.list_users()
    return [
        UserResponse(
            id=u.id,
            email=u.email,
            name=u.name,
            created_at=u.created_at,
        )
        for u in users
    ]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: uuid.UUID, service: UserService = Depends(get_user_service)):
    """Get user by ID."""
    try:
        user = await service.get_by_id(user_id)
        return UserResponse(
            id=user.id,
            email=user.email,
            name=user.name,
            created_at=user.created_at,
        )
    except UserNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


# Order endpoints
@router.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(data: CreateOrder, service: OrderService = Depends(get_order_service)):
    """Create a new order."""
    try:
        order = await service.create_order(data.user_id)
        return _order_to_response(order)
    except UserNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.get("/orders", response_model=List[OrderResponse])
async def list_orders(
    user_id: uuid.UUID = None,
    service: OrderService = Depends(get_order_service),
):
    """List orders, optionally filtered by user."""
    orders = await service.list_orders(user_id)
    return [_order_to_response(o) for o in orders]


@router.get("/orders/{order_id}", response_model=OrderDetailResponse)
async def get_order(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Get order by ID with full details."""
    try:
        order = await service.get_order(order_id)
        return _order_to_detail_response(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post("/orders/{order_id}/items", response_model=OrderItemResponse, status_code=status.HTTP_201_CREATED)
async def add_order_item(
    order_id: uuid.UUID,
    data: AddOrderItem,
    service: OrderService = Depends(get_order_service),
):
    """Add item to order."""
    try:
        item = await service.add_item(
            order_id,
            data.product_name,
            data.price,
            data.quantity,
        )
        return OrderItemResponse(
            id=item.id,
            product_name=item.product_name,
            price=item.price,
            quantity=item.quantity,
            subtotal=item.subtotal,
        )
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OrderCancelledError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except (InvalidQuantityError, InvalidPriceError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/orders/{order_id}/pay", response_model=OrderResponse)
async def pay_order(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Pay for an order."""
    try:
        order = await service.pay_order(order_id)
        return _order_to_response(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OrderAlreadyPaidError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except OrderCancelledError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/orders/{order_id}/cancel", response_model=OrderResponse)
async def cancel_order(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Cancel an order."""
    try:
        order = await service.cancel_order(order_id)
        return _order_to_response(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OrderAlreadyPaidError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.post("/orders/{order_id}/ship", response_model=OrderResponse)
async def ship_order(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Ship an order."""
    try:
        order = await service.ship_order(order_id)
        return _order_to_response(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/orders/{order_id}/complete", response_model=OrderResponse)
async def complete_order(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Complete an order."""
    try:
        order = await service.complete_order(order_id)
        return _order_to_response(order)
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/orders/{order_id}/history", response_model=List[OrderStatusChangeResponse])
async def get_order_history(order_id: uuid.UUID, service: OrderService = Depends(get_order_service)):
    """Get order status history."""
    try:
        history = await service.get_order_history(order_id)
        return [
            OrderStatusChangeResponse(
                id=h.id,
                status=h.status.value,
                changed_at=h.changed_at,
            )
            for h in history
        ]
    except OrderNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


# Helper functions
def _order_to_response(order) -> OrderResponse:
    """Convert Order domain object to response."""
    return OrderResponse(
        id=order.id,
        user_id=order.user_id,
        status=order.status.value,
        total_amount=order.total_amount,
        created_at=order.created_at,
        items=[
            OrderItemResponse(
                id=item.id,
                product_name=item.product_name,
                price=item.price,
                quantity=item.quantity,
                subtotal=item.subtotal,
            )
            for item in order.items
        ],
    )


def _order_to_detail_response(order) -> OrderDetailResponse:
    """Convert Order domain object to detailed response."""
    return OrderDetailResponse(
        id=order.id,
        user_id=order.user_id,
        status=order.status.value,
        total_amount=order.total_amount,
        created_at=order.created_at,
        items=[
            OrderItemResponse(
                id=item.id,
                product_name=item.product_name,
                price=item.price,
                quantity=item.quantity,
                subtotal=item.subtotal,
            )
            for item in order.items
        ],
        status_history=[
            OrderStatusChangeResponse(
                id=h.id,
                status=h.status.value,
                changed_at=h.changed_at,
            )
            for h in order.status_history
        ],
    )
