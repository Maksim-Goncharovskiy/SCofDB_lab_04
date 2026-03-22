"""Сервис для работы с заказами."""

import uuid
from decimal import Decimal
from typing import List, Optional

from app.domain.order import Order, OrderItem, OrderStatus
from app.domain.exceptions import OrderNotFoundError, UserNotFoundError


class OrderService:
    """Сервис для операций с заказами."""

    def __init__(self, order_repo, user_repo):
        self.order_repo = order_repo
        self.user_repo = user_repo

    # TODO: Реализовать create_order(user_id) -> Order
    async def create_order(self, user_id: uuid.UUID) -> Order:
        order = Order(
            user_id=user_id
        )
        
        await self.order_repo.save(order)

        return order 


    # TODO: Реализовать get_order(order_id) -> Order
    async def get_order(self, order_id: uuid.UUID) -> Order:
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        return order 
    

    # TODO: Реализовать add_item(order_id, product_name, price, quantity) -> OrderItem
    async def add_item(
        self,
        order_id: uuid.UUID,
        product_name: str,
        price: Decimal,
        quantity: int,
    ) -> OrderItem:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        
        order_item = order.add_item(product_name=product_name, price=price, quantity=quantity)

        await self.order_repo.save(order)

        return order_item


    # TODO: Реализовать pay_order(order_id) -> Order
    # КРИТИЧНО: гарантировать что нельзя оплатить дважды!
    async def pay_order(self, order_id: uuid.UUID) -> Order:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        
        order.pay()

        await self.order_repo.save(order)

        return order 


    # TODO: Реализовать cancel_order(order_id) -> Order
    async def cancel_order(self, order_id: uuid.UUID) -> Order:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        
        order.cancel()

        await self.order_repo.save(order)

        return order 

    # TODO: Реализовать ship_order(order_id) -> Order
    async def ship_order(self, order_id: uuid.UUID) -> Order:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        
        order.ship()

        await self.order_repo.save(order)

        return order 


    # TODO: Реализовать complete_order(order_id) -> Order
    async def complete_order(self, order_id: uuid.UUID) -> Order:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)
        
        order.complete()

        await self.order_repo.save(order)

        return order 


    # TODO: Реализовать list_orders(user_id: Optional) -> List[Order]
    async def list_orders(self, user_id: Optional[uuid.UUID] = None) -> List[Order]:
        return await self.order_repo.find_by_user(user_id)
    

    # TODO: Реализовать get_order_history(order_id) -> List[OrderStatusChange]
    async def get_order_history(self, order_id: uuid.UUID) -> List:
        order: Order = await self.order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundError(order_id)

        return order.status_history