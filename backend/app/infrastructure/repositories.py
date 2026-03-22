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
        sql_query = text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                email = EXCLUDED.email,
                name = EXCLUDED.name
        """)

        await self.session.execute(
            sql_query,
            {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "created_at": user.created_at,
            }
        )
        

    # TODO: Реализовать find_by_id(user_id: UUID) -> Optional[User]
    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        sql_query = text("""
            SELECT id, email, name, created_at FROM users WHERE id = :user_id
        """)
        result = await self.session.execute(
            sql_query,
            {"user_id": user_id}
        )

        row = result.first()

        if not row:
            return None 
        
        return User(name=row[2], email=row[1], created_at=row[3], id=row[0])


    # TODO: Реализовать find_by_email(email: str) -> Optional[User]
    async def find_by_email(self, email: str) -> Optional[User]:
        sql_query = text("""
            SELECT id, email, name, created_at FROM users WHERE email = :email
        """)
        result = await self.session.execute(
            sql_query,
            {"email": email}
        )

        row = result.first()

        if not row:
            return None 
        
        return User(name=row[2], email=row[1], created_at=row[3], id=row[0])


    # TODO: Реализовать find_all() -> List[User]
    async def find_all(self) -> List[User]:
        users: list[User] = []

        sql_query = text("""
            SELECT id, email, name, created_at FROM users
        """)

        result = await self.session.execute(sql_query)

        for row in result:
            users.append(User(name=row[2], email=row[1], created_at=row[3], id=row[0]))

        return users 


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(order: Order) -> None
    # Сохранить заказ, товары и историю статусов
    async def save(self, order: Order) -> None:
        insert_order_query = text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, :status, :total_amount, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount
        """)

        await self.session.execute(
            insert_order_query,
            {
                "id": order.id,
                "user_id": order.user_id,
                "status": order.status,
                "total_amount": order.total_amount,
                "created_at": order.created_at
            }
        )

        if order.items:
            # если вдруг какие-то элементы заказа были удалены пользователем
            delete_order_items_query = text("""DELETE FROM order_items WHERE order_id = :order_id""")
            
            await self.session.execute(delete_order_items_query, {"order_id": order.id})
            
            for item in order.items:
                insert_item_query = text("""
                    INSERT INTO order_items (id, order_id, product_name, price, quantity)
                    VALUES (:id, :order_id, :product_name, :price, :quantity)
                """)
                await self.session.execute(
                    insert_item_query,
                    {
                        "id": item.id,
                        "order_id": order.id,
                        "product_name": item.product_name,
                        "price": item.price,
                        "quantity": item.quantity,
                    }
                )
                          
        if order.status_history:
            delete_history_query = text("""DELETE FROM order_status_history WHERE order_id = :order_id""")
            
            await self.session.execute(delete_history_query, {"order_id": order.id})
                
            for history in order.status_history:
                insert_history_query = text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (:id, :order_id, :status, :changed_at)
                """)

                await self.session.execute(
                    insert_history_query,
                    {
                        "id": history.id,
                        "order_id": order.id,
                        "status": history.status.value,
                        "changed_at": history.changed_at,
                    }
                )

    # TODO: Реализовать find_by_id(order_id: UUID) -> Optional[Order]
    # Загрузить заказ со всеми товарами и историей
    # Используйте object.__new__(Order) чтобы избежать __post_init__
    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        get_order_query = text("""
            SELECT id, user_id, created_at, status, total_amount 
            FROM orders 
            WHERE id = :order_id
        """)
        
        order_result = await self.session.execute(get_order_query, {"order_id": order_id})
        order_row = order_result.first()
        
        if not order_row:
            return None
        
        order = object.__new__(Order)
        order.id = order_row[0]
        order.user_id = order_row[1]
        order.created_at = order_row[2]
        order.status = OrderStatus(order_row[3])
        order.total_amount = Decimal(str(order_row[4]))
        order.items = []
        order.status_history = []
        
        get_items_query = text("""
            SELECT id, product_name, price, quantity
            FROM order_items
            WHERE order_id = :order_id
        """)
        
        items_result = await self.session.execute(get_items_query, {"order_id": order_id})
        
        for row in items_result:
            item = object.__new__(OrderItem)
            item.id = row[0]
            item.order_id = order_id
            item.product_name = row[1]
            item.price = Decimal(str(row[2]))
            item.quantity = row[3]
            order.items.append(item)

        get_history_query = text("""
            SELECT id, status, changed_at FROM order_status_history
            WHERE order_id = :order_id
        """)
        
        history_result = await self.session.execute(get_history_query, {"order_id": order_id})
        
        for row in history_result:
            history = object.__new__(OrderStatusChange)
            history.id = row[0]
            history.order_id = order_id
            history.status = OrderStatus(row[1])
            history.changed_at = row[2]
            order.status_history.append(history)
        
        return order

    # TODO: Реализовать find_by_user(user_id: UUID) -> List[Order]
    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        sql_query = text("""SELECT id FROM orders WHERE user_id = :user_id""")
        
        result = await self.session.execute(sql_query, {"user_id": user_id})
        orders = []
        
        for row in result:
            order = await self.find_by_id(row[0])
            if order:
                orders.append(order)
        
        return orders

    # TODO: Реализовать find_all() -> List[Order]
    async def find_all(self) -> List[Order]:
        sql_query = text("""SELECT id FROM orders""")
        
        result = await self.session.execute(sql_query)
        orders = []
        
        for row in result:
            order = await self.find_by_id(row[0])
            if order:
                orders.append(order)
        
        return orders