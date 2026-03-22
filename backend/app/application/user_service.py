"""Сервис для работы с пользователями."""

import uuid
from typing import Optional, List

from app.domain.user import User
from app.domain.exceptions import EmailAlreadyExistsError, UserNotFoundError


class UserService:
    """Сервис для операций с пользователями."""

    def __init__(self, repo):
        self.repo = repo

    # TODO: Реализовать register(email, name) -> User
    # 1. Проверить что email не занят
    # 2. Создать User
    # 3. Сохранить через repo.save()
    async def register(self, email: str, name: str = "") -> User:
        is_taken = await self.repo.find_by_email(email)
        if is_taken:
            raise EmailAlreadyExistsError(email=email)

        user = User(name=name, email=email)
        
        await self.repo.save(user)
        return user


    # TODO: Реализовать get_by_id(user_id) -> User
    async def get_by_id(self, user_id: uuid.UUID) -> User:
        user = await self.repo.find_by_id(user_id)
        if not user:
            raise UserNotFoundError(user_id=user_id)
        return user


    # TODO: Реализовать get_by_email(email) -> Optional[User]
    async def get_by_email(self, email: str) -> Optional[User]:
        user = await self.repo.find_by_email(email)
        return user


    # TODO: Реализовать list_users() -> List[User]
    async def list_users(self) -> List[User]:
        users = await self.repo.find_all()
        return users