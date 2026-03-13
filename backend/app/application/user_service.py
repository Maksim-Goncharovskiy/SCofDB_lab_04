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
        raise NotImplementedError("TODO: Реализовать UserService.register")

    # TODO: Реализовать get_by_id(user_id) -> User
    async def get_by_id(self, user_id: uuid.UUID) -> User:
        raise NotImplementedError("TODO: Реализовать UserService.get_by_id")

    # TODO: Реализовать get_by_email(email) -> Optional[User]
    async def get_by_email(self, email: str) -> Optional[User]:
        raise NotImplementedError("TODO: Реализовать UserService.get_by_email")

    # TODO: Реализовать list_users() -> List[User]
    async def list_users(self) -> List[User]:
        raise NotImplementedError("TODO: Реализовать UserService.list_users")
