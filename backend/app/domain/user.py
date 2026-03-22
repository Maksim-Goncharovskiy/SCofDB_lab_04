"""Доменная сущность пользователя."""

import re 
import uuid
from datetime import datetime
from dataclasses import dataclass, field 

from .exceptions import InvalidEmailError


# TODO: Реализовать класс User
# - Использовать @dataclass
# - Поля: email, name, id, created_at
# - Реализовать валидацию email в __post_init__
# - Regex: r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

@dataclass
class User:
    email: str
    name: str = field(default="")
    created_at: datetime = field(default_factory=datetime.now)
    id: uuid.UUID = field(default_factory=uuid.uuid4) 
    
    def __post_init__(self):
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not bool(re.match(email_pattern, self.email)):
            raise InvalidEmailError(email=self.email)