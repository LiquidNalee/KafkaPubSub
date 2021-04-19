from enum import Enum


class Topic(Enum):
    ESTIMA = "estima"
    LEAD = "lead"

    def __str__(self) -> str:
        return self.value


class Key(Enum):
    CREATION = "creation"
    DELETION = "deletion"
    UPDATE = "update"
    READ = "read"

    def __str__(self) -> str:
        return self.value
