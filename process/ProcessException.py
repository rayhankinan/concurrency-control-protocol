from __future__ import annotations
from Data import Data


class ProcessException(Exception):
    def __init__(self, data: Data, message: str) -> None:
        self.data = data
        self.message = message
        super().__init__(self.message)
