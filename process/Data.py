from __future__ import annotations
import math


class Data:
    def __init__(self, filename: str) -> None:
        self.filename: str = filename

    def read(self) -> int:
        file = open(self.filename, "rb")
        content = file.read()
        file.close()

        return int.from_bytes(content, "big")

    def write(self, value: int) -> None:
        file = open(self.filename, "wb")

        length = math.ceil(value / (1 << 8))
        content = value.to_bytes(length, "big")
        file.write(content)

        file.close()
