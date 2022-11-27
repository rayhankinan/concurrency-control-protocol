class Data:
    def __init__(self) -> None:
        self.value: int = 0

    def getValue(self) -> int:
        return self.value

    def setValue(self, value: int) -> None:
        self.value = value
