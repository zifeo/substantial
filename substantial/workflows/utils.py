from datetime import datetime, timezone
import random
import uuid


class Utils:
    @staticmethod
    def now() -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    @staticmethod
    def random(a: int, b: int) -> int:
        return random.randint(a, b)

    @staticmethod
    def uuid4() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def log(message: str) -> None:
        timestamp = Utils.now()
        print(f"[{timestamp}] {message}")
