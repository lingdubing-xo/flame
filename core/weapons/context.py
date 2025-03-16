from typing import List
from .context_model import StringContextModel

class SimpleContext:
    def __init__(self, max_tokens: int = 4000):
        self.model = StringContextModel()
        self.history: List[str] = []
        self.max_tokens = max_tokens

    def append(self, content: str) -> bool:
        token_count = self.model.token_counter(content)
        total_tokens = sum(self.model.token_counter(piece) for piece in self.history) + token_count
        if total_tokens <= self.max_tokens:
            self.history.append(content)
            return True
        return False

    def get_history(self) -> List[str]:
        return self.history
