from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar, List
import tiktoken

ContextPiece = TypeVar("ContextPiece")


class ContextModelBase(Generic[ContextPiece], metaclass=ABCMeta):
    """
    具有令牌计数和验证的上下文模型的基类
    Base class for context models with token counting and validation.
    """

    def __init__(self, encoding_name: str = "cl100k_base"):
        """
        使用分词器编码进行初始化
        Initialize with a tokenizer encoding.
        """
        self.tokenizer = tiktoken.get_encoding(encoding_name)

    @property
    @abstractmethod
    def context_model_name(self) -> str:
        """
        返回上下文模型的名称
        Return the name of the context model.
        """
        ...

    @abstractmethod
    def token_counter(self, context_piece: ContextPiece) -> int:
        """
        计算单个上下文片段中的标记数量
        Calculate the number of tokens in a single context piece.
        """
        ...

    def token_counter_modifier(self, context_piece_list: List[ContextPiece], total_token_num: int) -> int:
        """
        调整总令牌数量。默认返回总和不变。
        Adjust total token count if needed. Default returns the sum unchanged.
        """
        return total_token_num

    def piece_type_validator(self, context_piece: ContextPiece) -> bool:
        """
        验证上下文片段的类型。默认接受所有类型。
        Validate the type of a context piece. Default accepts all types.
        """
        return True

    @property
    def is_counter_modified(self) -> bool:
        """
        检查是否覆盖了token_counter_modifier。
        Check if token_counter_modifier is overridden.
        """
        return self.__class__.token_counter_modifier != ContextModelBase.token_counter_modifier

    def count_tokens(self, context_pieces: List[ContextPiece]) -> int:
        """
        计算上下文片段列表中的总令牌数，并进行验证。
        Count total tokens for a list of context pieces with validation.
        """
        if not context_pieces:
            return 0

        total = 0
        for piece in context_pieces:
            if not self.piece_type_validator(piece):
                raise ValueError(f"Invalid context piece type: {type(piece)}")
            total += self.token_counter(piece)

        return self.token_counter_modifier(context_pieces, total)


class StringContextModel(ContextModelBase[str]):
    """
    字符串上下文模型的抽象实现
    Concrete implementation for string-based context pieces.
    """

    @property
    def context_model_name(self) -> str:
        return "string_context_model"

    def token_counter(self, context_piece: str) -> int:
        """
        计算字符串上下文片段中的令牌数量
        Count tokens using the tokenizer.
        """
        return len(self.tokenizer.encode(context_piece))

    def piece_type_validator(self, context_piece: str) -> bool:
        """
        验证上下文片段是否为字符串
        Ensure context piece is a string.
        """
        return isinstance(context_piece, str)
