from abc import ABCMeta, abstractmethod
from typing import Literal

class BasicSplitterTextInterface(metaclass=ABCMeta):
    """
    继承basic的类需实现split方法
    """
    @abstractmethod
    def split(self, text: str) -> Literal[str]:
        pass


