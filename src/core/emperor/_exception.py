class SignException(Exception):
    """
    关于信号和信号中心的基础类
    Base class for exceptions related to Sign and SignHub.
    """


class SignNodeException(SignException):
    """
    关于信号节点的基础类
    Base class for exceptions related to Sign.
    """


class SignHubException(SignException):
    """
    关于信号中心的基础类
    Base class for exceptions related to SignHub.
    """


class SignStatusError(SignNodeException):
    """
    当信号不是一个正确的状态时抛出的错误
    Raised when a Sign operation is attempted with an invalid status.
    """


class SignNotRunningError(SignNodeException):
    """
    当信号应该运行但未运行时抛出的错误
    Raised when a Sign should be running but isn't.
    """


class SignAlreadyRunningError(SignNodeException):
    """
    当信号运行时但是不应该是运行状态抛出的错误
    Raised when a Sign is found to be running when it should not be.
    """


class SignTaskNotSetError(SignNodeException):
    """
    当尝试使用未设置的Sign的_task属性时抛出的错误
    Raised when attempting to use a Sign's task attribute that has not been set.
    """


class SignHubNotRunningError(SignHubException):
    """
    当信号中心应该运行但未运行时抛出的错误
    Raised when SignHub should be running but isn't.
    """


class SignHubAlreadyRunningError(SignHubException):
    """
    Raised when SignHub is found to be running when it should not be.
    """


class SignNotFoundError(SignHubException):
    """
    当一个信号没有在信号中心找到时抛出的异常
    Raised when a Sign is not found in the SignHub by its ID.
    """
