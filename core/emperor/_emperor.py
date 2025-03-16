from __future__ import annotations
import asyncio
import threading
from typing import Optional, Callable, Coroutine, Any, Dict, List, Union, Literal
from abc import abstractmethod, ABC
from dataclasses import dataclass
from enum import Enum
import uuid
import logging
import inspect
from ._exception import *

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SignHub")

# 回调类型
CallbackType = Literal["on_start", "on_finished", "on_fail", "on_cancel"]

class SignStatus(Enum):
    """
    信号状态的枚举,包括等待,运行,结束,失败,取消五个状态
    status of jianshu, including pending, running, finished, failed, cancelled
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

@dataclass
class Sign:
    """定义一个信号
    Define a sign
    id: uuid.UUID 保证分布式任务id的唯一性
    Ensures the uniqueness of sign IDs in a distributed environment
    coro: Coroutine[Any, Any, Any] | [] 协程对象表明这个为sign实际执行的任务,不在sign初始化直接创建协程对象
    A coroutine object representing the actual execution content of the task
    priority: int = 0 信号优先级,默认为0,值越小,优先级越高
    sign priority, default is 0, the smaller the value, the higher the priority
    timeout: Optional[float] = None 信号超时时间,默认为None,表示不设置超时
    sign timeout, default is None, indicating no timeout is set
    parent: Optional[Task] = None 父节点,如果为空代表这个节点是根节点
    Parent node, if empty, it represents the root node
    children: Optional[List[Task]] = None 子节点,如果为没有子节点代表
    Child node, if there are no child nodes, it represents
    status: SignStatus = SignStatus.PENDING 任务状态,默认为等待中
    sign status, default is pending
    result: Any = None 任务结果, 允许任意类型,默认为None
    sign result, allowing any type, default is None
    exception: Optional[Exception] = None 任务失败时的异常,默认为None
    Exception when the jianshu fails, default is None
    callbacks: Dict[CallbackType, List[Callable]] = None 回调函数,默认为None
    Callback function, default is None
    data: Any = None 作为信号共享的数据,默认为None
    Shared data as a sign, default is None
    """
    _id: uuid.UUID
    _coro: Callable[[], Coroutine[Any, Any, Any]]
    _priority: int = 0
    _timeout: Optional[float] = None
    _parent: Optional[Sign] = None
    _children: Optional[List[Sign]] = None
    _status: SignStatus = SignStatus.PENDING
    _result: Any = None
    _exception: Optional[Exception] = None
    _callbacks: Dict[CallbackType, List[Callable]] = None
    _data: Any = None

    def __post_init__(self):
        self._children = self._children or []
        self._callbacks = self._callbacks or {}
        self._task: Optional[asyncio.Task] = None

    def __lt__(self, other):
        if not isinstance(other, Sign):
            return NotImplemented
        return self._id < other._id

    @property
    def id(self) -> uuid.UUID:
        """
        返回信号的编号
        back of sign id
        """
        return self._id

    @property
    def data(self) -> Any:
        """
        返回信号的数据
        :return:
        """
        return self._data

    @data.setter
    def data(self, value: Any) -> None:
        """
        设置信号的数据
        """
        self._data = value

    @property
    def is_running(self) -> bool:
        """
        判断信号是否正在被阅读
        Determine whether the sign has been read
        """
        return self._status == SignStatus.RUNNING

    @property
    def is_finished(self) -> bool:
        """
        判断信号是否已经阅读完毕或者不会在进行阅读了
        Determine whether the sign has been read or will not be read
        :return:
        """
        return self._status in (SignStatus.FINISHED, SignStatus.FAILED, SignStatus.CANCELLED)

    def add_callback(self, event: CallbackType, callback: Callable[[Sign], None]) -> None:
        """添加生命周期回调"""
        if event not in self._callbacks:
            self._callbacks[event] = []
        self._callbacks[event].append(callback)

    async def run(self, hub: SignHub) -> None:
        """
        执行信号并且更新状态
        Execute the signal and update the status
        :param hub:
        :return:
        """
        if self._status != SignStatus.PENDING:
            raise SignStatusError(f"Sign {self.id} cannot run, current status is {self._status}")
        self._status = SignStatus.RUNNING
        logger.info(f"Sign {self.id} is running")
        await self._execute_callbacks("on_start", hub)
        try:
            coro = self._coro()  # 在运行时生成协程
            if self._timeout:
                self._result = await asyncio.wait_for(coro, timeout=self._timeout)
            else:
                self._result = await coro
            self._status = SignStatus.FINISHED
            logger.info(f"Sign {self.id} finished with result: {self._result}")
            await self._execute_callbacks("on_finished", hub)
            hub.emit("sign_finished", self)
        except asyncio.TimeoutError as e:
            self._status = SignStatus.FAILED
            self._exception = e
            self._result = None
            logger.error(f"Sign {self.id} timed out with exception: {e}")
            await self._execute_callbacks("on_fail", hub)
            hub.emit("sign_failed", self)
        except Exception as e:
            self._status = SignStatus.FAILED
            self._exception = e
            self._result = None
            logger.error(f"Sign {self.id} failed with exception: {e}")
            await self._execute_callbacks("on_fail", hub)
            hub.emit("sign_failed", self)
        except asyncio.CancelledError:
            self._status = SignStatus.CANCELLED
            logger.info(f"Sign {self.id} was cancelled")
            await self._execute_callbacks("on_cancel", hub)
            hub.emit("sign_cancelled", self)
        finally:
            if self._parent:
                hub._remove_child(self._parent, self)

    async def _execute_callbacks(self, event: str, hub: SignHub) -> None:
        if event in self._callbacks:
            for callback in self._callbacks[event]:
                try:
                    sig = inspect.signature(callback)
                    if len(sig.parameters) == 1:
                        await callback(self)
                    else:
                        await callback(self, hub)
                except Exception as e:
                    logger.error(f"Callback {event} for Sign {self._id} failed: {e}")

    async def cancel(self) -> bool:
        """
        取消信号
        Cancel the signal
        """
        if self._status not in (SignStatus.PENDING, SignStatus.RUNNING):
            raise SignStatusError(f"Sign {self.id} cannot be cancelled, status: {self._status}")
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                self._status = SignStatus.CANCELLED
                logger.info(f"Sign {self.id} cancelled")
                await self._execute_callbacks("on_cancel", hub=None)
            return True
        return False

class EventListener(ABC):
    """
    事件监听器接口
    Event listener interface
    """
    @abstractmethod
    async def on_event(self, event: str, sign: Sign) -> None:
        pass

class SignHub:
    def __init__(self):
        """
        SignHub 是一个异步任务管理中心，负责调度和管理 Sign 对象。
        该方法初始化事件循环、任务存储、队列、监听器等核心组件
        SignHub is an asynchronous signal management center
        that is responsible for scheduling and managing Sign objects
        This method initializes core components such as event loops, task stores, queues, and listeners
        """
        # 获取当前线程的事件循环,用于异步任务的调度
        # Gets the event loop of the current thread for scheduling asynchronous tasks
        self._loop = asyncio.get_event_loop()
        # 存储所有sign对象
        # Stores all sign objects
        self._signs: Dict[uuid.UUID, Sign] = {}
        # 优先级队列，按照优先级顺序阅读sign
        # Priority queue, read sign in order of priority
        self._queue = asyncio.PriorityQueue()
        # 存储所有监听器
        # Stores all listeners
        self._listeners: Dict[str, List[EventListener]] = {}
        # 表示SignHub是否正在运行
        # Indicates whether SignHub is running
        self._running = False
        # 异步锁，用于保护共享资源的并发访问
        # Asynchronous lock, used to protect concurrent access to shared resources
        self._lock = asyncio.Lock()
        # 线程锁，用于线程安全的任务提交
        # Thread lock, used for thread-safe task submission
        self._thread_lock = threading.Lock()
        # 添加运行任务以便停止时管理
        self._run_task: Optional[asyncio.Task] = None

    async def start(self, initial_signs: Optional[List[Coroutine[Any, Any, Any]]] = None) -> None:
        """
        启动SignHub，并阅读初始信号
        start SignHub and read initial signs
        :param initial_signs:
        :return:
        """
        if self._running:
            raise SignHubAlreadyRunningError("SignHub is already running")
        self._running = True
        logger.info("SignHub started")
        if initial_signs:
            for coro in initial_signs:
                await self.submit(coro)
        self._run_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """
        停止SignHub并且取消所有阅读中的信号
        stop SignHub and cancel all reading signs
        :return:
        """
        if not self._running:
            raise SignHubNotRunningError("SignHub is not running")
        self._running = False
        # 取消运行循环任务
        if self._run_task and not self._run_task.done():
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                logger.debug("SignHub _run task cancelled")
        # 取消所有运行中的任务
        tasks = [sign._task for sign in self._signs.values() if sign._task and not sign._task.done()]
        for sign in self._signs.values():
            if sign.is_running:
                await sign.cancel()
        # 等待任务完成
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # 等待队列清空
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break
        logger.info("SignHub stopped")

    async def submit(self, coro: Union[Callable[[], Coroutine[Any, Any, Any]], Sign], parent: Optional[Sign] = None,
                     priority: int = 0, timeout: Optional[float] = None) -> Sign:
        """
        提交一个异步任务到SignHub，并返回一个Sign对象
        异步方法，不保证线程安全，只保证异步安全
        Submit an asynchronous task to SignHub and return a Sign object
        Asynchronous method, not thread-safe, only asynchronous-safe
        :param coro:
        :param parent:
        :param priority:
        :param timeout:
        :return:
        """
        async with self._lock:
            if isinstance(coro, Sign):
                sign = coro
                if sign._id not in self._signs:
                    self._signs[sign._id] = sign
            else:
                sign_id = uuid.uuid4()
                sign = Sign(_id=sign_id, _coro=coro, _parent=parent, _priority=priority, _timeout=timeout)
                self._signs[sign_id] = sign
            if parent:
                parent._children.append(sign)
            await self._queue.put((sign._priority, sign))
            logger.debug(f"Submitted sign {sign._id} with priority {priority}")
            return sign

    def submit_threadsafe(self, coro: Callable[[], Coroutine[Any, Any, Any]], parent: Optional[Sign] = None,
                          priority: int = 0, timeout: Optional[float] = None) -> uuid.UUID:
        """
        同步，线程安全的提交任务到SignHub，并返回一个Sign对象id
        :param coro:
        :param parent:
        :param priority:
        :param timeout:
        :return:
        """
        sign_id = uuid.uuid4()
        sign = Sign(_id=sign_id, _coro=coro, _parent=parent, _priority=priority, _timeout=timeout)
        with self._thread_lock:
            self._signs[sign_id] = sign
            if parent:
                parent._children.append(sign)
            asyncio.run_coroutine_threadsafe(self._queue.put((priority, sign)), self._loop)
        logger.debug(f"Threadsafe submitted sign {sign_id} with priority {priority}")
        return sign_id

    async def _run(self) -> None:
        """
        任务调度的主循环
        The main loop of task scheduling
        """
        while self._running or not self._queue.empty():
            try:
                priority, sign = await self._queue.get()
                sign._task = asyncio.create_task(sign.run(self))
                await sign._task
                self._queue.task_done()
            except (asyncio.CancelledError, RuntimeError) as e:
                logger.debug(f"SignHub _run stopped due to: {e}")
                break
        self._running = False

    def emit(self, event: str, sign: Sign) -> None:
        """
        触发一个事件，并传递一个Sign对象
        Trigger an event and pass a Sign object
        """
        if event in self._listeners:
            for listener in self._listeners[event]:
                asyncio.create_task(listener.on_event(event, sign))

    def add_listener(self, event: str, listener: EventListener) -> None:
        """
        添加一个事件监听器
        Add an event listener
        """
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(listener)

    def _remove_child(self, parent: Sign, child: Sign) -> None:
        """
        从父级中移除一个子级
        Remove a child from the parent
        """
        if child in parent._children:
            parent._children.remove(child)

    def get_sign(self, sign_id: uuid.UUID) -> Optional[Sign]:
        """
        根据id获取一个Sign对象
        Get a Sign object by id
        """
        return self._signs.get(sign_id)

    async def cancel_task(self, sign_id: uuid.UUID) -> bool:
        """
        取消指定任务
        Cancel the specified task
        """
        sign = self.get_sign(sign_id)
        if not sign:
            raise SignNotFoundError(f"Sign with ID {sign_id} not found in SignHub")
        return await sign.cancel()

    def add_simple_edge(self, from_sign: Sign, to_sign: Sign, data: Any = None) -> None:
        """
        为两个信号间添加一条简单的通道
        :param from_sign:
        :param to_sign:
        :param data:
        :return:
        """
        async def run_next(sign: Sign, hub: SignHub):
            if to_sign._status == SignStatus.PENDING:
                if data is not None:
                    to_sign.data = data
                if to_sign not in from_sign._children:
                    from_sign._children.append(to_sign)
                to_sign._parent = from_sign
                await hub.submit(to_sign)
        from_sign.add_callback("on_finished", run_next)
        logger.debug(f"Added simple workflow edge from {from_sign._id} to {to_sign._id}")

    def add_conditional_edge(self, from_sign: Sign, result_map: Dict[Any, Sign], data: Any = None) -> None:
        """
        为两个信号间添加一条条件通道
        """
        async def run_next(sign: Sign, hub: SignHub):
            result = sign._result
            to_sign = result_map.get(result)
            if to_sign and to_sign._status == SignStatus.PENDING:
                if data is not None:
                    to_sign.data = data
                if to_sign not in from_sign._children:
                    from_sign._children.append(to_sign)
                to_sign._parent = from_sign
                await hub.submit(to_sign)
        from_sign.add_callback("on_finished", run_next)
        logger.debug(f"Added conditional workflow edge from {from_sign._id} with result map")



