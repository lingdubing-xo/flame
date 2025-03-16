from typing import Callable, Any, Coroutine, Optional, Dict, List
import uuid
import logging
from ._emperor import Sign, SignStatus, SignHub

logger = logging.getLogger("Workflow")

logger = logging.getLogger("Workflow")

class Workflow:
    def __init__(self, hub: SignHub, name: str = "Unnamed Workflow"):
        """
        初始化工作流
        Initialize the workflow
        """
        self.hub = hub
        self.name = name
        self.nodes: Dict[str, Sign] = {}
        self.edges: List[tuple[str, str, Optional[Callable[[Any], bool]]]] = []

    def add_node(self, node_id: str, coro: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        """
        添加一个节点
        Add a node
        """
        sign = Sign(_id=uuid.uuid4(), _coro=coro)
        self.nodes[node_id] = sign

    def add_edge(self, from_id: str, to_id: str, condition: Optional[Callable[[Any], bool]] = None) -> None:
        """
        添加一个边
        Add an edge
        """
        if from_id not in self.nodes or to_id not in self.nodes:
            raise ValueError(f"Node {from_id} or {to_id} not found")
        self.edges.append((from_id, to_id, condition))

    async def run(self) -> None:
        """
        运行工作流
        """
        independent_nodes = {nid for nid in self.nodes if not any(e[1] == nid for e in self.edges)}
        for node_id in independent_nodes:
            await self.hub.submit(self.nodes[node_id])

        for from_id, to_id, condition in self.edges:
            from_sign = self.nodes[from_id]
            to_sign = self.nodes[to_id]

            async def edge_callback(sign: Sign, hub: SignHub):
                if sign._status == SignStatus.FINISHED:
                    logger.info(f"Edge triggered: {from_id} -> {to_id}, result: {sign._result}")
                    if condition is None or condition(sign._result):
                        to_sign.data = sign._result  # 确保数据传递
                        logger.info(f"Data passed to {to_id}: {to_sign.data}")
                        await hub.submit(to_sign)

            from_sign.add_callback("on_finished", edge_callback)

        logger.info(f"Workflow '{self.name}' started")
