import asyncio
from .emperor import SignHub, Workflow, SignStatus
from .weapons import SimpleContext
from .nomadic import SemanticTextSplitter, AsyncQdrantVector
from typing import List
import logging

logger = logging.getLogger("Example")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def split_text_task(text: str) -> List[str]:
    splitter = SemanticTextSplitter(max_sentences=2, max_tokens=1000, semantic=True)
    result = [chunk async for chunk in splitter.split(text)]
    logger.info(f"Split result: {result}")
    return result

async def next_step_task(chunks: List[str]) -> None:
    logger.info(f"Next step received chunks: {chunks}")
    # 这里可以添加后续处理逻辑，例如打印或存储
    return None

async def main():
    # Initialize the task hub
    hub = SignHub()
    await hub.start()

    # Define a workflow
    wf = Workflow(hub, "Text Processing")
    wf.add_node("split", lambda: split_text_task("Cats are cute. Dogs are loyal. Birds fly."))
    wf.add_node("next_step", lambda: next_step_task(wf.nodes["split"].data))  # 添加 next_step 节点
    wf.add_edge("split", "next_step")  # 连接两个节点

    # Run the workflow
    await wf.run()

    # Wait for tasks to complete
    while not all(node.is_finished for node in wf.nodes.values()):
        await asyncio.sleep(1)
        logger.info("Waiting for tasks to complete...")

    # Log results
    split_result = wf.nodes["split"]._result if wf.nodes["split"]._status == SignStatus.FINISHED else "Failed"
    next_result = wf.nodes["next_step"]._result if wf.nodes["next_step"]._status == SignStatus.FINISHED else "Failed"
    logger.info(f"Split result: {split_result}")
    logger.info(f"Next step result: {next_result}")

    await hub.stop()

if __name__ == "__main__":
    asyncio.run(main())
