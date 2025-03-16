#### DESC
flame-core是一个**轻量级、高效的异步任务流**框架，
旨在简化复杂异步任务的调度和管理。
无论是处理大规模并发任务、构建灵活的工作流，
还是对文本进行语义分割并存储到向量数据库，
flame-core 都能提供强大而直观的工具。
它结合了**异步编程**的性能优势和模块化设计，
适用于从简单脚本到复杂应用的各种场景

flame-core is a lightweight, 
efficient asynchronous task flow framework 
designed to simplify the scheduling 
and management of complex asynchronous tasks. 
Whether you're working with massively concurrent 
tasks, building flexible workflows, or semantically 
segmenting text and storing it in a vector database, flame provides powerful and intuitive tools. It combines the performance benefits of asynchronous programming with a modular design for a wide range of scenarios, from simple scripts to complex applications

#### CORE
 - 异步任务调度：通过 SignHub 管理任务的生命周期，支持优先级、超时和取消。
 - 工作流编排：使用 Workflow 轻松定义任务之间的依赖关系和执行顺序。
 - 语义文本分割：SemanticTextSplitter 提供基于语义的文本分块，支持最大句子数和 token 限制。
 - 向量存储集成：AsyncQdrantVector 实现高效的异步向量存储，适用于嵌入和搜索任务。
 - 可扩展性：模块化设计，支持自定义回调和事件监听

 - **Asynchronous Task Scheduling**: Manage task lifecycles with `SignHub`, supporting priorities, timeouts, and cancellation.
- **Workflow Orchestration**: Define task dependencies and execution order effortlessly using `Workflow`.
- **Semantic Text Splitting**: Use `SemanticTextSplitter` for meaning-aware text chunking, with configurable sentence and token limits.
- **Vector Storage Integration**: Store and manage embeddings asynchronously with `AsyncQdrantVector`.
- **Extensibility**: Modular architecture with custom callbacks and event listeners

```bash
pip install flame-core
```

#### Quick start
```python
import asyncio
from flame-core import SignHub, Workflow, SemanticTextSplitter

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


