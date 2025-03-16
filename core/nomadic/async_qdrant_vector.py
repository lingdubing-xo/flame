from __future__ import annotations
from datetime import datetime, timezone
from typing import Iterable, Literal, Any, AsyncIterable
import asyncio
import uuid
from .text_splitter import SemanticTextSplitter

try:
    from qdrant_client import AsyncQdrantClient, models
except ImportError:
    raise ImportError("Please install qdrant-client: pip install qdrant-client")

class AsyncQdrantVector:
    def __init__(
        self,
        position: str,
        position_type: Literal["memory", "disk", "server", "cloud"],
        api_key: str | None = None,
        text_splitter: SemanticTextSplitter | None = None,
    ):
        self._init_qdrant_client(position, position_type, api_key)
        self.text_splitter = text_splitter or SemanticTextSplitter(
            max_sentences=2,
            max_tokens=1000,
            semantic=True,
            semantic_threshold=0.3
        )

    def _init_qdrant_client(self, position: str, position_type: str, api_key: str | None):
        """
        初始化 Qdrant 客户端
        Initialize the Qdrant client
        """
        if position_type == "memory":
            self.client = AsyncQdrantClient(location=":memory:")
        elif position_type == "disk":
            self.client = AsyncQdrantClient(path=position)
        elif position_type == "server":
            self.client = AsyncQdrantClient(url=position, api_key=api_key)
        elif position_type == "cloud":
            if not api_key:
                raise ValueError("API key required for cloud position type")
            self.client = AsyncQdrantClient(url=position, api_key=api_key)
        else:
            raise ValueError(f"Unsupported position_type: {position_type}")

    async def create_collection(self, collection_name: str, **kwargs) -> bool:
        """
        创建集合，使用默认向量配置
        Create a collection with default vector configuration
        """
        return await self.client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=384,  # all-MiniLM-L6-v2 的向量维度
                distance=models.Distance.COSINE
            ),
            **kwargs
        )

    async def add(
        self,
        collection_name: str,
        documents: Iterable[str],
        metadata: Iterable[dict[str, Any]] | None = None,
        ids: Iterable[str] | None = None,
        **kwargs
    ) -> list[str]:
        """
        添加文档到集合
        Add documents to the collection
        """
        current_time = datetime.now(timezone.utc).isoformat()
        texts = []
        payloads = []

        # 分割文档并准备元数据
        for i, doc in enumerate(documents):
            async for chunk in self.text_splitter._split_by_semantic(doc):
                texts.append(chunk)
                payload = {"text": chunk, "created_at": current_time}
                if metadata and i < len(metadata):
                    payload.update(metadata[i])
                payloads.append(payload)

        # 生成嵌入
        embeddings = await self.text_splitter._get_embeddings_async(texts)

        # 准备点数据，使用 UUID 作为 id
        points = []
        id_list = list(ids) if ids else [str(uuid.uuid4()) for _ in range(len(texts))]
        for i, (embedding, payload) in enumerate(zip(embeddings, payloads)):
            point_id = id_list[i] if i < len(id_list) else str(uuid.uuid4())
            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload
                )
            )

        await self.client.upsert(collection_name=collection_name, points=points, **kwargs)
        return [point.id for point in points]

    async def query(
        self,
        collection_name: str,
        query_text: str,
        limit: int = 5,
        return_text: bool = True,
        **kwargs
    ) -> list[str] | list[models.ScoredPoint]:
        """
        查询相似文档
        Query similar documents
        """
        query_embedding = (await self.text_splitter._get_embeddings_async([query_text]))[0]
        results = await self.client.search(
            collection_name=collection_name,
            query_vector=query_embedding,
            limit=limit,
            **kwargs
        )
        if return_text:
            return [result.payload.get("text", "") for result in results]
        return results

    async def delete_collection(self, collection_name: str) -> None:
        """
        删除集合
        Delete the collection
        """
        await self.client.delete_collection(collection_name=collection_name)

    async def does_collection_exist(self, collection_name: str) -> bool:
        """
        检查集合是否存在
        Check if the collection exists
        """
        return await self.client.collection_exists(collection_name=collection_name)

    async def count(self, collection_name: str) -> int:
        """
        统计集合中的点数
        Count the number of points in the collection
        """
        result = await self.client.count(collection_name=collection_name)
        return result.count
