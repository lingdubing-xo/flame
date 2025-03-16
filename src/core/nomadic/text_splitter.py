import re
import asyncio
from typing import Iterable, List, AsyncIterable, Optional
import tiktoken
import numpy as np
from sklearn.cluster import KMeans
from src.core.weapons.context_model import ContextModelBase

class SemanticTextSplitter:
    def __init__(
            self,
            max_sentences: int = 5,
            max_tokens: int = 4000,
            semantic: bool = True,
            semantic_threshold: float = 0.3,
            model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
            context_model: Optional[ContextModelBase[str]] = None,
            encoding_name: str = "cl100k_base"
    ):
        try:
            from sentence_transformers import SentenceTransformer
            self.embedding_model = SentenceTransformer(model_name)
            self._embed_func = self.embedding_model.encode
        except ImportError:
            raise ImportError("Please install sentence-transformers: pip install sentence-transformers")

        self.max_sentences = max_sentences
        self.max_tokens = max_tokens
        self.semantic = semantic
        self.semantic_threshold = semantic_threshold
        self.context_model = context_model
        self.tokenizer = tiktoken.get_encoding(encoding_name) if not context_model else None
        if not 0 <= semantic_threshold <= 1:
            raise ValueError("'semantic_threshold' must be between 0 and 1.")

    def _count_tokens(self, text: str) -> int:
        if self.context_model:
            return self.context_model.token_counter(text)
        return len(self.tokenizer.encode(text))

    async def split(self, text: str) -> AsyncIterable[str]:  # 修改为异步接口
        if not text or not text.strip():
            return
        if self.semantic:
            async for chunk in self._split_by_semantic(text):
                yield chunk
        else:
            for chunk in self._split_by_sentence(text):
                yield chunk

    def _split_by_sentence(self, text: str) -> Iterable[str]:
        sentences = re.split(r'(?<=[。！？\.!?])', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        if not sentences:
            return

        current_chunk = []
        current_tokens = 0
        current_sentence_count = 0

        for sentence in sentences:
            sentence_tokens = self._count_tokens(sentence)
            if (current_sentence_count >= self.max_sentences or
                    current_tokens + sentence_tokens > self.max_tokens):
                if current_chunk:
                    yield "".join(current_chunk).strip()
                current_chunk = [sentence]
                current_tokens = sentence_tokens
                current_sentence_count = 1
            else:
                current_chunk.append(sentence)
                current_tokens += sentence_tokens
                current_sentence_count += 1

        if current_chunk:
            yield "".join(current_chunk).strip()

    async def _get_embeddings_async(self, texts: List[str]) -> List[List[float]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self._embed_func(texts, convert_to_numpy=True).tolist())

    async def _split_by_semantic(self, text: str) -> AsyncIterable[str]:
        sentences = re.split(r'(?<=[。！？\.!?])', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        if len(sentences) <= 1:
            for sentence in sentences:
                yield sentence.strip()
            return

        embeddings = await self._get_embeddings_async(sentences)
        n_clusters = max(1, min(len(sentences) // self.max_sentences, len(sentences)))
        if n_clusters > 1:
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = kmeans.fit_predict(np.array(embeddings))
        else:
            cluster_labels = [0] * len(sentences)

        current_chunk = []
        current_tokens = 0
        current_sentence_count = 0
        current_cluster = cluster_labels[0]

        for idx, (sentence, cluster) in enumerate(zip(sentences, cluster_labels)):
            sentence_tokens = self._count_tokens(sentence)
            should_split = (
                    current_sentence_count >= self.max_sentences or
                    current_tokens + sentence_tokens > self.max_tokens or
                    (cluster != current_cluster and current_chunk and current_sentence_count > 1)
            )
            if should_split and current_chunk:
                yield "".join(current_chunk).strip()
                current_chunk = [sentence]
                current_tokens = sentence_tokens
                current_sentence_count = 1
                current_cluster = cluster
            else:
                current_chunk.append(sentence)
                current_tokens += sentence_tokens
                current_sentence_count += 1
                current_cluster = cluster

        if current_chunk:
            yield "".join(current_chunk).strip()

