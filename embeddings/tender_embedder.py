# embeddings/tender_embedder.py

from __future__ import annotations

import os
from typing import Iterable, List, Sequence, Union

from sentence_transformers import SentenceTransformer

DEFAULT_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")


class TenderEmbedder:
    def __init__(self, model_name: str | None = None) -> None:
        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.model = SentenceTransformer(self.model_name)

    def embed(self, texts: Union[str, Sequence[str]]) -> List[List[float]]:
        """
        Encode text(s) into embedding vectors.
        """
        normalized = self._normalize_texts(texts)
        if not normalized:
            return []

        vectors = self.model.encode(
            normalized,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return vectors.tolist()

    @staticmethod
    def _normalize_texts(texts: Union[str, Sequence[str]]) -> List[str]:
        if isinstance(texts, str):
            return [texts]

        if isinstance(texts, Iterable):
            return [t for t in texts if isinstance(t, str) and t.strip()]

        return []
