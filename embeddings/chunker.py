# embeddings/chunker.py

from typing import List


def chunk_text(text: str, max_chars: int = 500) -> List[str]:
    """
    Split text into chunks of up to max_chars using paragraph boundaries.
    """
    if not text or max_chars <= 0:
        return []

    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks: List[str] = []
    current_chunk = ""

    for para in paragraphs:
        if not current_chunk:
            current_chunk = para
            continue

        if len(current_chunk) + 1 + len(para) <= max_chars:
            current_chunk += " " + para
        else:
            chunks.append(current_chunk)
            current_chunk = para

    if current_chunk:
        chunks.append(current_chunk)

    return chunks
