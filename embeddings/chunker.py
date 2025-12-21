# embeddings/chunker.py

def chunk_text(text, max_chars=500):
    """
    Splits text into semantic chunks of ~max_chars
    """
    if not text:
        return []

    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks = []
    current_chunk = ""

    for para in paragraphs:
        if len(current_chunk) + len(para) <= max_chars:
            current_chunk += " " + para
        else:
            chunks.append(current_chunk.strip())
            current_chunk = para

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks
