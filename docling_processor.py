from docling.document_converter import DocumentConverter
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["tender_db"]

documents = db["tender_documents"]
docling_outputs = db["docling_outputs"]

converter = DocumentConverter()


def _serialize_docling_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize_docling_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _serialize_docling_value(v) for k, v in value.items()}
    for attr in ("model_dump", "to_dict", "dict"):
        method = getattr(value, attr, None)
        if callable(method):
            try:
                return _serialize_docling_value(method())
            except Exception:
                break
    if hasattr(value, "__dict__"):
        return {str(k): _serialize_docling_value(v) for k, v in value.__dict__.items()}
    return str(value)


def process_pending_documents(limit=10):
    """
    Process PDFs that are not yet passed through Docling
    """

    pending_docs = documents.find(
        {"docling_status": "pending"},
        limit=limit
    )

    for doc in pending_docs:
        document_id = doc["_id"]
        tender_id = doc["tender_id"]
        pdf_path = doc["local_path"]

        print(f"📄 Docling → {pdf_path}")

        try:
            result = converter.convert(pdf_path)

            sections_value = getattr(result.document, "sections", None)

            docling_outputs.update_one(
                {"document_id": document_id},
                {
                    "$set": {
                        "tender_id": tender_id,
                        "document_id": document_id,
                        "text": result.document.export_to_text(),
                        "tables": _serialize_docling_value(result.document.tables),
                        "sections": _serialize_docling_value(sections_value),
                        "extracted_at": datetime.utcnow(),
                        "docling_version": "v1"
                    }
                },
                upsert=True
            )

            documents.update_one(
                {"_id": document_id},
                {"$set": {"docling_status": "done"}}
            )

            print("✅ Docling success")

        except Exception as e:
            documents.update_one(
                {"_id": document_id},
                {"$set": {"docling_status": "failed"}}
            )
            print("❌ Docling failed:", e)


def main():
    process_pending_documents()


if __name__ == "__main__":
    main()
