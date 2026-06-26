import fitz  # PyMuPDF
import easyocr
from pathlib import Path


reader = easyocr.Reader(["ru", "en"])

def get_pdf_text(pdf_path: Path) -> str:
    all_text: list[str] = []

    doc = fitz.open(pdf_path)

    for page_index, page in enumerate(doc):
        # Растеризуем страницу в изображение с высоким DPI
        pixmap = page.get_pixmap(dpi=300)
        image_bytes = pixmap.tobytes("png")

        # EasyOCR умеет принимать bytes напрямую
        results = reader.readtext(image_bytes)

        page_text = " ".join(text for _, text, _ in results)
        all_text.append(page_text)

        print(f"Страница {page_index + 1}/{len(doc)} — {len(results)} фрагментов")

    doc.close()

    full_text = "\n\n".join(all_text)

    return full_text