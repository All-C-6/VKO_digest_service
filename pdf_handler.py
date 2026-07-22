import json

import fitz  # PyMuPDF
import easyocr
from pathlib import Path

import opendataloader_pdf


reader = easyocr.Reader(["ru", "en"])
text_objects = {"heading", "paragraph", "list item"}

def extract_pdf_text_with_EasyOCR(pdf_path: Path) -> str:
    all_text: list[str] = []

    doc = fitz.open(pdf_path)

    for page_index, page in enumerate(doc):
        # Растеризуем страницу в изображение с высоким DPI
        pixmap = page.get_pixmap(dpi=300)
        image_bytes = pixmap.tobytes("png")

        results = reader.readtext(image_bytes)

        page_text = " ".join(text for _, text, _ in results)
        all_text.append(page_text)

        print(f"Страница {page_index + 1}/{len(doc)} — {len(results)} фрагментов")

    doc.close()

    full_text = "\n\n".join(all_text)

    return full_text


def get_text_from_opendataloader_json(orig_pdf_path: Path) -> str:
    filename = orig_pdf_path.stem

    filepath = Path("output") / f"{filename}.json"

    with open(filepath, "r") as ocr_json_fule:
        ocr_json_data = json.load(ocr_json_fule)

    parts = ocr_json_data["kids"]

    ocr_full_text = [part for part in parts if part["type"] in text_objects]


def extract_pdf_text_with_opendataloader(pdf_path: Path) -> str:
    # Batch all files in one call — each convert() spawns a JVM process, so repeated calls are slow
    opendataloader_pdf.convert(
        input_path=pdf_path._str,
        output_dir="/home/all-c/Документы/Dophamine/Python/VKO_sources_service/pdf_output",
        hybrid="docling-fast"
    )


if __name__ == "__main__":
    path = Path("/home/all-c/Загрузки/7317-У_27052026.pdf")
    if path.exists():
        get_text_from_opendataloader_json(path)