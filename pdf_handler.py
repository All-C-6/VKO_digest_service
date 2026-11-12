import json
import os
import tempfile
import fitz  # PyMuPDF
import easyocr
from pathlib import Path
import httpx
import opendataloader_pdf

from utils import setup_logging

logger = setup_logging("logs/pdf_handler.log", level="INFO")


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


def get_text_from_opendataloader_json(orig_pdf_path: str, output_folder = "output/pdf_output/") -> str:
    filename = Path(orig_pdf_path).stem

    filepath = Path(output_folder) / f"{filename}.json"

    with open(filepath, "r") as ocr_json_fule:
        ocr_json_data = json.load(ocr_json_fule)

    parts = ocr_json_data["kids"]

    ocr_full_text = [part["content"] for part in parts if part["type"] in text_objects]
    print(ocr_full_text)
    return "\n".join(ocr_full_text)


def get_pdf_summary(pdf_url: str) -> str:
    """
    Извлекает текст из PDF и возвращает его в виде Markdown.
    """

    # Сперва загружает PDF во временный файл
    """Скачивает PDF по ссылке и парсит его в чистый Markdown/структурированный текст."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
        tmp_path = tmp_file.name


    try:
        with httpx.Client(
            timeout=30, follow_redirects=True, headers=headers
        ) as client:
            logger.info(f"Скачиваем PDF {pdf_url} во временный файл {tmp_path}")
            with client.stream("GET", pdf_url) as resp:
                resp.raise_for_status()

                # 1. Быстрая проверка заголовка (отсекаем явный HTML/JSON)
                content_type = resp.headers.get("content-type", "").lower()
                if "text/html" in content_type or "application/json" in content_type:
                    logger.error(f"Ожидался PDF, но получен {content_type}")
                    return ""

                # 2. Проверка сигнатуры (магических байтов) при чтении чанков
                is_first_chunk = True
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=8192):
                        if not chunk:
                            continue
                        if is_first_chunk:
                            # По спецификации PDF заголовок %PDF- может быть в первых 1024 байтах
                            if b"%PDF-" not in chunk[:1024]:
                                logger.error("Файл не начинается с сигнатуры %PDF-")
                                return ""
                            is_first_chunk = False
                        f.write(chunk)

                # На случай, если ответ пришел пустым (0 байт)
                if is_first_chunk:
                    logger.error("Скачан пустой файл")
                    return ""

    except Exception as e:
        logger.exception(f"Ошибка при скачивании PDF {pdf_url}: {e}")
        # Удаляем битый/неверный файл, если он успел создаться
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return ""

    except Exception as e:
        logger.error(f"Ошибка при скачивании PDF: {e}")
        return ""
    else:
        logger.info(f"Успешно скачан PDF {pdf_url}")

    # Затем извлекает текст с помощью OpenDataLoader
    opendataloader_pdf.convert(
    input_path=tmp_path,
    output_dir="output/pdf_output/",
    hybrid="docling-fast",
    format="markdown"
    )

    # извлекает текст из JSON, сгенерированного OpenDataLoader
    with open(f"output/pdf_output/{Path(tmp_path).stem}.md", "r") as ocr_md_file:
        text = ocr_md_file.read()

    logger.info(f"Успешно считан текст из PDF {pdf_url}")

    return text