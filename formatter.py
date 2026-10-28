import os
import re
from datetime import datetime
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn, nsdecls


def clean_title(title: str) -> str:
    """Очищает заголовок от markdown-разметки, эмодзи и неразрывных пробелов."""
    text = title.replace('\xa0', ' ').replace('&nbsp;', ' ')
    text = re.sub(r'\*+', '', text)
    text = re.sub(r'^[^\wА-Яа-яЁё]+', '', text)
    return text.strip()


def format_date(date_str: str) -> str:
    """Преобразует дату из YYYY-MM-DD в DD.MM.YYYY."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        return date_str


def get_source_label(link: str) -> str:
    """Определяет читаемое название источника по URL."""
    link_lower = link.lower()
    if 't.me/centralbank_russia' in link_lower:
        return 'Телеграм-канал Банка России'
    elif 'cbr.ru' in link_lower:
        return 'сайт Банка России'
    elif 'kremlin.ru' in link_lower:
        return 'сайт Администрации Президента'
    elif 'roskazna.gov.ru' in link_lower:
        return 'сайт Федерального казначейства'
    elif 'ach.gov.ru' in link_lower:
        return 'сайт Счетной палаты'
    return 'официальный источник'


def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    """Устанавливает внутренние отступы ячейки (padding)."""
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement('w:tcMar')
    for m, val in [('w:top', top), ('w:bottom', bottom), ('w:left', left), ('w:right', right)]:
        node = OxmlElement(m)
        node.set(qn('w:w'), str(val))
        node.set(qn('w:type'), 'dxa')
        tcMar.append(node)
    tcPr.append(tcMar)


def set_table_borders(table, color="D3D3D3", sz="4", val="single"):
    """Задает тонкие границы таблицы."""
    tblPr = table._tbl.tblPr
    borders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>\n'
        f'  <w:top w:val="{val}" w:sz="{sz}" w:space="0" w:color="{color}"/>\n'
        f'  <w:bottom w:val="{val}" w:sz="{sz}" w:space="0" w:color="{color}"/>\n'
        f'  <w:insideH w:val="{val}" w:sz="{sz}" w:space="0" w:color="{color}"/>\n'
        f'  <w:insideV w:val="{val}" w:sz="{sz}" w:space="0" w:color="{color}"/>\n'
        f'  <w:left w:val="none"/>\n'
        f'  <w:right w:val="none"/>\n'
        f'</w:tblBorders>'
    )
    tblPr.append(borders)


def create_digest_document(items: list[dict], output_path: str = "дайджест_новостей.docx"):
    """
    Создает Word-документ с предисловием и таблицей новостей с пропорциями колонок 25% - 50% - 25%.
    """
    doc = Document()

    # 1. Настройка полей страницы
    section = doc.sections[0]
    section.top_margin = Inches(0.8)
    section.bottom_margin = Inches(0.8)
    section.left_margin = Inches(0.8)
    section.right_margin = Inches(0.8)

    # Вычисляем общую полезную ширину страницы между полями
    content_width = section.page_width - section.left_margin - section.right_margin

    # Пропорции 25% - 50% - 25%
    col_widths = [
        content_width * 0.25,
        content_width * 0.50,
        content_width * 0.25
    ]

    # ------------------ ПРЕДИСЛОВИЕ ------------------
    h1 = doc.add_paragraph()
    h1_run = h1.add_run("Новости")
    h1_run.font.name = "Calibri"
    h1_run.font.size = Pt(18)
    h1_run.font.bold = True
    h1.paragraph_format.space_after = Pt(4)

    sub_p = doc.add_paragraph()
    sub_run = sub_p.add_run("Полный текст новости доступен по ссылке в QR коде.")
    sub_run.font.name = "Calibri"
    sub_run.font.size = Pt(11)
    sub_run.font.italic = True
    sub_p.paragraph_format.space_after = Pt(8)

    src_title_p = doc.add_paragraph()
    src_title_run = src_title_p.add_run("Используемые источники:")
    src_title_run.font.name = "Calibri"
    src_title_run.font.size = Pt(10.5)
    src_title_run.font.bold = True
    src_title_p.paragraph_format.space_after = Pt(2)

    sources = [
        "Сайт Банка России cbr.ru",
        "Сайт Администрации Президента kremlin.ru",
        "Сайт Федерального казначейства roskazna.gov.ru",
        "Сайт Счетной палаты ach.gov.ru",
        "Телеграм-канал Банка России t.me/centralbank_russia"
    ]
    for src in sources:
        p = doc.add_paragraph()
        r = p.add_run(src)
        r.font.name = "Calibri"
        r.font.size = Pt(10)
        p.paragraph_format.space_after = Pt(1)
        p.paragraph_format.line_spacing = 1.05

    doc.add_paragraph().paragraph_format.space_after = Pt(6)

    # ------------------ ТАБЛИЦА ------------------
    table = doc.add_table(rows=0, cols=3)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    set_table_borders(table)

    # Задаем ширину таблицы на 100% (5000 пятидесятых долей процента)
    tblPr = table._tbl.tblPr
    tblW = tblPr.find(qn('w:tblW'))
    if tblW is None:
        tblW = OxmlElement('w:tblW')
        tblPr.append(tblW)
    tblW.set(qn('w:type'), 'pct')
    tblW.set(qn('w:w'), '5000')

    # Пропорции колонок в pct: 25% = 1250, 50% = 2500, 25% = 1250
    pct_widths = ['1250', '2500', '1250']

    for item in items:
        row = table.add_row()
        trPr = row._tr.get_or_add_trPr()
        trPr.append(parse_xml(f'<w:cantSplit {nsdecls("w")}/>'))

        cell_meta, cell_summary, cell_qr = row.cells

        # Применяем процентную ширину к ячейкам
        for idx, cell in enumerate([cell_meta, cell_summary, cell_qr]):
            tcPr = cell._tc.get_or_add_tcPr()
            tcW = tcPr.find(qn('w:tcW'))
            if tcW is None:
                tcW = OxmlElement('w:tcW')
                tcPr.append(tcW)
            
            # Указываем процентную ширину без вызова .dxa
            tcW.set(qn('w:type'), 'pct')
            tcW.set(qn('w:w'), pct_widths[idx])

            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            set_cell_margins(cell, top=120, bottom=120, left=100, right=100)

        # 1. Колонка (25%): Метаданные
        p_title = cell_meta.paragraphs[0]
        p_title.paragraph_format.space_after = Pt(4)
        p_title.paragraph_format.line_spacing = 1.1
        r_title = p_title.add_run(clean_title(item.get("title", "")))
        r_title.font.name = "Calibri"
        r_title.font.size = Pt(10)
        r_title.font.bold = True

        p_date = cell_meta.add_paragraph()
        p_date.paragraph_format.space_after = Pt(2)
        r_date = p_date.add_run(f"Дата публикации: {format_date(item.get('pub_date', ''))}")
        r_date.font.name = "Calibri"
        r_date.font.size = Pt(9.5)
        r_date.font.color.rgb = RGBColor(90, 90, 90)

        p_src = cell_meta.add_paragraph()
        p_src.paragraph_format.space_after = Pt(0)
        source_name = get_source_label(item.get("link", ""))
        r_src = p_src.add_run(f"Источник: {source_name}")
        r_src.font.name = "Calibri"
        r_src.font.size = Pt(9.5)
        r_src.font.color.rgb = RGBColor(90, 90, 90)

        # 2. Колонка (50%): Текст
        p_summary = cell_summary.paragraphs[0]
        p_summary.paragraph_format.space_after = Pt(0)
        p_summary.paragraph_format.line_spacing = 1.15
        p_summary.alignment = WD_ALIGN_PARAGRAPH.LEFT
        r_summary = p_summary.add_run(item.get("summary", ""))
        r_summary.font.name = "Calibri"
        r_summary.font.size = Pt(10)

        # 3. Колонка (25%): QR-код
        p_qr = cell_qr.paragraphs[0]
        p_qr.paragraph_format.space_after = Pt(0)
        p_qr.alignment = WD_ALIGN_PARAGRAPH.CENTER

        qr_path = item.get("qr_path")
        if qr_path and os.path.exists(qr_path):
            r_qr = p_qr.add_run()
            # Ширина изображения чуть меньше ширины ячейки (1.3 дюйма отлично вписывается в 25%)
            r_qr.add_picture(qr_path, width=Inches(1.3))
        else:
            r_qr = p_qr.add_run("[QR-код]")
            r_qr.font.size = Pt(8.5)
            r_qr.font.italic = True

    # Фиксируем ширину колонок в структуре таблицы
    for i, col in enumerate(table.columns):
        col.width = int(col_widths[i])

    doc.save(output_path)
    print(f"Документ успешно сохранен: {output_path}")