import pdfplumber
import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
import re
import json
# import openai

### Step 1: Identify PDF Type (Text or Scanned)
def is_text_based_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text and len(text.strip()) > 50:
                return True
    return False  # If no text, assume scanned PDF

### Step 2: Extract Text and Structure It
def extract_text_with_structure(pdf_path):
    structured_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text()
            if text:
                structured_text += f"\n\n### Page {page_num}: Text Section\n"
                structured_text += text + "\n"
    return structured_text

### Step 3: Extract Tables (Aligned & Misaligned)
def extract_table_from_pdf(pdf_path):
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.append(table)
    return tables

### Step 4: Extract Key-Value Pairs (For invoices with fields like Invoice No, GSTIN, etc.)
def extract_key_value_pairs(pdf_path):
    key_values = {}
    doc = fitz.open(pdf_path)
    for page in doc:
        text_blocks = page.get_text("blocks")
        for block in text_blocks:
            text = block[4]  # Extracted text
            if ":" in text:  # Key-value pair format
                parts = text.split(":")
                if len(parts) == 2:
                    key, value = parts[0].strip(), parts[1].strip()
                    key_values[key] = value
    return key_values

### Step 5: OCR for Scanned PDFs
def ocr_pdf(pdf_path):
    images = convert_from_path(pdf_path)
    extracted_text = ""
    for image in images:
        text = pytesseract.image_to_string(image, config="--psm 6")  # Preserve key-value structure
        extracted_text += text + "\n"
    return extracted_text

### Step 6: Extract Embedded Images
def extract_images(pdf_path):
    doc = fitz.open(pdf_path)
    image_positions = []
    for page_num, page in enumerate(doc, 1):
        images = page.get_images(full=True)
        for img_index, img in enumerate(images, 1):
            image_positions.append(f"Page {page_num}: Image {img_index} at (x={img[2]}, y={img[3]})")
    return image_positions

def generate_markdown(invoice_data, structured_text, tables, image_positions):
    markdown_text = "## NIC codes data\n\n"

    # Invoice Details (Key-Value Pairs)
    markdown_text += "### Details\n"
    for key, value in invoice_data.items():
        markdown_text += f"- **{key}**: {value}\n"

    # Extracted Text Sections
    if structured_text:
        markdown_text += "\n### Extracted Text Sections\n"
        markdown_text += structured_text + "\n"

    # Table Sections (Handles Aligned & Misaligned Tables)
    if tables:
        markdown_text += "\n### Extracted Tables\n"
        for table in tables:
            if not table or not table[0]:  # Ensure table is not empty
                continue

            headers = [str(h) if h else "Unknown" for h in table[0]]  # Ensure headers are strings
            markdown_text += "| " + " | ".join(headers) + " |\n"
            markdown_text += "| " + " | ".join(["-" * len(h) for h in headers]) + " |\n"

            for row in table[1:]:
                row = [str(cell) if cell else "N/A" for cell in row]  # Ensure all row values are strings
                markdown_text += "| " + " | ".join(row) + " |\n"

    # Embedded Images
    if image_positions:
        markdown_text += "\n### Detected Embedded Images\n"
        for image in image_positions:
            markdown_text += f"- {image}\n"

    return markdown_text


### Step 8: Process PDF Using Hybrid Approach
def process_pdf(pdf_path):
    is_text_pdf = is_text_based_pdf(pdf_path)

    if is_text_pdf:
        invoice_data = extract_key_value_pairs(pdf_path)  # Extract key-value structured data
        structured_text = extract_text_with_structure(pdf_path)  # Extract structured text
        tables = extract_table_from_pdf(pdf_path)  # Extract tables
    else:
        structured_text = ocr_pdf(pdf_path)  # OCR for scanned PDFs
        invoice_data = extract_key_value_pairs(pdf_path)  # Extract key-value data using OCR text
        tables = []  # OCR might not retain table structure

    image_positions = extract_images(pdf_path)  # Extract images if present

    # Convert extracted data into structured Markdown for LLM
    markdown_text = generate_markdown(invoice_data, structured_text, tables, image_positions)

    return markdown_text
