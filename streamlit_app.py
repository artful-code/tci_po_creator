import pandas as pd
import json
import streamlit as st
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
import pdfplumber
import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
import docx
import openpyxl
import email
import os
import re
import tempfile
from io import BytesIO

# Initialize OpenAI client
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# System prompt for accurate extraction
SYSTEM_PROMPT = """
You are an expert data extraction system specialized in analyzing transportation and logistics documents. Your task is to carefully extract information from purchase orders, invoices, and transport documents.

Extract the following fields EXACTLY as they appear in the document or as close as possible:
- FROM_LOCATION: The origin location of transportation
- TO_LOCATION: The destination location
- MATERIAL_TYPE: Type of materials being transported (e.g., "Autoparts", "Steel", "Electronics")
- BODY_TYPE: Vehicle or container type (e.g., "20FT Container", "Truck", "Van")
- FREQUENCY: How frequent the shipment occurs (numeric value if possible)
- WEIGHT: Weight of goods with unit (e.g., "200 T", "500 kg")
- RATE_UOM: Rate unit of measure (e.g., "Per KG", "Per Ton", "Per Trip")
- TRIPS_IN_MONTH: Number of trips per month (numeric value)
- START_DATE: Contract start date in YYYY-MM-DD format
- END_DATE: Contract end date in YYYY-MM-DD format

For any field not found in the document, use null. Analyze the entire document carefully, looking for these fields in headers, tables, key-value pairs, and body text.
Return ONLY a valid JSON object with these fields and nothing else.
"""

# User prompt template
USER_PROMPT_TEMPLATE = """
Extract the transportation order details from the following document text. 
The document contains details about shipping/transportation orders.
Return ONLY the JSON object with the required fields.

Here's the document content:
"""

# Function to extract text from PDF (using the provided code)
def process_pdf(file_content):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
        temp_file.write(file_content)
        temp_path = temp_file.name
    
    try:
        # Using the existing PDF processing function
        is_text_pdf = is_text_based_pdf(temp_path)

        if is_text_pdf:
            invoice_data = extract_key_value_pairs(temp_path)
            structured_text = extract_text_with_structure(temp_path)
            tables = extract_table_from_pdf(temp_path)
        else:
            structured_text = ocr_pdf(temp_path)
            invoice_data = extract_key_value_pairs(temp_path)
            tables = []

        image_positions = extract_images(temp_path)
        markdown_text = generate_markdown(invoice_data, structured_text, tables, image_positions)
        
        return markdown_text
    finally:
        # Clean up the temporary file
        os.unlink(temp_path)

# Function to check if PDF is text-based
def is_text_based_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text and len(text.strip()) > 50:
                return True
    return False

# Function to extract structured text from PDF
def extract_text_with_structure(pdf_path):
    structured_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text()
            if text:
                structured_text += f"\n\n### Page {page_num}: Text Section\n"
                structured_text += text + "\n"
    return structured_text

# Function to extract tables from PDF
def extract_table_from_pdf(pdf_path):
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.append(table)
    return tables

# Function to extract key-value pairs from PDF
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

# Function to OCR scanned PDFs
def ocr_pdf(pdf_path):
    images = convert_from_path(pdf_path)
    extracted_text = ""
    for image in images:
        text = pytesseract.image_to_string(image, config="--psm 6")
        extracted_text += text + "\n"
    return extracted_text

# Function to extract images from PDF
def extract_images(pdf_path):
    doc = fitz.open(pdf_path)
    image_positions = []
    for page_num, page in enumerate(doc, 1):
        images = page.get_images(full=True)
        for img_index, img in enumerate(images, 1):
            image_positions.append(f"Page {page_num}: Image {img_index} at (x={img[2]}, y={img[3]})")
    return image_positions

# Function to generate markdown from extracted PDF data
def generate_markdown(invoice_data, structured_text, tables, image_positions):
    markdown_text = "## Document data\n\n"

    # Invoice Details (Key-Value Pairs)
    markdown_text += "### Details\n"
    for key, value in invoice_data.items():
        markdown_text += f"- **{key}**: {value}\n"

    # Extracted Text Sections
    if structured_text:
        markdown_text += "\n### Extracted Text Sections\n"
        markdown_text += structured_text + "\n"

    # Table Sections
    if tables:
        markdown_text += "\n### Extracted Tables\n"
        for table in tables:
            if not table or not table[0]:
                continue

            headers = [str(h) if h else "Unknown" for h in table[0]]
            markdown_text += "| " + " | ".join(headers) + " |\n"
            markdown_text += "| " + " | ".join(["-" * len(h) for h in headers]) + " |\n"

            for row in table[1:]:
                row = [str(cell) if cell else "N/A" for cell in row]
                markdown_text += "| " + " | ".join(row) + " |\n"

    # Embedded Images
    if image_positions:
        markdown_text += "\n### Detected Embedded Images\n"
        for image in image_positions:
            markdown_text += f"- {image}\n"

    return markdown_text

# Function to extract text from DOCX files
def process_docx(file_content):
    markdown_text = "## Document data\n\n"
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as temp_file:
        temp_file.write(file_content)
        temp_path = temp_file.name
    
    try:
        doc = docx.Document(temp_path)
        
        # Extract paragraphs
        markdown_text += "\n### Text Content\n"
        for para in doc.paragraphs:
            if para.text.strip():
                markdown_text += para.text + "\n\n"
        
        # Extract tables
        if doc.tables:
            markdown_text += "\n### Tables\n"
            for i, table in enumerate(doc.tables):
                markdown_text += f"\n#### Table {i+1}\n"
                
                # Create the markdown table
                for row in table.rows:
                    cells = [cell.text.replace("\n", " ").strip() for cell in row.cells]
                    markdown_text += "| " + " | ".join(cells) + " |\n"
                    
                    # Add separator row after header (first row)
                    if row == table.rows[0]:
                        markdown_text += "| " + " | ".join(["---"] * len(row.cells)) + " |\n"
        
        return markdown_text
    finally:
        os.unlink(temp_path)

# Function to extract text from Excel files
def process_xlsx(file_content):
    markdown_text = "## Document data\n\n"
    
    try:
        # Load the workbook from bytes
        workbook = openpyxl.load_workbook(BytesIO(file_content))
        
        # Process each worksheet
        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]
            markdown_text += f"\n### Sheet: {sheet_name}\n\n"
            
            # Find the used range
            min_row, max_row = 1, sheet.max_row
            min_col, max_col = 1, sheet.max_column
            
            # Create a markdown table for the sheet data
            for row in range(min_row, max_row + 1):
                row_data = []
                for col in range(min_col, max_col + 1):
                    cell_value = sheet.cell(row=row, column=col).value
                    row_data.append(str(cell_value) if cell_value is not None else "")
                
                markdown_text += "| " + " | ".join(row_data) + " |\n"
                
                # Add separator after header (first row)
                if row == min_row:
                    markdown_text += "| " + " | ".join(["---"] * len(row_data)) + " |\n"
        
        return markdown_text
    except Exception as e:
        return f"## Error processing Excel file\n\nError: {str(e)}"

# Function to extract text from email (.eml) files
def process_eml(file_content):
    markdown_text = "## Email data\n\n"
    
    try:
        # Parse the email content
        msg = email.message_from_bytes(file_content)
        
        # Extract headers
        markdown_text += "### Email Headers\n"
        for header in ['From', 'To', 'Subject', 'Date']:
            value = msg.get(header, "")
            if value:
                markdown_text += f"- **{header}**: {value}\n"
        
        # Extract body
        markdown_text += "\n### Email Body\n"
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))
                
                # Extract text parts
                if content_type == "text/plain" and "attachment" not in content_disposition:
                    try:
                        body = part.get_payload(decode=True).decode()
                        markdown_text += body + "\n\n"
                    except:
                        markdown_text += "(Unable to decode plain text body)\n\n"
                elif content_type == "text/html" and "attachment" not in content_disposition:
                    try:
                        html_body = part.get_payload(decode=True).decode()
                        # Very simple HTML to text conversion
                        text_body = re.sub('<[^<]+?>', ' ', html_body)
                        markdown_text += text_body + "\n\n"
                    except:
                        markdown_text += "(Unable to decode HTML body)\n\n"
        else:
            try:
                body = msg.get_payload(decode=True).decode()
                markdown_text += body + "\n\n"
            except:
                markdown_text += "(Unable to decode email body)\n\n"
        
        # List attachments
        markdown_text += "\n### Attachments\n"
        has_attachments = False
        
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            
            filename = part.get_filename()
            if filename:
                has_attachments = True
                markdown_text += f"- {filename}\n"
        
        if not has_attachments:
            markdown_text += "No attachments found.\n"
        
        return markdown_text
    except Exception as e:
        return f"## Error processing Email file\n\nError: {str(e)}"

# Function to process a file based on its type
def process_file(file_content, file_extension):
    if file_extension.lower() == '.pdf':
        return process_pdf(file_content)
    elif file_extension.lower() == '.docx':
        return process_docx(file_content)
    elif file_extension.lower() == '.xlsx':
        return process_xlsx(file_content)
    elif file_extension.lower() == '.eml':
        return process_eml(file_content)
    else:
        return f"Unsupported file type: {file_extension}"

# Function to process a row from the dataframe
def process_row(row):
    file_name = row['file_name']
    file_content = row['file_content']
    file_extension = os.path.splitext(file_name)[1]
    
    # Extract text from the file based on its type
    extracted_text = process_file(file_content, file_extension)
    
    # Create the user prompt by combining the template and extracted text
    user_prompt = USER_PROMPT_TEMPLATE + "\n" + extracted_text
    
    try:
        # Call the LLM
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,  # Lower temperature for more consistent extraction
            max_tokens=1000,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        
        # Extract the content from the response
        content = response.choices[0].message.content
        
        # Remove code block markers if present
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        
        # Parse the cleaned content into JSON
        content = content.strip()
        response_json = json.loads(content)
        
        # Ensure all required fields are present
        required_fields = [
            "FROM_LOCATION", "TO_LOCATION", "MATERIAL_TYPE", "BODY_TYPE", 
            "FREQUENCY", "WEIGHT", "RATE_UOM", "TRIPS_IN_MONTH", 
            "START_DATE", "END_DATE"
        ]
        
        for field in required_fields:
            if field not in response_json:
                response_json[field] = None
        
        # Normalize the JSON into tabular format
        normalized = pd.json_normalize(response_json)
        
        # Add the file name for reference
        normalized["file_name"] = file_name
        
        return normalized
    
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        # Create a default dataframe with null values
        default_data = {
            "FROM_LOCATION": None, "TO_LOCATION": None, "MATERIAL_TYPE": None,
            "BODY_TYPE": None, "FREQUENCY": None, "WEIGHT": None,
            "RATE_UOM": None, "TRIPS_IN_MONTH": None, "START_DATE": None,
            "END_DATE": None, "file_name": file_name
        }
        return pd.DataFrame([default_data])

# Main function to process all files
def process_files(input_file, output_file, max_workers=10):
    # Read the input dataframe
    df = pd.read_csv(input_file)
    
    # Ensure the required columns exist
    if not {'file_name', 'file_content'}.issubset(df.columns):
        raise ValueError("Input CSV must contain 'file_name' and 'file_content' columns.")
    
    # Placeholder for normalized data
    normalized_data = []
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
        
        for future in futures:
            result = future.result()
            if result is not None:
                normalized_data.append(result)
    
    # Combine all normalized data into a single DataFrame
    if normalized_data:
        final_df = pd.concat(normalized_data, ignore_index=True)
        
        # Save the DataFrame to the output file
        final_df.to_csv(output_file, index=False)
        print(f"Normalized data saved to {output_file}")
        return final_df
    else:
        print("No data to save.")
        return None

# Streamlit App Example
def streamlit_app():
    st.title("Transportation Order Data Extractor")
    
    st.write("Upload transportation order files (.pdf, .docx, .xlsx, .eml) to extract structured data")
    
    uploaded_files = st.file_uploader("Choose files", accept_multiple_files=True, 
                                     type=["pdf", "docx", "xlsx", "eml"])
    
    if uploaded_files and st.button("Process Files"):
        # Create temporary dataframe to store files
        data = []
        
        for file in uploaded_files:
            file_content = file.read()
            data.append({"file_name": file.name, "file_content": file_content})
        
        temp_df = pd.DataFrame(data)
        
        # Save temporary CSV
        temp_input = "temp_input.csv"
        temp_output = "temp_output.csv"
        temp_df.to_csv(temp_input, index=False)
        
        with st.spinner("Processing files..."):
            results_df = process_files(temp_input, temp_output)
            
        if results_df is not None:
            st.success("Processing complete!")
            st.dataframe(results_df)
            
            # Allow user to download results
            csv = results_df.to_csv(index=False)
            st.download_button(
                label="Download CSV Results",
                data=csv,
                file_name="transportation_orders_data.csv",
                mime="text/csv"
            )
            
            # Also offer JSON format
            json_data = results_df.to_json(orient="records")
            st.download_button(
                label="Download JSON Results",
                data=json_data,
                file_name="transportation_orders_data.json",
                mime="application/json"
            )
        else:
            st.error("Error processing files")
            
# If running directly, start the Streamlit app
if __name__ == "__main__":
    streamlit_app()