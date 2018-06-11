'''
Utility module of Excel operation
'''
# modules for Data Science
import pandas as pd

# modules for handling files
import base64
import io

# Convert DataFrame to Excel in Base64 encoding
def save_excel_base64(
    df
    , sheetname):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=True)
        writer.save()
        xlsx_data = output.getvalue()
    b64 = base64.b64encode(xlsx_data)
    return b64.decode('utf-8')

# Convert DataFrame to Excel in File
def save_excel_file(
    df
    , filepath
    , sheetname):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=True)
        writer.save()