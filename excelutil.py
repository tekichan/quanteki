'''
Utility module of Excel operation
'''
# modules for Data Science
import pandas as pd

# modules for handling files
import base64
import io

### Constant Values ###
LABEL_DATAFRAME = 'dataframe'
LABEL_SHEETNAME = 'sheet_name'
LABEL_SAVE_INDEX = 'save_index'

# Convert DataFrame to Excel in Base64 encoding
def save_excel_base64(
    df
    , sheetname
    , save_index=True
):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=save_index)
        writer.save()
        xlsx_data = output.getvalue()
    b64 = base64.b64encode(xlsx_data)
    return b64.decode('utf-8')

# Convert DataFrame to Excel in File
def save_excel_file(
    df
    , filepath
    , sheetname
    , save_index=True
):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=save_index)
        writer.save()

# Convert multiple DataFrame to Excel in Base64 encoding
def save_multi_worksheet_base64(
    list_df = []
):
    '''
    This function is to create a downloadable link of an Excel content 
    and display the link in Jupyter notebook.
    Parameters
    ----------
    list_df : List of Dict with Pandas DataFrame as well as Worksheet Name (LABEL_DATAFRAME, LABEL_SHEETNAME, LABEL_SAVE_INDEX)
         Data Content of the Excel
    Returns
    -------
    String in Base 64 which represents the created Excel object
    '''
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for dict_df in list_df:
            obj_df = dict_df[LABEL_DATAFRAME]
            sheet_name = dict_df[LABEL_SHEETNAME]
            save_index = (LABEL_SAVE_INDEX in dict_df) and dict_df[LABEL_SAVE_INDEX]
            obj_df.to_excel(writer, encoding='utf-8', sheet_name=sheet_name, index=save_index)
        writer.save()
        xlsx_data = output.getvalue()
    b64 = base64.b64encode(xlsx_data)
    return b64.decode('utf-8')

# Convert multiple DataFrame to Excel in File
def save_multi_worksheet_file(
    list_df = []
    , filepath
):
    '''
    This function is to create a downloadable link of an Excel content 
    and display the link in Jupyter notebook.
    Parameters
    ----------
    list_df : List of Dict with Pandas DataFrame as well as Worksheet Name (LABEL_DATAFRAME, LABEL_SHEETNAME, LABEL_SAVE_INDEX)
         Data Content of the Excel
    filepath: Path of saved Excel file
    '''
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        for dict_df in list_df:
            obj_df = dict_df[LABEL_DATAFRAME]
            sheet_name = dict_df[LABEL_SHEETNAME]
            save_index = (LABEL_SAVE_INDEX in dict_df) and dict_df[LABEL_SAVE_INDEX]
            obj_df.to_excel(writer, encoding='utf-8', sheet_name=sheet_name, index=save_index)
        writer.save()
