import os
import pandas as pd
import asyncio
import json
from dotenv import load_dotenv
from fastapi import FastAPI, Form, UploadFile, File
from fastapi.responses import FileResponse
from src.llm_functions import payroll_transformer
from src.preprocessing_functions import assert_is_date, preprocess_numeric_data, preprocess_input, preprocess_template
from src.check_data import build_check_data
from src.deduction_data import build_deduction_data
import zipfile
import aiofiles
import traceback

load_dotenv()

app = FastAPI()

@app.get("/health")
async def health():
    return {"message": "Server is OK"}

upload_dir = "./input_files"
output_dir = "./output_files"

async def save_output_csvs(check_data_df: pd.DataFrame, deduction_df: pd.DataFrame, tax_mappings_df: pd.DataFrame):
    check_data_path = output_dir + "/check_data.csv"
    deduction_data_path = output_dir + "/deduction_data.csv"
    tax_mappings_path = output_dir + "/tax_mappings.csv"
    await asyncio.gather(
        asyncio.to_thread(check_data_df.to_csv, check_data_path, index=False),
        asyncio.to_thread(deduction_df.to_csv, deduction_data_path, index=False),
        asyncio.to_thread(tax_mappings_df.to_csv, tax_mappings_path, index=False)
    )
    zip_path = output_dir + "/files.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(check_data_path, arcname="check_data.csv")
        z.write(deduction_data_path, arcname="deduction_data.csv")
        z.write(tax_mappings_path, arcname="tax_mappings.csv")
    return zip_path

@app.post("/process_payroll")
async def process_payroll(input_file: UploadFile=File(...)):

    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    try:
        # First we save the file for traceability
        file_path = os.path.join(upload_dir, input_file.filename)
        content = await input_file.read()

        # Use aiofiles to not block the event loop while writing
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(content)
        print("Input file read")

        confusing_cols = ["DetailType", "TaxType", "TaxCode", "TaxablePay", "PayAmt"]
        template_task = asyncio.to_thread(preprocess_template, confusing_cols)
        preprocess_task = asyncio.to_thread(preprocess_input, file_path)
        (check_col_desc, deduction_col_desc, tax_type_list, deduction_template_df), (input_df, input_cols, input_format) = await asyncio.gather(template_task, preprocess_task)
        print("Preprocessing done")
        # Create the appropriate column mappings for each output
        check_mapping_task = asyncio.create_task(payroll_transformer(check_col_desc, input_cols, "gpt-4o"))
        deduction_mapping_task = asyncio.create_task(payroll_transformer(deduction_col_desc, input_cols, "gpt-5.2"))
        check_mapping, deduction_mapping = await asyncio.gather(check_mapping_task, deduction_mapping_task)
        print("Column mapping done")
        check_mapping_json = json.loads(check_mapping)
        deduction_mapping_json = json.loads(deduction_mapping)
        if input_format == "rippling":
            if "California Employment Training Tax" in deduction_mapping_json["TaxDed"]:
                deduction_mapping_json["TaxDed"].remove("California Employment Training Tax")
            if "California Employment Training Tax" not in deduction_mapping_json["TaxLiab"]:
                deduction_mapping_json["TaxLiab"].append("California Employment Training Tax")
        for cat in confusing_cols:
            deduction_mapping_json[cat] = []
        deduction_mapping_json["PayAmt"] = deduction_mapping_json['RegPay']
        print("JSON mappings done")

        # Build the Check data spreadsheet
        check_data_task = asyncio.to_thread(build_check_data, input_df, check_mapping_json)

        # Build the deduction data spreadsheet
        deduction_data_task = asyncio.create_task(build_deduction_data(input_df, deduction_mapping_json, check_mapping_json, deduction_template_df))

        check_data_df, (deduction_data_df, tax_mappings_df) = await asyncio.gather(check_data_task, deduction_data_task)
        print("Output Tables done")
        zip_path = await save_output_csvs(check_data_df, deduction_data_df, tax_mappings_df)

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename="results.zip",
        )
    except Exception as e:
        traceback.print_exc()
        return {"Error": str(e)}













