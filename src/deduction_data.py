import pandas as pd
import asyncio
from src.check_data import get_correct_date_format
from src.tax_functions import load_tax_listings, get_location_from_input_file, get_tax_codes, aggregate_employee_employer_taxes

def aggregate_check_data(input_df):
    # We consider only the rows that have a checknumber to discrad the subtotals and not overcount
    input_cols = input_df.columns
    aggregated_input = {}
    for i in range(len(input_df)):
        checknum = input_df.loc[i, "CHECK/VOUCHER NUMBER"]
        if checknum != 0.0:
            if checknum not in aggregated_input:
                aggregated_input[checknum] = {}
            for col in input_cols:
                if col not in aggregated_input[checknum]:
                    aggregated_input[checknum][col] = []

                aggregated_input[checknum][col].append(input_df.loc[i, col])

    # For the same paycheck/checknum we aggregate the numerical ammounts
    f_aggregated_input = {}
    for checknum in aggregated_input:
        summable = False
        for col in aggregated_input[checknum]:
            if col not in f_aggregated_input:
                f_aggregated_input[col] = []
            if col == "GROSS PAY":
                summable = True
            if summable:

                f_aggregated_input[col].append(sum(aggregated_input[checknum][col]))
            else:
                f_aggregated_input[col].append(aggregated_input[checknum][col][0])

    aggregated_input_df = pd.DataFrame(f_aggregated_input)
    return aggregated_input_df

def get_one_row_per_earning_or_deduction(aggregated_input_df, deduction_mapping_json, check_mapping_json):
    deduction_mapping_json["EmployeeId"] = check_mapping_json["EmployeeId"]
    output_json = {}
    for col in deduction_mapping_json:
        # This one is a made up column to make it easier to have one row per earning but does not go in the output file
        if col == "Earnings":
            continue
        output_json[col] = []
    # Keep track of each deduction name:
    ded_len = len(deduction_mapping_json["TaxDed"])
    liab_len = len(deduction_mapping_json["TaxLiab"])
    earning_len = len(deduction_mapping_json["Earnings"])
    edm_len = ded_len + liab_len + earning_len
    for i, row in enumerate(aggregated_input_df.iterrows()):
        row = row[1]
        checknum = row["CHECK/VOUCHER NUMBER"]
        for col in deduction_mapping_json:
            input_col = deduction_mapping_json[col]
            if input_col:
                # We need one row per deduction, both taxded and taxliab
                if col == "TaxDed":
                    if "TaxLiab" not in output_json:
                        output_json["TaxLiab"] = []
                    for subcol in input_col:
                        if pd.isna(row[subcol]) == False:
                            output_json[col].append(row[subcol])
                        else:
                            output_json[col].append(0)
                        # For the non zero tax ded, the tax liab is zero
                        output_json["TaxLiab"].append(0)
                        output_json["TaxType"].append(subcol)
                        output_json["DetailType"].append("Tax")
                        output_json["PayAmt"].append(row[deduction_mapping_json["RegPay"][0]])
                elif col == "TaxLiab":
                    for subcol in input_col:
                        if pd.isna(row[subcol]) == False:
                            output_json[col].append(row[subcol])
                        else:
                            output_json[col].append(0)
                        # For the non zero tax liab, the tax ded is zero
                        output_json["TaxDed"].append(0)
                        output_json["TaxType"].append(subcol)
                        output_json["DetailType"].append("Tax")
                        output_json["PayAmt"].append(row[deduction_mapping_json["RegPay"][0]])
                elif col == "Earnings":
                    for subcol in input_col:
                        if pd.isna(row[subcol]) == False:
                            value = row[subcol]
                        else:
                            value = 0
                        if subcol == "REGULAR EARNINGS":
                            value = 0
                        output_json["PayAmt"].append(value)
                        # For the cols mapped to PayAmt, the taxes will be 0 since they are earnings, and the detail type will be the name of each subcolumn
                        output_json["TaxDed"].append(0)
                        output_json["TaxLiab"].append(0)
                        output_json["TaxType"].append("")
                        if "ADDITIONAL" in subcol:
                            subcol = subcol.replace("ADDITIONAL EARNINGS  : ", "")
                        output_json["DetailType"].append(subcol)
                elif col == "TaxType":
                    continue
                elif col == "DetailType":
                    continue
                elif col == "PayAmt":
                    continue
                else:
                    # For all other columns we insert same value from the first mapped column the number of times equivalent to the sum of the subcategories of Ded and Liab (edm_len)
                    output_json[col].extend([row[input_col[0]]] * edm_len)

                # This way we insert tax_len values in the output rows for each row of the input

            else:
                output_json[col].extend(["None"] * edm_len)
    deduction_df = pd.DataFrame(output_json)
    return deduction_df

async def build_deduction_data(input_df, deduction_mapping_json, check_mapping_json, deduction_template_df):
    tax_listing_task = asyncio.to_thread(load_tax_listings)
    aggregated_input_df = await asyncio.to_thread(aggregate_check_data,input_df)
    deduction_df = await asyncio.to_thread(get_one_row_per_earning_or_deduction,aggregated_input_df, deduction_mapping_json, check_mapping_json)
    state_tax_df, local_tax_df = await tax_listing_task
    processed_deduction_df = await asyncio.to_thread(get_tax_codes, deduction_df, aggregated_input_df, deduction_template_df, state_tax_df, local_tax_df)
    processed_deduction_df = await asyncio.to_thread(aggregate_employee_employer_taxes, processed_deduction_df)
    return processed_deduction_df

