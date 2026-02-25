import pandas as pd
from src.rippling.format_normalization import normalize_data_packets, normalize_pay_periods

def assert_is_date(date_string):
    try:
        spl1 = date_string.split("/")
        spl2 = date_string.split("-")
        spl3 = date_string.split(".")
        spl4 = date_string.split("_")
        condition1 = len(spl1) == 3 or len(spl2) == 3 or len(spl3) == 3 or len(spl4) == 3
        condition2 = (any(c.isdigit() for c in spl1) or
                      any(c.isdigit() for c in spl2) or
                      any(c.isdigit() for c in spl3) or
                      any(c.isdigit() for c in spl4))
        if condition1 and condition2:
            return True
        else:
            return False
    except:
        return False

# Function to run through all the columns
# If the column has a numeric value we turn it into a clean float, if not we leave it alone
def preprocess_numeric_data(num):
    type_num = type(num)
    if type_num == float or type_num == int:
        return num
    elif type_num == str:
        num = num.replace("$", "")
        is_date = assert_is_date(num)
        # Any letters would indicate its not a numeric value
        has_letters = any(c.isalpha() for c in num)
        if has_letters or is_date:
            return num
        else:
            try:
                spl = num.split(",")
                num = "".join(spl)
                return float(num)
            except:
                return 0.0

    else:
        return 0.0

def preprocess_template(confusing_cols):

    template_path = "./static_data/Payroll Import Template.xlsx"

    check_template_df = pd.read_excel(template_path, sheet_name="Checks Legend")
    # Build the columns description string:
    check_col_desc = ""
    for i in range(len(check_template_df)):
        col = check_template_df.loc[i, "Column"]
        desc = check_template_df.loc[i, "Description"]
        if col == "SSN":
            add_info = "Also known as Tax ID"
        else:
            add_info = ""
        check_col_desc += f"{col}: {desc} {add_info}\n"

    # Columns that confuse the LLM and throw off the tax mappings

    deduction_template_df = pd.read_excel(template_path, sheet_name="EarningsDeductions Legend")
    # Build the columns description string:
    deduction_col_desc = ""
    for i in range(len(deduction_template_df)):
        col = deduction_template_df.loc[i, "Column"]
        desc = deduction_template_df.loc[i, "Description"]
        if col in confusing_cols:
            continue
        if col == "TaxDed":
            desc = "Any Tax deduction paid by the EMPLOYEE"
        elif col == "TaxLiab":
            desc = "Any Tax deduction paid by the EMPLOYER"
        deduction_col_desc += f"{col}: {desc}\n"

    # We'll add an extra one called earnings just so GPT can map it to all the different kinds of earnings
    # We need this to easily build an output with one row per earning even though we won't actually have this column in the output
    deduction_col_desc += "Earnings: Any type of earnings for the employee\n"

    tax_types = deduction_template_df.loc[7, "Enumerated/Acceptable Values"]
    tax_type_list = tax_types.split("\n")

    return check_col_desc, deduction_col_desc, tax_type_list, deduction_template_df

def detect_input_format(input_df):

    try:
        input_df, total_employer_taxes, total_employee_taxes = normalize_data_packets(input_df)
        input_df = normalize_pay_periods(input_df)
        input_format = "rippling"
    except Exception as e:
        print(e)
        input_format = "ADP"

    print("Input format:", input_format)

    return input_format, input_df

def preprocess_input(file_path):

    if file_path.endswith(".xlsx"):
        input_df = pd.read_excel(file_path)
    elif file_path.endswith(".csv"):
        input_df = pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        input_df = pd.read_json(file_path)
    else:
        raise Exception("Invalid file format")

    input_format, input_df = detect_input_format(input_df)
    input_cols = input_df.columns.to_list()
    input_df = input_df.fillna(0)
    for col in input_df.columns:
        input_df[col] = input_df[col].apply(preprocess_numeric_data)
    return input_df, input_cols, input_format