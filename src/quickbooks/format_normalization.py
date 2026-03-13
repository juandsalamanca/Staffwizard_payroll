import pandas as pd
from datetime import timedelta
from dateutil import parser
from src.llm_functions import get_total_columns

def get_all_features_in_one_column(input_df):
    for i in range(len(input_df)):
        feature = input_df.loc[i, "Unnamed: 3"]
        if pd.isna(feature):
            for j in range(3):
                replacement = input_df.loc[i, f"Unnamed: {j}"]
                if pd.isna(replacement):
                    continue
                else:
                    input_df.loc[i, "Unnamed: 3"] = replacement

    for j in range(len(input_df.columns)):
        if j < 3 or (j > 3 and j % 2 != 0):
            input_df = input_df.drop(columns=f"Unnamed: {j}")

    for i in range(len(input_df["Unnamed: 3"])):
        if input_df["Unnamed: 3"][i] == "Hourly":
            hourly_idx = i
        if input_df["Unnamed: 3"][i] == "Overtime (x1.5) hourly":
            overtime_idx = i
        if input_df["Unnamed: 3"][i] == "Total Gross Pay":
            gross_pay_idx = i
        if input_df["Unnamed: 3"][i] == "Adjusted Gross Pay":
            ad_gross_pay_idx = i
        if input_df["Unnamed: 3"][i] == "Net Pay":
            net_pay_idx = i

    return input_df

def turn_pay_triple_values_into_one_row_each(input_df):

    # First rename the rows and save the indexes
    for i in range(len(input_df["Unnamed: 3"])):
        if input_df["Unnamed: 3"][i] == "Hourly":
            hourly_idx = i
        if input_df["Unnamed: 3"][i] == "Overtime (x1.5) hourly":
            overtime_idx = i
        if input_df["Unnamed: 3"][i] == "Total Gross Pay":
            gross_pay_idx = i
        if input_df["Unnamed: 3"][i] == "Adjusted Gross Pay":
            ad_gross_pay_idx = i
        if input_df["Unnamed: 3"][i] == "Net Pay":
            net_pay_idx = i

    temp_df = input_df.drop(columns=["Unnamed: 3"]).copy()

    emp_hours = {}
    gross_packet = []
    gross_adjusted_packet = []
    net_packet = []
    r_packet = []
    o_packet = []
    emp_pay = {}
    date_list = {}

    for j, col in enumerate(temp_df.columns):
        # print(hourly_idx)
        # print(j)
        r_packet.append(temp_df.iloc[hourly_idx, j])
        o_packet.append(temp_df.iloc[overtime_idx, j])
        if "Unnamed" not in col:
            emp_name = col
        if temp_df.iloc[0, j] == "Hours" or "Week of" in temp_df.iloc[0, j]:
            gross_packet.append(temp_df.iloc[gross_pay_idx, j])
            gross_adjusted_packet.append(temp_df.iloc[ad_gross_pay_idx, j])
            net_packet.append(temp_df.iloc[net_pay_idx, j])

        if (j + 1) % 3 == 0:
            date_list[emp_name] = [temp_df.iloc[0, j]]
            emp_hours[emp_name] = {"regular": r_packet, "overtime": o_packet}
            emp_pay[emp_name] = {"gross": gross_packet, "adjusted_gross": gross_adjusted_packet, "net": net_packet}
            r_packet = []
            o_packet = []
            gross_packet = []
            gross_adjusted_packet = []
            net_packet = []

    normalized_hours = {
        "category": ["period_begin", "regular_hours", "regular_rate", "regular_pay", "overtime_hours", "overtime_rate",
                     "overtime_pay", "gross_hours", "gross_pay", "adjusted_gross_hours", "adjusted_gross_pay",
                     "net_hours", "net_pay"]}
    gross = {"category": ["gross_hours", "gross_pay"]}
    adjusted_gross = {"category": ["adjusted_gross_hours", "adjusted_gross_pay"]}
    net = {"category": ["net_hours", "net_pay"]}
    for emp in emp_hours:
        if len(emp_hours[emp]["regular"]) != 3 or len(emp_hours[emp]["overtime"]) != 3:
            print("Problem with :", emp)
        normalized_hours[emp] = date_list[emp] + emp_hours[emp]["regular"] + emp_hours[emp]["overtime"] + emp_pay[emp][
            "gross"] + emp_pay[emp]["adjusted_gross"] + emp_pay[emp]["net"]
        gross[emp] = emp_pay[emp]["gross"]
        adjusted_gross[emp] = emp_pay[emp]["adjusted_gross"]
        net[emp] = emp_pay[emp]["net"]

    normalized_hours_df = pd.DataFrame(normalized_hours)
    idx_list = [hourly_idx, overtime_idx, gross_pay_idx, ad_gross_pay_idx, net_pay_idx]
    return input_df, temp_df, normalized_hours_df, idx_list


def add_six_days_to_date_string(date_str):
    dt_obj = parser.parse(date_str)
    new_dt_obj = dt_obj + timedelta(days=6)
    return str(new_dt_obj)

def re_structure_columns(input_df, temp_df, normalized_hours_df, idx_list):

    # Re arange the columns to for the emp names to match the column of the week period
    temp_df_cols = temp_df.columns
    emp_group_list = []
    emp_group = []
    for i, col in enumerate(temp_df_cols):
        emp_group.append(col)
        if (i + 1) % 3 == 0:
            emp_name = emp_group[0]
            emp_group_list.extend([emp_group[2], emp_group[1], emp_group[0]])
            emp_group = []

    # Now use the emp groups as column and drop the unnamed columns which will match the ones that don't contain any values
    # These columns were only valuable for the features of triple values but those are taken care of above
    category_input_col = input_df["Unnamed: 3"]

    modified_temp_df = temp_df.copy()[1:]
    modified_temp_df.columns = emp_group_list
    for col in modified_temp_df.columns:
        if "Unnamed" in col:
            modified_temp_df = modified_temp_df.drop(columns=col)
    modified_temp_df.insert(0, "category", category_input_col)
    modified_temp_df = modified_temp_df.drop(idx_list)


    # Now we concatenate the re strucutured triple values with the regualr ones
    input_df = pd.concat([modified_temp_df, normalized_hours_df])
    new_idx = input_df["category"]
    new_idx.name = "employee_name"
    input_df.index = new_idx
    input_df = input_df.drop(columns="category")
    input_df = input_df.T
    input_df.insert(0, "employee_name", input_df.index.to_list())
    final_idx = range(len(input_df))

    input_df.index = final_idx
    input_df.columns.name = ''

    # Prepare date columns
    input_df["checkdate"] = input_df["period_begin"].apply(lambda x: x.replace("Week of", ""))
    input_df["period_begin"] = input_df["period_begin"].apply(lambda x: x.replace("Week of", ""))
    input_df["period_end"] = input_df["period_begin"].apply(add_six_days_to_date_string)

    # Drop useless columns:
    input_df = input_df.dropna(axis=1, how='all')
    # Drop the aggregate/totals columns to not throw off the LLM
    input_cols = input_df.columns.to_list()
    total_cols = get_total_columns(input_cols, "gpt-4o-mini")
    input_df = input_df.drop(columns=total_cols)

    return input_df

def preprocess_quickbooks(input_df):

    input_df = get_all_features_in_one_column(input_df)
    input_df, temp_df, normalized_hours_df, idx_list = turn_pay_triple_values_into_one_row_each(input_df)
    input_df = re_structure_columns(input_df, temp_df, normalized_hours_df, idx_list)
    return input_df