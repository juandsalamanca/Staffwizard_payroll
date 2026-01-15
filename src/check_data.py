import pandas as pd
from dateutil import parser

def get_correct_date_format(date_string):
  dt = parser.parse(date_string)
  return dt.strftime('%Y-%m-%d')

def build_check_data(input_df, check_mapping_json):
    output_json = {}

    for i, row in enumerate(input_df.iterrows()):
        row = row[1]
        checknum = row["CHECK/VOUCHER NUMBER"]
        # We don't count the rows that contain subtotals in order to not overcount
        # The rows corresponding to subtotals, had NaN as Checknum. Now they have a 0
        if checknum != 0.0:
            if checknum not in output_json:
                output_json[checknum] = {}
            for col in check_mapping_json:
                if col not in output_json[checknum]:
                    output_json[checknum][col] = []
                input_col = check_mapping_json[col]
                if input_col:
                    output_json[checknum][col].append(row[input_col[0]])
                else:
                    output_json[checknum][col].append("None")

    for checknum in output_json:
        for col in check_mapping_json:
            if col in ["GrossPay", "NetPay"]:
                output_json[checknum][col] = sum(output_json[checknum][col])
            else:
                output_json[checknum][col] = output_json[checknum][col][0]

    final_output = {}
    for checknum in output_json:
        # final_output["CHECK/VOUCHER NUMBER"].append(checknum)
        for col in output_json[checknum]:
            if col not in final_output:
                final_output[col] = []
            # Since the columns go in th order first, middle and last name we can define all of them when we get to the first name column
            # We later set the middle and last names when we arrive to their respective columns
            if col == "FirstName":
                spl = output_json[checknum][col].split(",")
                last_name = spl[0]
                name_spl = spl[1].strip().split(" ")
                first_name = name_spl[0]
                # If there is a middle name, we save it
                if len(name_spl) > 1:
                    middle_name = name_spl[1]
                else:
                    middle_name = "None"
                final_output[col].append(first_name)
            elif col == "MiddleName":
                final_output[col].append(middle_name)
            elif col == "LastName":
                final_output[col].append(last_name)
            else:
                final_output[col].append(output_json[checknum][col])

    check_data_df = pd.DataFrame(final_output)
    # Final cleaning and formatting:
    check_data_df["CheckDate"] = check_data_df["CheckDate"].apply(get_correct_date_format)
    check_data_df["PeriodBeginDate"] = check_data_df["PeriodBeginDate"].apply(get_correct_date_format)
    check_data_df["PeriodEndDate"] = check_data_df["PeriodEndDate"].apply(get_correct_date_format)
    for i in range(len(check_data_df["SSN"])):
        check_data_df.loc[i, "SSN"] = str(check_data_df.loc[i, "SSN"]).replace("-", "")

    return check_data_df