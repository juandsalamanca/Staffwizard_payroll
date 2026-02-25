import pandas as pd
from dateutil import parser

def normalize_data_packets(white_glove_df):
    input_cols = ['Pay_date', 'Pay_period'] + white_glove_df.loc[5].to_list()
    input_packets = []
    pay_date = None
    for i in range(len(white_glove_df)):

        # The pay date and pay period rows only have that information and nothing else
        if white_glove_df.loc[i, "Payroll journal report"] == "Pay date":
            pay_date = white_glove_df.loc[i, "Unnamed: 1"]

        elif i > 0 and white_glove_df.loc[i - 1, "Payroll journal report"] == "Pay date":
            pay_period = white_glove_df.loc[i, "Unnamed: 1"]

        # This marks the end of the individual paychek information
        elif "Total Employee Earnings" in white_glove_df.loc[i, "Payroll journal report"]:
            break

        # We avoid rows where the data hasnt started or the ones that are just column names
        elif pay_date is None or white_glove_df.loc[i, "Payroll journal report"] == "Name":
            continue

        # The rest of the rows are filled with valuable data so we get the whole row
        else:
            row = [pay_date, pay_period] + white_glove_df.loc[i].to_list()
            input_packets.append(row)

    input_df = pd.DataFrame(input_packets, columns=input_cols)
    input_df = input_df.fillna(0)
    input_df["CHECK/VOUCHER NUMBER"] = [i + 1 for i in range(len(input_df))]
    # Drop columns that confuse LLM and represent total amounts
    total_employer_taxes = input_df["Total Employer Taxes"].to_list()
    total_employee_taxes = input_df["Total Employee Taxes"].to_list()
    input_df = input_df.drop(
        columns=["Total Employee Taxes", "Total Employer Taxes", "Total Company Payable Taxes", "Total Taxes"])

    return input_df, total_employer_taxes, total_employee_taxes


def normalize_pay_periods(input_df):
    period_begin_date_list = []
    period_end_date_list = []

    quarter_date_mapping = {"Q1": "Jan 1st - Mar 31st",
                            "Q2": "Apr 1st - Jun 30th",
                            "Q3": "Jul 1st - Sep 30th",
                            "Q4": "Oct 1st - Dec 31st"}

    for i in range(len(input_df)):
        pay_date = input_df.loc[i, "Pay_date"]
        dt = parser.parse(pay_date)
        dt.strftime('%Y-%m-%d')
        year = dt.year
        pay_period = input_df.loc[i, "Pay_period"]
        if "Q" in pay_period:
            for word in pay_period.split():
                if "Q" in word:
                    quarter = word
                    break
            pay_period = quarter_date_mapping[quarter]
        if "Extra hours" in pay_period:
            n = 1
            while "Extra hours" in pay_period:
                pay_period = input_df.loc[i - n, "Pay_period"]
                n += 1
        if "Bulk New Hire Payroll" in pay_period:
            pay_period_begin = pay_period.split()[-3]
            pay_period_end = pay_period.split()[-1]
            period_begin_date_list.append(pay_period_begin)
            period_end_date_list.append(pay_period_end)
            continue

        pay_spl = pay_period.split("-")
        pay_beg_str = pay_spl[0] + f", {year}"
        pay_period_begin = parser.parse(pay_beg_str).strftime('%Y-%m-%d')
        pay_end_str = pay_spl[1] + f", {year}"
        pay_period_end = parser.parse(pay_end_str).strftime('%Y-%m-%d')
        period_begin_date_list.append(pay_period_begin)
        period_end_date_list.append(pay_period_end)

    input_df["PeriodBeginDate"] = period_begin_date_list
    input_df["PeriodEndDate"] = period_end_date_list
    input_df = input_df.drop(columns=["Pay_period"])

    return input_df