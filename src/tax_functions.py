import pandas as pd
import time
import json
from src.llm_functions import get_state_code, map_tax_types, get_correct_tax_name, get_correct_state_tax_code, detect_state_and_local_columns

def load_tax_listings():
    state_tax_df = pd.read_excel("./static_data/swpe_sui_tax_listing.xlsx")
    local_tax_df = pd.read_excel("./static_data/swpe_local_tax_listing.xlsx")
    return state_tax_df, local_tax_df

def get_location_from_input_file(aggregated_input_df, checknum, location_col, input_checknum_col):
  if location_col:
    filterer_input_df = aggregated_input_df.loc[aggregated_input_df[input_checknum_col]==checknum].reset_index(drop=True)
    location = filterer_input_df.loc[0, location_col]
    return location
  else:
    return None

def get_tax_codes(deduction_df, aggregated_input_df, deduction_template_df, state_tax_df, local_tax_df, deduction_mapping_json):
    #samples = 30
    #deduction_df = deduction_df[:samples]
    tax_types = deduction_template_df.loc[7, "Enumerated/Acceptable Values"]
    tax_type_list = tax_types.split("\n")
    input_cols = aggregated_input_df.columns.to_list()
    tax_mappings = {"input_tax": [], "tax_type": [], "tax_code": []}
    gpt_json = json.loads(detect_state_and_local_columns(input_cols, "gpt-4o-mini"))
    state_col = gpt_json["state_column"]
    local_col = gpt_json["city_county_column"]
    input_checknum_col = deduction_mapping_json["CheckNum"][0]
    tax_code_list = """SIT (State Income Tax)
      SDI (Employee SDI Tax)
      ER_SDI (Employer SDI Tax)
      FLI (Employee FLI Tax)
      ER_FLI (Employer FLI Tax)"""
    for i in range(len(deduction_df)):
        checknum = deduction_df.loc[i, "CheckNum"]
        state = get_location_from_input_file(aggregated_input_df, checknum, state_col, input_checknum_col)
        local = get_location_from_input_file(aggregated_input_df, checknum, local_col, input_checknum_col)
        state_code = get_state_code(state)
        input_tax_type = deduction_df.loc[i, "TaxType"]
        if input_tax_type:
            correct_tax_type = map_tax_types(input_tax_type, tax_type_list)

            time.sleep(1)
            # For the SUI taxes we used the SUI df, filter by state and map one of the remaining options
            if correct_tax_type == "SU (SUI)":
                cropped_state_tax_df = state_tax_df.loc[state_tax_df["State"] == state_code]
                state_tax_name_list = cropped_state_tax_df["Name"].to_list()
                correct_tax_name = get_correct_tax_name(input_tax_type, state_tax_name_list, local)
                if correct_tax_name != "None":
                    idx = state_tax_name_list.index(correct_tax_name)
                    tax_code = cropped_state_tax_df["Tax_ID"].to_list()[idx]
                else:
                    tax_code = "**NO CODE**"
            # For the local taxes we use the local tax df, filter by state and map one of the remaining options
            elif correct_tax_type == "CT (Local Tax)":
                cropped_local_tax_df = local_tax_df.loc[local_tax_df["State"] == state_code]
                local_tax_name_list = cropped_local_tax_df["Name"].to_list()
                correct_tax_name = get_correct_tax_name(input_tax_type, local_tax_name_list, local)
                if correct_tax_name != "None":
                    idx = local_tax_name_list.index(correct_tax_name)
                    tax_code = cropped_local_tax_df["Symmetry_Tax_Id"].to_list()[idx]
                else:
                    tax_code = "**NO CODE**"
            # For the rest of the taxes we use the instruction provided in the specs
            elif correct_tax_type == "ST (State Taxes)":
                spec_tax_code = get_correct_state_tax_code(input_tax_type, tax_code_list)
                tax_code = state_code + spec_tax_code
            # Federal taxes have implicit code, we leave it blank
            else:
                tax_code = ""
            deduction_df.loc[i, "TaxCode"] = tax_code
            deduction_df.loc[i, "TaxType"] = correct_tax_type
            tax_mappings["input_tax"].append(input_tax_type)
            tax_mappings["tax_type"].append(correct_tax_type)
            tax_mappings["tax_code"].append(tax_code)
        else:
            deduction_df.loc[i, "TaxCode"] = ""
            deduction_df.loc[i, "TaxType"] = ""

    tax_mappings_df = pd.DataFrame(tax_mappings)
    tax_mappings_df.to_csv("./output_files/tax_mappings.csv")
    return deduction_df

def aggregate_employee_employer_taxes(processed_deduction_df):
    current_checknum = ""
    tax_type_list = []
    idx_list = []
    for i in processed_deduction_df.index:
        checknum = processed_deduction_df.loc[i, "CheckNum"]
        if checknum != current_checknum:
            current_checknum = checknum
            tax_type_list = []
            idx_list = []
        tax_type = processed_deduction_df.loc[i, "TaxType"]
        tax_code = processed_deduction_df.loc[i, "TaxCode"]
        tax_ded = processed_deduction_df.loc[i, "TaxDed"]
        tax_liab = processed_deduction_df.loc[i, "TaxLiab"]
        # If we get same tax type for a single checknum we look into it
        if tax_type not in tax_type_list:
            tax_type_list.append(tax_type)
            idx_list.append(i)
        else:
            #print("Duplicate:", tax_type)
            # If the tax is federal (no tax code) we aggregate taxliab and taxded
            if tax_code == "":
                idx = tax_type_list.index(tax_type)
                idx = idx_list[idx]
                total_tax_ded = tax_ded + processed_deduction_df.loc[idx, "TaxDed"]
                total_tax_liab = tax_liab + processed_deduction_df.loc[idx, "TaxLiab"]
                processed_deduction_df.loc[idx, "TaxDed"] = total_tax_ded
                processed_deduction_df.loc[idx, "TaxLiab"] = total_tax_liab
                processed_deduction_df = processed_deduction_df.drop([i])

    return processed_deduction_df
