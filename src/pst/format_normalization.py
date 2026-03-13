def preprocess_pst(pst_df):
    cols = pst_df.loc[8].to_list()
    input_df = pst_df[9:].copy().reset_index(drop=True)
    input_df.columns = cols
    return input_df