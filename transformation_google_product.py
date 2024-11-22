import pandas as pd

# automic
def flattenDataframe(df):
    def flatten_list_column(df, col):

        # Convert list-like values into separate rows.

        return df.explode(col).reset_index(drop=True)

    def flatten_dict_column(df, col):

        # Normalize a dictionary-like column into separate columns.

        dict_df = pd.json_normalize(df[col])

        # Rename the new columns to indicate their origin

        dict_df.columns = [f"{col}_{subcol}" for subcol in dict_df.columns]

        return df.drop(col, axis=1).join(dict_df)

    # Continue flattening until there are no list-like or dictionary-like columns left
    while any(df.apply(lambda x: isinstance(x, (list, dict))).any()):

        for col in df.columns:

            # Check if the column contains list-like structures and flatten if necessary

            if df[col].apply(lambda x: isinstance(x, list)).any():

                df = flatten_list_column(df, col)

            # Check if the column contains dictionary-like structures and flatten if necessary

            if df[col].apply(lambda x: isinstance(x, dict)).any():

                df = flatten_dict_column(df, col)
    return df


