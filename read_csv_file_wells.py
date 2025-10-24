import pandas as pd


def rename_columns(input_df):
    df = input_df.copy()
    column_mapping = {
        "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
        "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
        "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
        "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
        "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "AVG_RESERVOIR_PRESSURE(PSIA)",
    }
    return df.rename(columns=column_mapping)


def flatten_df_columns(input_df):
    df = input_df.copy()
    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_")
        for col in df.columns.values
    ]
    # Remove trailing empty columns
    df = df.loc[:, df.columns != ""]
    return df


def gen_datetime_column(input_df, drop_columns=["DATE", "Time"]):
    df = input_df.copy()
    dates = pd.to_datetime(df["DATE"])
    origin = dates.min().normalize()
    dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")
    df.insert(0, "DateTime", dt)
    df = df.drop(columns=drop_columns)
    df.set_index("DateTime", inplace=True)
    return df


def gen_df_monthly_rates(input_df, n_decimals=2):
    df = input_df.copy()
    df_monthly_cum = df.resample("ME").last()
    print(df_monthly_cum.head())
    df_monthly_prod = df_monthly_cum.diff().dropna()
    print(df_monthly_prod.head())
    days_in_month = df_monthly_prod.index.days_in_month
    df_mr = pd.DataFrame(index=df_monthly_prod.index)
    for col in df.columns:
        if "CUML" in col:
            df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
        elif "PRESSURE" in col:
            df_mr[col] = round(df[col].resample("ME").mean(), n_decimals)
    return df_mr


def manipulate_df_monthly_index_and_offset(input_df, months_offset=0):
    df = input_df.copy()
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=months_offset)
    return df


# Configuration
sim_parent_folder = "D:/temp/0_sim_models/0_BasicModelFromIXF"
afi_file_name = "a_base_model"
csv_folder_name = f"{sim_parent_folder}/{afi_file_name}_summary_results/"
field_csv_file_name = f"{afi_file_name}_FIELD.CSV"
# wells_csv_file_name = f"{afi_file_name}_WELLS.CSV"


# Read CSV with multi-level headers
df_field = pd.read_csv(f"{csv_folder_name}/{field_csv_file_name}", header=[0, 1, 2])


# Flatten column names
df_field = flatten_df_columns(df_field)


df_field = gen_datetime_column(
    df_field, drop_columns=["DATE", "Time", "Entity Type", "Entity Name"]
)

write_to_csv_interim = False
if write_to_csv_interim:
    df_field.to_csv(f"{csv_folder_name}/{afi_file_name}_FIELD_PRC.csv", index=True)


df_monthly_rates = gen_df_monthly_rates(df_field)
df_monthly_rates = rename_columns(df_monthly_rates)
df_monthly_rates = manipulate_df_monthly_index_and_offset(df_monthly_rates)

df_monthly_rates.to_csv(f"{csv_folder_name}/{afi_file_name}_FIELD_MR.csv", index=True)

# for entity_name in df["Entity Name"].unique():
#     df_1 = df.loc[df["Entity Name"] == entity_name, :]
#     dates = pd.to_datetime(df["DATE"])
#     origin = dates.min().normalize()
#     dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")
#     df_1.insert(0, "DateTime", dt)
#     df_1 = df_1.drop(columns=["DATE", "Time"])
#     df_1.set_index("DateTime", inplace=True)
#     # print(df_1.head())
#     # df_1.fillna(0, inplace=True)

#     monthly_cum = df_1.resample("ME").last()
#     monthly_prod = monthly_cum.diff().dropna()
#     days_in_month = monthly_prod.index.days_in_month

#     # Create monthly rates dataframe
#     monthly_rates = pd.DataFrame(index=monthly_prod.index)

#     for col in df_1.columns:
#         if "CUML" in col:
#             monthly_rates[col] = monthly_prod[col] / days_in_month
#         elif "PRESSURE" in col:
#             monthly_rates[col] = df_1[col].resample("ME").mean()

# print(monthly_prod)

# # Create monthly rates dataframe

# monthly_rates = rename_columns(monthly_rates)
# print(monthly_rates)


# df.to_csv(f"{input_folder}/df_x.csv", index=False)


# dates = pd.to_datetime(df["DATE"], format="%d-%b-%y")
# origin = dates.min().normalize()
# dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")

# df.insert(0, "DateTime", dt)
# # df = df.drop(columns=["DATE", "Time"])

# print(df)

# # Insert DateTime column and drop unnecessary columns
# df.insert(0, "DateTime", dt)
# df = df.drop(
#     columns=[date_col, time_col, ("Entity Type", "", ""), ("Entity Name", "", "")],
#     errors="ignore",
# )


# # Flatten column names with units
# def flatten_column(column_tuple):
#     l0, l1, l2 = column_tuple
#     base = "_".join([p for p in (l0, l1) if p])
#     return f"{base} ({l2})" if l2 else base


# df.columns = ["DateTime"] + [flatten_column(c) for c in df.columns[1:]]
# df = df.set_index("DateTime")

# # Convert to monthly rates
# monthly_cum = df.resample("ME").last()
# monthly_prod = monthly_cum.diff().dropna()
# days_in_month = monthly_prod.index.days_in_month

# # Create monthly rates dataframe
# monthly_rates = pd.DataFrame(index=monthly_prod.index)

# for col in df.columns:
#     if "CUML" in col:
#         monthly_rates[col] = monthly_prod[col] / days_in_month
#     elif "PRESSURE" in col:
#         monthly_rates[col] = df[col].resample("ME").mean()

# # Rename columns to descriptive names
# column_mapping = {
#     "FWIT_WATER_INJECTION_CUML (STB)": "FIELD_WATER_INJECTION_RATE (STB/DAY)",
#     "FGIT_GAS_INJECTION_CUML (MSCF)": "FIELD_GAS_INJECTION_RATE (MSCF/DAY)",
#     "FWPT_WATER_PRODUCTION_CUML (STB)": "FIELD_WATER_PRODUCTION_RATE (STB/DAY)",
#     "FGPT_GAS_PRODUCTION_CUML (MSCF)": "FIELD_GAS_PRODUCTION_RATE (MSCF/DAY)",
#     "FOPT_OIL_PRODUCTION_CUML (STB)": "FIELD_OIL_PRODUCTION_RATE (STB/DAY)",
#     "FPR_PRESSURE (PSIA)": "AVG_RESERVOIR_PRESSURE (PSIA)",
# }
# monthly_rates.rename(columns=column_mapping, inplace=True)

# # Format output
# monthly_rates = monthly_rates.round(2)
# monthly_rates.index = monthly_rates.index.to_period("M").to_timestamp()

# # Save files
# monthly_output = os.path.join(input_folder, "Model_From_IXF_FIELD_monthly_rates.csv")
# processed_output = os.path.join(input_folder, "Model_From_IXF_FIELD_processed.csv")

# monthly_rates.to_csv(monthly_output, index=True)
# df.to_csv(processed_output, index=True)

# print(f"Monthly rates saved to: {monthly_output}")
# print(f"Processed data saved to: {processed_output}")
# print("\nMonthly rates preview:")
# print(monthly_rates.head())


# # D:\temp\0_sim_models\0_BasicModelFromIXF\base_model_ixf_summary_results
