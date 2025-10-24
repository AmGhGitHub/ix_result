import pandas as pd


def rename_columns(input_df):
    df = input_df.copy()
    column_mapping = {
        "WGIT_GAS_INJECTION_CUML_MSCF": "WELL_GAS_INJECTION_RATE(MSCF/DAY)",
        "WWPT_WATER_PRODUCTION_CUML_STB": "WELL_WATER_PRODUCTION_RATE(STB/DAY)",
        "WWIT_WATER_INJECTION_CUML_STB": "WELL_WATER_INJECTION_RATE(STB/DAY)",
        "WGPT_GAS_PRODUCTION_CUML_MSCF": "WELL_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "WOPT_OIL_PRODUCTION_CUML_STB": "WELL_OIL_PRODUCTION_RATE(STB/DAY)",
        "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "WELL_BOTTOM_HOLE_PRESSURE(PSIA)",
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


def gen_dt(input_df):
    df = input_df.copy()
    df_ws = pd.DataFrame()
    for name in df["Entity Name"].unique():
        df_w = df.loc[df["Entity Name"] == name, :]
        dates = pd.to_datetime(df_w["DATE"])
        origin = dates.min().normalize()
        dt = origin + pd.to_timedelta(df_w["Time"].astype(float), unit="D")
        df_w.insert(0, "DateTime", dt)
        df_w = df_w.drop(columns=["DATE", "Time", "Entity Type"])
        df_w.set_index("DateTime", inplace=True)
        df_ws = pd.concat([df_ws, df_w])
    return df_ws


def gen_df_monthly_rates(input_df, n_decimals=2):
    df = input_df.copy()
    df_ws = pd.DataFrame()

    for name in df["Entity Name"].unique():
        df_w = df.loc[df["Entity Name"] == name, :]
        df_w = df_w.drop(columns=["Entity Name"])
        df_monthly_cum = df_w.resample("ME").last()
        df_monthly_prod = df_monthly_cum.diff().dropna()
        days_in_month = df_monthly_prod.index.days_in_month
        df_mr = pd.DataFrame(index=df_monthly_prod.index)
        for col in df_w.columns:
            if "CUML" in col:
                df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
            elif "PRESSURE" in col:
                df_mr[col] = round(df_w[col].resample("ME").mean(), n_decimals)
        df_mr.insert(0, "WELL_NAME", name)
        df_ws = pd.concat([df_ws, df_mr])
    return df_ws


def manipulate_df_monthly_index_and_offset(input_df):
    df = input_df.copy()
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=1)
    df.index = df.index + pd.offsets.MonthEnd(1)
    return df


# Configuration
sim_parent_folder = "D:/temp/0_sim_models/0_BasicModelFromIXF"
afi_file_name = "a_base_model"
csv_folder_name = f"{sim_parent_folder}/{afi_file_name}_summary_results"
# field_csv_file_name = f"{afi_file_name}_FIELD.CSV"
wells_csv_file_name = f"{afi_file_name}_WELL.CSV"
wells_csv_file_path = f"{csv_folder_name}/{wells_csv_file_name}"


# Read CSV with multi-level headers
df_wells = pd.read_csv(wells_csv_file_path, header=[0, 1, 2])


# Flatten column names
df_wells = flatten_df_columns(df_wells)


df_wells = gen_dt(df_wells)

# write_to_csv_interim = False
WRITE_TO_CSV_INTERIM = False
if WRITE_TO_CSV_INTERIM:
    df_wells.to_csv(f"{csv_folder_name}/{afi_file_name}_WELL_PRC.csv", index=True)


df_monthly_rates = gen_df_monthly_rates(df_wells)
df_monthly_rates = rename_columns(df_monthly_rates)
df_monthly_rates = manipulate_df_monthly_index_and_offset(df_monthly_rates)
df_monthly_rates.to_csv(f"{csv_folder_name}/{afi_file_name}_WELL_MR.csv", index=True)
