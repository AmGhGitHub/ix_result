from unittest import result

import pandas as pd


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
def read_field_csv(file_path, data_scope):
    """Read and preprocess field CSV data."""
    df = pd.read_csv(file_path, header=[0, 1, 2])
    df = flatten_df_columns(df)
    if data_scope == "field":
        df = create_datetime_index(
            df, data_scope, drop_columns=["DATE", "Time", "Entity Type", "Entity Name"]
        )
    elif data_scope == "well":
        df = create_datetime_index(
            df, data_scope, drop_columns=["DATE", "Time", "Entity Type"]
        )
    return df


def flatten_df_columns(df_input):
    """Flatten multi-level column headers and remove empty columns."""
    df = df_input.copy()
    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_")
        for col in df.columns.values
    ]
    df = df.loc[:, df.columns != ""]
    return df


def create_datetime_index(df_input, data_scope, drop_columns):
    """Create DateTime index from DATE and Time columns."""

    df = df_input.copy()

    if data_scope == "field":
        dates = pd.to_datetime(df["DATE"])
        origin = dates.min().normalize()
        dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")
        df.insert(0, "DateTime", dt)
        df = df.drop(columns=drop_columns)
        df.set_index("DateTime", inplace=True)
        return df

    if data_scope == "well":
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


def get_df_monthly_rates(df_input, data_scope, n_decimals=2):
    """Calculate monthly production rates from cumulative data."""
    df = df_input.copy()

    if data_scope == "field":
        df_monthly_cum = df.resample("ME").last()
        df_monthly_prod = df_monthly_cum.diff().dropna()
        days_in_month = df_monthly_prod.index.days_in_month

        df_mr = pd.DataFrame(index=df_monthly_prod.index)
        for col in df.columns:
            if "CUML" in col and df[col].dtype == float:
                df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
            elif "PRESSURE" in col and df[col].dtype == float:
                df_mr[col] = round(df[col].resample("ME").mean(), n_decimals)
        return df_mr

    if data_scope == "well":
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


def format_df_monthly(df_input, data_scope):
    df = df_input.copy()

    column_mapping = {
        "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
        "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
        "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
        "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
        "FPR_PRESSURE_PSIA": "AVG_RES_PRESSURE(PSIA)",
    }

    if data_scope == "well":
        column_mapping = {
            "WGIT_GAS_INJECTION_CUML_MSCF": "WELL_GAS_INJECTION_RATE(MSCF/DAY)",
            "WWPT_WATER_PRODUCTION_CUML_STB": "WELL_WATER_PRODUCTION_RATE(STB/DAY)",
            "WWIT_WATER_INJECTION_CUML_STB": "WELL_WATER_INJECTION_RATE(STB/DAY)",
            "WGPT_GAS_PRODUCTION_CUML_MSCF": "WELL_GAS_PRODUCTION_RATE(MSCF/DAY)",
            "WOPT_OIL_PRODUCTION_CUML_STB": "WELL_OIL_PRODUCTION_RATE(STB/DAY)",
            "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "WELL_BOTTOM_HOLE_PRESSURE(PSIA)",
        }

    df = df.rename(columns=column_mapping)
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=1)
    df.index = df.index + pd.offsets.MonthEnd(1)

    return df


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================
def gen_df_monthly_rates(csv_file_path, data_scope, save_intermediate_data):
    """Process field data from CSV to monthly rates."""
    df = read_field_csv(csv_file_path, data_scope)
    if save_intermediate_data:
        df.to_csv(f"{csv_file_path.replace('.CSV', '')}_PRC.CSV", index=True)
    df_monthly_rates = get_df_monthly_rates(df, data_scope)
    df_monthly_rates = format_df_monthly(df_monthly_rates, data_scope)

    df_monthly_rates.to_csv(f"{csv_file_path.replace('.CSV', '')}_MR.CSV", index=True)

    return 1


# =============================================================================
# EXECUTION
# =============================================================================
if __name__ == "__main__":
    # Configuration
    SIM_FOLDER_PATH = "D:/temp/0_sim_models/0_BasicModelFromIXF"
    SIM_FILE_NAME = "a_base_model"
    CSV_FOLDER_PATH = f"{SIM_FOLDER_PATH}/{SIM_FILE_NAME}_summary_results/"
    DATA_SCOPE = "WELL"
    SAVE_INTERMEDIATE_DATA = True

    if DATA_SCOPE.lower() == "field":
        CSV_FILE_PATH = f"{CSV_FOLDER_PATH}/{SIM_FILE_NAME}_FIELD.CSV"
    elif DATA_SCOPE.lower() == "well":
        CSV_FILE_PATH = f"{CSV_FOLDER_PATH}/{SIM_FILE_NAME}_WELL.CSV"
    else:
        raise ValueError(f"Invalid data scope: {DATA_SCOPE}")

    try:
        result = gen_df_monthly_rates(CSV_FILE_PATH, DATA_SCOPE, SAVE_INTERMEDIATE_DATA)
        if result == 1:
            print("Processing completed successfully!")
        else:
            print("Processing failed!")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
