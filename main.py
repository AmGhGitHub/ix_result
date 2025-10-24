import pandas as pd


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
def read_field_csv(file_path):
    """Read and preprocess field CSV data."""
    df = pd.read_csv(file_path, header=[0, 1, 2])
    df = flatten_df_columns(df)
    df = create_datetime_index(
        df, drop_columns=["DATE", "Time", "Entity Type", "Entity Name"]
    )
    return df


def flatten_df_columns(df):
    """Flatten multi-level column headers and remove empty columns."""
    df = df.copy()
    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_")
        for col in df.columns.values
    ]
    df = df.loc[:, df.columns != ""]
    return df


def create_datetime_index(df, drop_columns=None):
    """Create DateTime index from DATE and Time columns."""
    if drop_columns is None:
        drop_columns = ["DATE", "Time"]

    df = df.copy()
    dates = pd.to_datetime(df["DATE"])
    origin = dates.min().normalize()
    dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")
    df.insert(0, "DateTime", dt)
    df = df.drop(columns=drop_columns)
    df.set_index("DateTime", inplace=True)
    return df


def calculate_monthly_rates(df, n_decimals=2):
    """Calculate monthly production rates from cumulative data."""
    df = df.copy()
    df_monthly_cum = df.resample("ME").last()
    df_monthly_prod = df_monthly_cum.diff().dropna()
    days_in_month = df_monthly_prod.index.days_in_month

    df_mr = pd.DataFrame(index=df_monthly_prod.index)
    for col in df.columns:
        if "CUML" in col and pd.api.types.is_numeric_dtype(df[col]):
            df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
        elif "PRESSURE" in col and pd.api.types.is_numeric_dtype(df[col]):
            df_mr[col] = round(df[col].resample("ME").mean(), n_decimals)
    return df_mr


def rename_columns(df):
    """Rename columns to more descriptive names."""
    column_mapping = {
        "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
        "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
        "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
        "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
        "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "AVG_RESERVOIR_PRESSURE(PSIA)",
    }
    return df.rename(columns=column_mapping)


def format_monthly_index(df_input):
    """Format monthly index and apply offset."""
    df = df_input.copy()
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=1)
    df.index = df.index + pd.offsets.MonthEnd(1)
    return df


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================
def process_field_data(csv_folder_name, field_csv_file_name, afi_file_name):
    """Process field data from CSV to monthly rates."""
    df_field = read_field_csv(f"{csv_folder_name}/{field_csv_file_name}")
    df_monthly_rates = calculate_monthly_rates(df_field)
    df_monthly_rates = rename_columns(df_monthly_rates)
    df_monthly_rates = format_monthly_index(df_monthly_rates)

    df_monthly_rates.to_csv(
        f"{csv_folder_name}/{afi_file_name}_FIELD_MR.csv", index=True
    )
    return 1


# =============================================================================
# EXECUTION
# =============================================================================
if __name__ == "__main__":
    # Configuration
    sim_parent_folder = "D:/temp/0_sim_models/0_BasicModelFromIXF"
    afi_file_name = "a_base_model"
    csv_folder_name = f"{sim_parent_folder}/{afi_file_name}_summary_results/"
    field_csv_file_name = f"{afi_file_name}_FIELD.CSV"

    try:
        result = process_field_data(csv_folder_name, field_csv_file_name, afi_file_name)
        if result == 1:
            print("Processing completed successfully!")
        else:
            print("Processing failed!")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_folder_name}/{field_csv_file_name}")
    except Exception as e:
        print(f"Error processing data: {e}")
