from typing import List

import pandas as pd

# =============================================================================
# CONSTANTS
# =============================================================================
FIELD_SCOPE = "FIELD"
WELL_SCOPE = "WELL"
CUML_PREFIX = "CUML"
PRESSURE_PREFIX = "PRESSURE"


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
def read_field_csv(file_path: str, data_scope: str) -> pd.DataFrame:
    """Read and preprocess field CSV data.

    Args:
        file_path: Path to the CSV file
        data_scope: Either 'field' or 'well' to determine processing approach

    Returns:
        Processed DataFrame with DateTime index
    """
    df = pd.read_csv(file_path, header=[0, 1, 2])
    df = flatten_df_columns(df)

    if data_scope == FIELD_SCOPE:
        df = gen_dt_index(
            df, data_scope, drop_columns=["DATE", "Time", "Entity Type", "Entity Name"]
        )
    elif data_scope == WELL_SCOPE:
        df = gen_dt_index(df, data_scope, drop_columns=["DATE", "Time", "Entity Type"])
    else:
        raise ValueError(
            f"Invalid data_scope: {data_scope}. Must be '{FIELD_SCOPE}' or '{WELL_SCOPE}'"
        )

    return df


def flatten_df_columns(df_input: pd.DataFrame) -> pd.DataFrame:
    """Flatten multi-level column headers and remove empty columns.

    Args:
        df_input: DataFrame with multi-level columns

    Returns:
        DataFrame with flattened column names
    """
    df = df_input.copy()
    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_")
        for col in df.columns.values
    ]
    df = df.loc[:, df.columns != ""]
    return df


def gen_dt_index(
    df_input: pd.DataFrame, data_scope: str, drop_columns: List[str]
) -> pd.DataFrame:
    """Create DateTime index from DATE and Time columns.

    Args:
        df_input: Input DataFrame with DATE and Time columns
        data_scope: Either 'field' or 'well' to determine processing approach
        drop_columns: List of columns to drop after processing

    Returns:
        DataFrame with DateTime index
    """
    df = df_input.copy()

    if data_scope == FIELD_SCOPE:
        return _process_field_datetime(df, drop_columns)
    elif data_scope == WELL_SCOPE:
        return _process_well_datetime(df)
    else:
        raise ValueError(f"Invalid data_scope: {data_scope}")


def _process_field_datetime(df: pd.DataFrame, drop_columns: List[str]) -> pd.DataFrame:
    """Process field data to create DateTime index."""
    dates = pd.to_datetime(df["DATE"])
    origin = dates.min().normalize()
    dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")
    df.insert(0, "DateTime", dt)
    df = df.drop(columns=drop_columns)
    df.set_index("DateTime", inplace=True)
    return df


def _process_well_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Process well data to create DateTime index for each well."""
    well_dataframes = []

    for name in df["Entity Name"].unique():
        df_w = df.loc[df["Entity Name"] == name, :].copy()
        dates = pd.to_datetime(df_w["DATE"])
        origin = dates.min().normalize()
        dt = origin + pd.to_timedelta(df_w["Time"].astype(float), unit="D")
        df_w.insert(0, "DateTime", dt)
        df_w = df_w.drop(columns=["DATE", "Time", "Entity Type"])
        df_w.set_index("DateTime", inplace=True)
        well_dataframes.append(df_w)

    return pd.concat(well_dataframes, ignore_index=False)


def get_df_monthly_rates(
    df_input: pd.DataFrame, data_scope: str, n_decimals: int = 2
) -> pd.DataFrame:
    """Calculate monthly production rates from cumulative data.

    Args:
        df_input: Input DataFrame with production data
        data_scope: Either 'field' or 'well' to determine processing approach
        n_decimals: Number of decimal places for rounding

    Returns:
        DataFrame with monthly production rates
    """
    if data_scope == FIELD_SCOPE:
        return _calculate_field_monthly_rates(df_input, n_decimals)
    elif data_scope == WELL_SCOPE:
        return _calculate_well_monthly_rates(df_input, n_decimals)
    else:
        raise ValueError(f"Invalid data_scope: {data_scope}")


def _calculate_field_monthly_rates(df: pd.DataFrame, n_decimals: int) -> pd.DataFrame:
    """Calculate monthly rates for field data."""
    df_monthly_cum = df.resample("ME").last()
    df_monthly_prod = df_monthly_cum.diff().dropna()
    days_in_month = df_monthly_prod.index.days_in_month

    df_mr = pd.DataFrame(index=df_monthly_prod.index)
    for col in df.columns:
        if CUML_PREFIX in col and df[col].dtype == float:
            df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
        elif PRESSURE_PREFIX in col and df[col].dtype == float:
            df_mr[col] = round(df[col].resample("ME").mean(), n_decimals)
    return df_mr


def _calculate_well_monthly_rates(df: pd.DataFrame, n_decimals: int) -> pd.DataFrame:
    """Calculate monthly rates for well data."""
    well_dataframes = []

    for name in df["Entity Name"].unique():
        df_w = df.loc[df["Entity Name"] == name, :].copy()
        df_w = df_w.drop(columns=["Entity Name"])
        df_monthly_cum = df_w.resample("ME").last()
        df_monthly_prod = df_monthly_cum.diff().dropna()
        days_in_month = df_monthly_prod.index.days_in_month

        df_mr = pd.DataFrame(index=df_monthly_prod.index)
        for col in df_w.columns:
            if CUML_PREFIX in col:
                df_mr[col] = round(df_monthly_prod[col] / days_in_month, n_decimals)
            elif PRESSURE_PREFIX in col:
                df_mr[col] = round(df_w[col].resample("ME").mean(), n_decimals)
        df_mr.insert(0, "WELL_NAME", name)
        well_dataframes.append(df_mr)

    return pd.concat(well_dataframes, ignore_index=False)


def format_df_monthly(df_input: pd.DataFrame, data_scope: str) -> pd.DataFrame:
    """Format monthly DataFrame with proper column names and date indexing.

    Args:
        df_input: Input DataFrame with monthly rates
        data_scope: Either 'field' or 'well' to determine column mapping

    Returns:
        Formatted DataFrame with renamed columns and adjusted index
    """
    df = df_input.copy()

    column_mapping = _get_column_mapping(data_scope)
    df = df.rename(columns=column_mapping)
    df = _adjust_monthly_index(df)

    return df


def _get_column_mapping(data_scope: str) -> dict:
    """Get column mapping based on data scope."""
    if data_scope == FIELD_SCOPE:
        return {
            "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
            "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
            "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
            "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
            "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
            "FPR_PRESSURE_PSIA": "AVG_RES_PRESSURE(PSIA)",
        }
    elif data_scope == WELL_SCOPE:
        return {
            "WGIT_GAS_INJECTION_CUML_MSCF": "WELL_GAS_INJECTION_RATE(MSCF/DAY)",
            "WWPT_WATER_PRODUCTION_CUML_STB": "WELL_WATER_PRODUCTION_RATE(STB/DAY)",
            "WWIT_WATER_INJECTION_CUML_STB": "WELL_WATER_INJECTION_RATE(STB/DAY)",
            "WGPT_GAS_PRODUCTION_CUML_MSCF": "WELL_GAS_PRODUCTION_RATE(MSCF/DAY)",
            "WOPT_OIL_PRODUCTION_CUML_STB": "WELL_OIL_PRODUCTION_RATE(STB/DAY)",
            "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "WELL_BOTTOM_HOLE_PRESSURE(PSIA)",
        }
    else:
        raise ValueError(f"Invalid data_scope: {data_scope}")


def _adjust_monthly_index(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust the monthly index for proper date formatting."""
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=1)
    df.index = df.index + pd.offsets.MonthEnd(1)
    return df


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================
def gen_df_monthly_rates(
    csv_file_path: str, data_scope: str, save_intermediate_data: bool
) -> int:
    """Process field data from CSV to monthly rates.

    Args:
        csv_file_path: Path to the input CSV file
        data_scope: Either 'field' or 'well' to determine processing approach
        save_intermediate_data: Whether to save intermediate processed data

    Returns:
        1 if processing completed successfully
    """
    try:
        df = read_field_csv(csv_file_path, data_scope)

        if save_intermediate_data:
            intermediate_path = f"{csv_file_path.replace('.CSV', '')}_PRC.CSV"
            df.to_csv(intermediate_path, index=True)

        df_monthly_rates = get_df_monthly_rates(df, data_scope)
        df_monthly_rates = format_df_monthly(df_monthly_rates, data_scope)

        output_path = f"{csv_file_path.replace('.CSV', '')}_MR.CSV"
        df_monthly_rates.to_csv(output_path, index=True)

        return 1

    except Exception as e:
        print(f"Error processing {csv_file_path}: {str(e)}")
        raise


# =============================================================================
# EXECUTION
# =============================================================================
if __name__ == "__main__":
    # Configuration
    SIM_FOLDER_PATH = "D:/temp/0_sim_models/0_BasicModelFromIXF"
    SIM_AFI_FILE_NAME = "a_base_model"

    CSV_FOLDER_PATH = f"{SIM_FOLDER_PATH}/{SIM_AFI_FILE_NAME}_summary_results/"
    DATA_SCOPE = "field"  # "field" or "well"
    SAVE_INTERMEDIATE_DATA = False

    # Validate and set data scope
    data_scope_upper = DATA_SCOPE.upper()
    if data_scope_upper == FIELD_SCOPE:
        CSV_FILE_PATH = f"{CSV_FOLDER_PATH}/{SIM_AFI_FILE_NAME}_FIELD.CSV"
    elif data_scope_upper == WELL_SCOPE:
        CSV_FILE_PATH = f"{CSV_FOLDER_PATH}/{SIM_AFI_FILE_NAME}_WELL.CSV"
    else:
        raise ValueError(
            f"Invalid data scope: {DATA_SCOPE}. Must be '{FIELD_SCOPE}' or '{WELL_SCOPE}'"
        )

    try:
        result = gen_df_monthly_rates(
            CSV_FILE_PATH, data_scope_upper, SAVE_INTERMEDIATE_DATA
        )
        if result == 1:
            print("Processing completed successfully!")
        else:
            print("Processing failed!")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
    except (ValueError, KeyError, pd.errors.EmptyDataError) as e:
        print(f"Data processing error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
