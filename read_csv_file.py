from pathlib import Path
from typing import List, Optional

import pandas as pd

# Configuration constants
COLUMN_MAPPING = {
    "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
    "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
    "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
    "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
    "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
    "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "AVG_RESERVOIR_PRESSURE(PSIA)",
}

DEFAULT_DROP_COLUMNS = ["DATE", "Time", "Entity Type", "Entity Name"]


def flatten_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten multi-level column headers and clean column names."""
    df = df.copy()
    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)]).strip("_")
        for col in df.columns.values
    ]
    # Remove trailing empty columns
    df = df.loc[:, df.columns != ""]
    return df


def create_datetime_index(
    df: pd.DataFrame, drop_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Create DateTime index from DATE and Time columns."""
    if drop_columns is None:
        drop_columns = DEFAULT_DROP_COLUMNS.copy()

    dates = pd.to_datetime(df["DATE"])
    origin = dates.min().normalize()
    dt = origin + pd.to_timedelta(df["Time"].astype(float), unit="D")

    df = df.copy()
    df.insert(0, "DateTime", dt)
    df = df.drop(columns=drop_columns)
    df.set_index("DateTime", inplace=True)
    return df


def calculate_monthly_rates(df: pd.DataFrame, n_decimals: int = 2) -> pd.DataFrame:
    """Calculate monthly production rates from cumulative data."""
    # Get monthly cumulative values
    monthly_cum = df.resample("ME").last()
    monthly_prod = monthly_cum.diff().dropna()
    days_in_month = monthly_prod.index.days_in_month

    # Initialize result DataFrame
    monthly_rates = pd.DataFrame(index=monthly_prod.index)

    # Process columns based on type
    for col in df.columns:
        if "CUML" in col:
            monthly_rates[col] = round(monthly_prod[col] / days_in_month, n_decimals)
        elif "PRESSURE" in col:
            monthly_rates[col] = round(df[col].resample("ME").mean(), n_decimals)

    return monthly_rates


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns using predefined mapping."""
    return df.rename(columns=COLUMN_MAPPING)


def format_monthly_index(df: pd.DataFrame, months_offset: int = 0) -> pd.DataFrame:
    """Format monthly index and apply offset."""
    df = df.copy()
    df.index = df.index.to_period("M").to_timestamp()
    if months_offset != 0:
        df.index = df.index - pd.DateOffset(months=months_offset)
    return df


def process_field_data(
    csv_file_path: str,
    output_folder: str,
    file_prefix: str,
    months_offset: int = 0,
    save_intermediate: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Complete pipeline to process field data from CSV to monthly rates.

    Returns:
        tuple: (processed_field_data, monthly_rates_data)
    """
    # Read and process CSV
    field_df = pd.read_csv(csv_file_path, header=[0, 1, 2])
    field_df = flatten_df_columns(field_df)
    field_df = create_datetime_index(field_df)

    # Calculate monthly rates
    monthly_rates_df = calculate_monthly_rates(field_df)
    monthly_rates_df = rename_columns(monthly_rates_df)
    monthly_rates_df = format_monthly_index(monthly_rates_df, months_offset)

    # Save outputs
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    if save_intermediate:
        field_df.to_csv(output_path / f"{file_prefix}_FIELD_processed.csv", index=True)

    monthly_rates_df.to_csv(output_path / f"{file_prefix}_FIELD_MR.csv", index=True)

    return field_df, monthly_rates_df


# Example usage
if __name__ == "__main__":
    # Configuration
    sim_parent_folder = "D:/temp/0_sim_models/0_BasicModelFromIXF"
    afi_file_name = "a_base_model"
    csv_folder_name = f"{sim_parent_folder}/{afi_file_name}_summary_results/"
    field_csv_file_path = f"{csv_folder_name}/{afi_file_name}_FIELD.CSV"

    # Process the data using the streamlined pipeline
    try:
        field_data, monthly_rates_data = process_field_data(
            csv_file_path=field_csv_file_path,
            output_folder=csv_folder_name,
            file_prefix=afi_file_name,
            months_offset=0,
            save_intermediate=False,
        )

        print("✅ Processing completed successfully!")
        print(f"📊 Field data shape: {field_data.shape}")
        print(f"📈 Monthly rates shape: {monthly_rates_data.shape}")
        print(f"💾 Files saved to: {csv_folder_name}")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {field_csv_file_path}")
    except (ValueError, KeyError, pd.errors.EmptyDataError) as e:
        print(f"❌ Error processing data: {e}")
