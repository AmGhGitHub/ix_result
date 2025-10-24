import numpy as np
import pandas as pd


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
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


def create_datetime_index_wells(df):
    """Create DateTime index for wells data (handles multiple entities)."""
    df = df.copy()
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


def calculate_monthly_rates_field(df, n_decimals=2):
    """Calculate monthly production rates for field data."""
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


def calculate_monthly_rates_wells(df, n_decimals=2):
    """Calculate monthly production rates for wells data."""
    df = df.copy()
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


def rename_columns_field(df):
    """Rename columns for field data."""
    column_mapping = {
        "FGIT_GAS_INJECTION_CUML_MSCF": "FIELD_GAS_INJECTION_RATE(MSCF/DAY)",
        "FWPT_WATER_PRODUCTION_CUML_STB": "FIELD_WATER_PRODUCTION_RATE(STB/DAY)",
        "FWIT_WATER_INJECTION_CUML_STB": "FIELD_WATER_INJECTION_RATE(STB/DAY)",
        "FGPT_GAS_PRODUCTION_CUML_MSCF": "FIELD_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "FOPT_OIL_PRODUCTION_CUML_STB": "FIELD_OIL_PRODUCTION_RATE(STB/DAY)",
        "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "AVG_RESERVOIR_PRESSURE(PSIA)",
    }
    return df.rename(columns=column_mapping)


def rename_columns_wells(df):
    """Rename columns for wells data."""
    column_mapping = {
        "WGIT_GAS_INJECTION_CUML_MSCF": "WELL_GAS_INJECTION_RATE(MSCF/DAY)",
        "WWPT_WATER_PRODUCTION_CUML_STB": "WELL_WATER_PRODUCTION_RATE(STB/DAY)",
        "WWIT_WATER_INJECTION_CUML_STB": "WELL_WATER_INJECTION_RATE(STB/DAY)",
        "WGPT_GAS_PRODUCTION_CUML_MSCF": "WELL_GAS_PRODUCTION_RATE(MSCF/DAY)",
        "WOPT_OIL_PRODUCTION_CUML_STB": "WELL_OIL_PRODUCTION_RATE(STB/DAY)",
        "WBHP_BOTTOM_HOLE_PRESSURE_PSIA": "WELL_BOTTOM_HOLE_PRESSURE(PSIA)",
    }
    return df.rename(columns=column_mapping)


def format_monthly_index(df):
    """Format monthly index and apply offset."""
    df = df.copy()
    df.index = df.index.to_period("M").to_timestamp()
    df.index = df.index - pd.DateOffset(months=1)
    df.index = df.index + pd.offsets.MonthEnd(1)
    return df


# =============================================================================
# MAIN PROCESSING PIPELINES
# =============================================================================
def process_field_data(
    csv_folder_name, field_csv_file_name, afi_file_name, save_intermediate=False
):
    """Process field data from CSV to monthly rates."""
    print("Processing field data...")

    # Read and preprocess CSV
    df_field = pd.read_csv(f"{csv_folder_name}/{field_csv_file_name}", header=[0, 1, 2])
    df_field = flatten_df_columns(df_field)
    df_field = create_datetime_index(
        df_field, drop_columns=["DATE", "Time", "Entity Type", "Entity Name"]
    )

    # Save intermediate processed data if requested
    if save_intermediate:
        df_field.to_csv(f"{csv_folder_name}/{afi_file_name}_FIELD_PRC.csv", index=True)

    # Calculate monthly rates
    df_monthly_rates = calculate_monthly_rates_field(df_field)
    df_monthly_rates = rename_columns_field(df_monthly_rates)
    df_monthly_rates = format_monthly_index(df_monthly_rates)

    # Save final output
    df_monthly_rates.to_csv(
        f"{csv_folder_name}/{afi_file_name}_FIELD_MR.csv", index=True
    )
    print("Field data processing completed!")
    return df_monthly_rates


def process_wells_data(
    csv_folder_name, wells_csv_file_name, afi_file_name, save_intermediate=False
):
    """Process wells data from CSV to monthly rates."""
    print("Processing wells data...")

    # Read and preprocess CSV
    df_wells = pd.read_csv(f"{csv_folder_name}/{wells_csv_file_name}", header=[0, 1, 2])
    df_wells = flatten_df_columns(df_wells)
    df_wells = create_datetime_index_wells(df_wells)

    # Save intermediate processed data if requested
    if save_intermediate:
        df_wells.to_csv(f"{csv_folder_name}/{afi_file_name}_WELL_PRC.csv", index=True)

    # Calculate monthly rates
    df_monthly_rates = calculate_monthly_rates_wells(df_wells)
    df_monthly_rates = rename_columns_wells(df_monthly_rates)
    df_monthly_rates = format_monthly_index(df_monthly_rates)

    # Save final output
    df_monthly_rates.to_csv(
        f"{csv_folder_name}/{afi_file_name}_WELL_MR.csv", index=True
    )
    print("Wells data processing completed!")
    return df_monthly_rates


def process_all_data(
    csv_folder_name,
    afi_file_name,
    process_field=True,
    process_wells=True,
    save_intermediate=False,
):
    """Process both field and wells data."""
    results = {}

    if process_field:
        field_csv_file_name = f"{afi_file_name}_FIELD.CSV"
        try:
            results["field"] = process_field_data(
                csv_folder_name, field_csv_file_name, afi_file_name, save_intermediate
            )
        except FileNotFoundError:
            print(
                f"Warning: Field CSV file not found at {csv_folder_name}/{field_csv_file_name}"
            )
            results["field"] = None
        except Exception as e:
            print(f"Error processing field data: {e}")
            results["field"] = None

    if process_wells:
        wells_csv_file_name = f"{afi_file_name}_WELL.CSV"
        try:
            results["wells"] = process_wells_data(
                csv_folder_name, wells_csv_file_name, afi_file_name, save_intermediate
            )
        except FileNotFoundError:
            print(
                f"Warning: Wells CSV file not found at {csv_folder_name}/{wells_csv_file_name}"
            )
            results["wells"] = None
        except Exception as e:
            print(f"Error processing wells data: {e}")
            results["wells"] = None

    return results


# =============================================================================
# EXECUTION
# =============================================================================
if __name__ == "__main__":
    # Configuration
    sim_parent_folder = "D:/temp/0_sim_models/0_BasicModelFromIXF"
    afi_file_name = "a_base_model"
    csv_folder_name = f"{sim_parent_folder}/{afi_file_name}_summary_results/"

    # Processing options
    process_field_data_flag = True
    process_wells_data_flag = True
    save_intermediate_files = False

    try:
        print("Starting combined data processing...")
        results = process_all_data(
            csv_folder_name=csv_folder_name,
            afi_file_name=afi_file_name,
            process_field=process_field_data_flag,
            process_wells=process_wells_data_flag,
            save_intermediate=save_intermediate_files,
        )

        print("\nProcessing Summary:")
        if results.get("field") is not None:
            print(
                f"✅ Field data processed successfully - Shape: {results['field'].shape}"
            )
        else:
            print("❌ Field data processing failed or skipped")

        if results.get("wells") is not None:
            print(
                f"✅ Wells data processed successfully - Shape: {results['wells'].shape}"
            )
        else:
            print("❌ Wells data processing failed or skipped")

        print(f"\n📁 Output files saved to: {csv_folder_name}")

    except Exception as e:
        print(f"❌ Error in main processing: {e}")
