import pandas as pd

# You can provide explicit case names here if you prefer not to use --cases
# Example: ["PatC_102-16-02", "PatB_101-05-01"]
data_folder = "input_data"
output_folder = "output_data"
input_file_name = "Nisku_PatXtC_SP_Sor40pct.xlsx"
output_file_name = input_file_name.replace(".xlsx", "_monthly_avg_rate.xlsx")
n_rows_to_skip = 4
Columns_names = [
    "date",
    "inj_gas_cum_sm3",
    "prd_gas_cum_sm3",
    "prd_oil_cum_sm3",
    "prd_water_cum_sm3",
]


def main():
    # Load cumulative data
    df = pd.read_excel(
        f"{data_folder}/{input_file_name}", header=None, skiprows=n_rows_to_skip
    )
    df.rename(
        columns={
            0: Columns_names[0],
            1: Columns_names[1],
            2: Columns_names[2],
            3: Columns_names[3],
            4: Columns_names[4],
        },
        inplace=True,
    )
    df[Columns_names[0]] = pd.to_datetime(df[Columns_names[0]])
    df = df.set_index(Columns_names[0])

    # Resample to monthly end to get the last cumulative value per month
    monthly_cum = df.resample("ME").last()

    # Calculate monthly production by taking difference of consecutive cumulative values
    monthly_prod = monthly_cum.diff().dropna()

    # Calculate average daily rate for each month
    # Get the number of days in each month
    days_in_month = monthly_prod.index.days_in_month

    # Divide monthly production by number of days to get average daily rate
    monthly_avg_rate = monthly_prod.div(days_in_month, axis=0)

    # Round to 2 decimal places
    monthly_avg_rate = monthly_avg_rate.round(2)

    # Change index to first day of each month for clarity
    monthly_avg_rate.index = monthly_avg_rate.index.to_period("M").to_timestamp()

    # Rename columns to reflect that these are now rates
    monthly_avg_rate.rename(
        columns={
            Columns_names[1]: "inj_gas_rate_sm3_per_day",
            Columns_names[2]: "prd_gas_rate_sm3_per_day",
            Columns_names[3]: "prd_oil_rate_sm3_per_day",
            Columns_names[4]: "prd_water_rate_sm3_per_day",
        },
        inplace=True,
    )

    # Save the monthly average rates
    # output_file = (
    #     f"{output_folder}/{input_file_name.replace('.xlsx', '_monthly_avg_rate.xlsx')}"
    # )
    monthly_avg_rate.to_excel(f"{output_folder}/{output_file_name}", index=True)


if __name__ == "__main__":
    main()
