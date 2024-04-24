import pandas as pd
import numpy as np
from scipy.stats import gamma, norm

def filter_data_for_spi(df):
    df['Date'] = pd.to_datetime(df['Date'])
    filtered_df = df[(df['Date'].dt.year >= 1985) & (df['Date'].dt.year <= 2015)]
    stations_with_full_coverage = filtered_df.groupby('Nazwa Stacji').filter(
        lambda x: x['Date'].dt.year.nunique() == (2015 - 1985 + 1))
    stations = stations_with_full_coverage['Nazwa Stacji'].unique()
    print("STATIONS WITH 30Y COVERAGE: ", stations)
    return stations_with_full_coverage, stations


def calculate_spi_for_period(values):
    # Ensure that values are positive and not all zeros for fitting gamma distribution
    filtered_values = values[values > 0]

    if len(filtered_values) == 0:
        return pd.Series([np.nan] * len(values), index=values.index)

    # Fit the gamma distribution
    params = gamma.fit(filtered_values, floc=0)

    # Calculate the CDF from the gamma distribution
    cdf_values = gamma.cdf(values, *params)

    # Transform CDF to Z-scores (SPI)
    spi = norm.ppf(cdf_values)

    # Replace infinite values with NaNs to handle edge cases
    spi[np.isinf(spi)] = np.nan

    return pd.Series(spi, index=values.index)


def calculate_spi_adjusted(data):
    print("Calculating SPI...")
    # if 'Date' not in data.columns or pd.to_datetime(data['Date'], errors='coerce').isna().any():
    #     data['Date'] = pd.to_datetime(data[['Rok', 'Miesiac', 'Dzien']])
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)
    data_grouped = data.groupby('Nazwa Stacji')

    # Prepare DataFrame to collect results
    results = pd.DataFrame()

    for name, group in data_grouped:
        # Perform rolling sum within each group
        grouped = group.resample('ME').sum()['Suma Opadow [mm]']
        spi_data = pd.DataFrame({
            'SPI-1': grouped.rolling(window=1, min_periods=1).sum(),
            'SPI-3': grouped.rolling(window=3, min_periods=3).sum(),
            'SPI-12': grouped.rolling(window=12, min_periods=12).sum()
        })

        # Calculate SPI for each period
        for period in ['SPI-1', 'SPI-3', 'SPI-12']:
            spi_data[period] = calculate_spi_for_period(spi_data[period])

        spi_data['Nazwa Stacji'] = name
        results = pd.concat([results, spi_data])

    # Adjust index and columns for output
    results.reset_index(inplace=True)
    results.set_index(['Nazwa Stacji', 'Date'], inplace=True)

    # Formatting Date to Year-Month only
    results.index = results.index.set_levels([results.index.levels[0], results.index.levels[1].to_period('M')])
    #pd.set_option('display.max_rows', 2000)
    #print("SPI RESULTS: ", results.head(10))
    return results


def save_spi_data_to_parquet(spi_data, filepath):
    spi_data.to_parquet(filepath)


df = pd.read_parquet('data/parquet_files/rainfall_filled_data.parquet', engine='pyarrow')
stations_with_full_coverage, stations = filter_data_for_spi(df)
spi_adjusted_data = calculate_spi_adjusted(stations_with_full_coverage)
save_spi_data_to_parquet(spi_adjusted_data, 'data/parquet_files/spi_data.parquet')
