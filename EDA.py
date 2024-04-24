import pandas as pd
import matplotlib.pyplot as plt
import os

# Ensure the directory exists
plot_save_dir = 'data/plots'
data_save_dir = 'data/parquet_files'
#os.makedirs(save_dir, exist_ok=True)
os.makedirs(plot_save_dir, exist_ok=True)

def load_data():
    # Load data files
    station_time_ranges = pd.read_parquet('data/parquet_files/station_time_ranges', engine='pyarrow')
    localization_data = pd.read_parquet('data/localization/filtered_localization_data.parquet', engine='pyarrow')
    all_data = pd.read_parquet('data/parquet_files/filtered_data/data.parquet', engine='pyarrow')
    return all_data, localization_data, station_time_ranges

def display_data_info(data):
    # Display basic information and the first few rows of the datasets
    print(f"Data head:\n", data.head(3))
    #print(f"Data info:\n" , data.info())
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.max_rows', 5)
    #return data.info()


def plot_time_range_coverage(data):
    # Parse dates and create a datetime column
    data['Date'] = pd.to_datetime(
        data['Rok'].astype(str) + '-' + data['Miesiac'].astype(str) + '-' + data['Dzien'].astype(str),
        format='%Y-%m-%d')
    station_date_range = data.groupby('Nazwa Stacji')['Date'].agg(['min', 'max'])
    total_days = (data['Date'].max() - data['Date'].min()).days + 1
    station_date_range['Days Covered'] = (station_date_range['max'] - station_date_range['min']).dt.days + 1
    station_date_range['Coverage %'] = (station_date_range['Days Covered'] / total_days) * 100

    fig, ax = plt.subplots(figsize=(10, 20))
    for index, row in station_date_range.iterrows():
        ax.plot([row['min'], row['max']], [index, index], marker='o', linestyle='-')
    ax.set_yticks(range(len(station_date_range)))
    ax.set_yticklabels(station_date_range.index)
    ax.set_title('Time Range Coverage for Each Station')
    plt.xticks(rotation=45)
    plt.show()
    plt.savefig(os.path.join(plot_save_dir,'timerange.png'))


def plot_precipitation_boxplots(data):
    # Plot boxplots for precipitation data at specified stations
    stations = ['KRZYŻ', 'WETLINA', 'LIPNICA DOLNA']
    for station in stations:
        plt.figure()
        plt.boxplot(data[data['Nazwa Stacji'] == station]['Suma Opadow [mm]'])
        plt.title(f'Boxplot of Suma Opadow [mm] at {station}')
        plt.ylabel('Suma Opadow [mm]')
        plt.show()
        plt.savefig(os.path.join(plot_save_dir,f'{station}_boxplot.png'))


def display_nan_statistics(data):
    nan_counts = data.isnull().sum()
    total_values = len(data)
    nan_percentage = (nan_counts / total_values) * 100
    nan_percentage_df = pd.DataFrame({'Column Name': nan_counts.index, 'NaN Percentage': nan_percentage.values})
    nan_percentage_df_sorted = nan_percentage_df.sort_values(by='NaN Percentage', ascending=False)

    plt.figure(figsize=(12, 8))
    bars = plt.bar(nan_percentage_df_sorted['Column Name'], nan_percentage_df_sorted['NaN Percentage'])
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, round(yval, 2), ha='center', va='bottom')
    plt.title('Percentage of Null Values per Column')
    plt.xlabel('Column Name')
    plt.ylabel('Percentage of Null Values')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(os.path.join(plot_save_dir,'nan_statistics.png'))
    plt.show()

def process_status_code(data):
    non_nan_smdb = data[data['Status pomiaru SMDB'] == 8.0]
    print(f"Rows with 'Status pomiaru SMDB' == 8.0: {len(non_nan_smdb)}")
    return non_nan_smdb
def deduplicate_data(data):
    initial_size = data.shape
    print(f"Initial size of the DataFrame: {initial_size}")
    print("Dropping duplicates...")
    data_no_duplicates = data.drop_duplicates()
    new_size = data_no_duplicates.shape
    print(f"Initial size of the DataFrame: {initial_size}")
    print(f"Size of the DataFrame after dropping duplicates: {new_size}")
    return data_no_duplicates


def apply_rolling_median(data):
    print("Applying rolling median to fill rows with no measurment...")
    data['Date'] = pd.to_datetime(data['Date'])  # Ensure 'Date' is in datetime format
    data = data.groupby('Nazwa Stacji').apply(lambda group: fill_rainfall(group)).reset_index(drop=True)

    data.to_parquet(os.path.join(data_save_dir,'rainfall_filled_data.parquet'))

    return data

def fill_rainfall(group):
    #print("Filling rows with no measure...")
    group['Rolling Median Rain'] = group['Suma Opadow [mm]'].rolling(window=14, center=True).mean()
    mask = (group['Status pomiaru SMDB'] == 8.0) & (group['Suma Opadow [mm]'] == 0.0)
    group.loc[mask, 'Suma Opadow [mm]'] = group.loc[mask, 'Rolling Median Rain']
    return group.drop(columns=['Rolling Median Rain'])

def calculate_change(data):
    zmieniono= data[(data['Status pomiaru SMDB'] == 8.0) & (data['Suma Opadow [mm]'] != 0.0)].count()
    print(f"Changed {zmieniono['Suma Opadow [mm]']} rows.")

def describe_precipitation(data):
    print("Calculating basic staticstics...")
    descriptions = data.groupby('Nazwa Stacji')['Suma Opadow [mm]'].describe()
    min_mean_station = descriptions['mean'].idxmin()
    max_mean_station = descriptions['mean'].idxmax()
    max_max_station = descriptions['max'].idxmax()
    print(f"Station with the minimum mean precipitation: {min_mean_station} with mean {descriptions['mean'].min()}")
    print(f"Station with the maximum mean precipitation: {max_mean_station} with mean {descriptions['mean'].max()}")
    print(f"Station with the maximum precipitation: {max_max_station} with max {descriptions['max'].max()}")
    return descriptions

def plot_histograms(data, stations):
    print("Plotting histograms...")
    for station in stations:
        plt.figure()
        data[data['Nazwa Stacji'] == station]['Suma Opadow [mm]'].hist()
        plt.title(f'Histogram of Suma Opadow [mm] at {station}')
        plt.xlabel('Suma Opadow [mm]')
        plt.ylabel('Frequency')
        plt.show()

def plot_all_station_hist(data):
    print("Plotting combined histogram for all stations...")
    plt.figure(figsize=(10, 6))  # You can customize the size as needed

    # Plot histogram for each station on the same figure
    for station in data['Nazwa Stacji'].unique():
        station_data = data[data['Nazwa Stacji'] == station]['Suma Opadow [mm]']
        plt.hist(station_data, bins=50, alpha=0.7)  # Bins and alpha can be adjusted

    plt.title('Combined Histogram of Suma Opadow [mm] for All Stations')
    plt.xlabel('Suma Opadow [mm]')
    plt.ylabel('Frequency')
    plt.legend(loc='upper right')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(plot_save_dir,'combined_hist.png'))  # Save the figure
    plt.close()

# Example usage
all_data, localization_data, station_time_ranges = load_data()
display_data_info(all_data)
display_data_info(localization_data)
display_data_info(station_time_ranges)

plot_time_range_coverage(all_data)

non_nan_data = process_status_code(all_data)
display_nan_statistics(all_data)
deduplicated_data = deduplicate_data(all_data)
rainfall_adjusted_data = apply_rolling_median(deduplicated_data)
calculate_change(rainfall_adjusted_data)
descriptions = describe_precipitation(rainfall_adjusted_data)
plot_histograms(rainfall_adjusted_data, ['KRZYŻ', 'WETLINA', 'LIPNICA DOLNA'])
plot_precipitation_boxplots(rainfall_adjusted_data)
plot_all_station_hist(rainfall_adjusted_data)