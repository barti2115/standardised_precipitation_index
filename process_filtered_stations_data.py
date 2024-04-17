import os
import pandas as pd
import pyarrow.parquet as pq

def read_filtered_data(file_path):
    print("Reading filtered data...")
    table = pq.read_table(file_path, use_threads=True)
    filtered_stations_df = table.to_pandas()
    print("Filtered data read successfully.")
    return filtered_stations_df

def group_data_by_station(df):
    print("Grouping data by station...")
    grouped = df.groupby('Nazwa Stacji')
    print("Data grouped successfully.")
    return grouped

def save_station_data(grouped, directory_path):
    print("Saving data for each station...")
    station_data_ranges = {}
    for station, data in grouped:
        print(f"Processing station: {station}")
        # Drop the 'Nazwa Stacji' column
        data_without_station_name = data.drop(columns=['Nazwa Stacji'])
        # Define the path for saving the parquet file
        file_path = os.path.join(directory_path, f'{station}.parquet')
        # Save the DataFrame as a parquet file
        data_without_station_name.to_parquet(file_path, index=False)
        print(f"Data for station {station} saved to: {file_path}")
        # Calculate data range
        min_date = data[["Rok", "Miesiac", "Dzien"]].min().to_dict()
        max_date = data[["Rok", "Miesiac", "Dzien"]].max().to_dict()
        station_data_ranges[station] = (min_date, max_date)
    return station_data_ranges

def save_station_time_ranges(station_data_ranges, directory_path):
    # Convert dictionary to DataFrame
    df_station_time_ranges = pd.DataFrame.from_dict(station_data_ranges, orient='index', columns=['Min_Date', 'Max_Date'])
    # Save DataFrame as parquet file
    file_path = os.path.join(directory_path, 'station_time_ranges.parquet')
    df_station_time_ranges.to_parquet(file_path)
    print("Data saved successfully.")

if __name__ == "__main__":
    file_path = "./data/parquet_files/filtered_data/data.parquet"
    filtered_stations_df = read_filtered_data(file_path)
    
    grouped = group_data_by_station(filtered_stations_df)

    # Define the directory paths
    station_data_directory = 'data/parquet_files/station_data'
    station_time_ranges_directory = 'data/parquet_files/station_time_ranges'

    # Create the directories if they don't exist
    for directory in [station_data_directory, station_time_ranges_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory created: {directory}")
        else:
            print(f"Directory already exists: {directory}")

    station_data_ranges = save_station_data(grouped, station_data_directory)
    save_station_time_ranges(station_data_ranges, station_time_ranges_directory)
