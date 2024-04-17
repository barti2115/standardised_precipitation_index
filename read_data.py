import os
import pandas as pd
from multiprocessing import Pool, cpu_count


def read_csv_file(file_path, column_names):
    try:
        return pd.read_csv(file_path, names=column_names, encoding='windows-1250')
    except UnicodeDecodeError as e:
        print(f"Error reading file '{file_path}' with encoding 'windows-1250': {e}")
        return None
    except Exception as e:
        print(f"An error occurred while reading file '{file_path}' with encoding 'windows-1250': {e}")
        return None

def read_csv_files(root_dir, column_names):
    # Construct list of file paths
    files = [os.path.join(subdir, file) for subdir, _, files in os.walk(root_dir) for file in files if file.endswith('.csv')]
    # Determine the number of worker processes to use (default to number of CPU cores)
    num_processes = min(cpu_count(), len(files)) if len(files) > 0 else 1
    # Create a pool of worker processes
    with Pool(processes=num_processes) as pool:
        # Read files in parallel
        results = [pool.apply_async(read_csv_file, args=(file, column_names)) for file in files]

        # Get the results from the asynchronous calls
        dfs = [result.get() for result in results]

    # Filter out None values (failed reads) and concatenate DataFrames
    combined_df = pd.concat([df for df in dfs if df is not None], ignore_index=True)
    return combined_df

if __name__ == "__main__":
    data_directory = "data/precipitation"
    column_names = [
        "Kod stacji",
        "Nazwa Stacji",
        "Rok",
        "Miesiac",
        "Dzien",
        "Suma Opadow [mm]",
        "Status pomiaru SMDB",
        "Rodzaj opadu [S/W/ ]",
        "Wysokosc pokrywy snieznej cm",
        "Status pomiaru PKSN",
        "Wysokosc swiezospadlego sniedu [cm]",
        "Status pomiaru HSS",
        "Gatunek śniegu  [kod]",
        "Status pomiaru GATS",
        "Rodzaj pokrywy śnieżnej [kod]",
        "Status pomiaru RPSN"
        ]

    # Call the function to read all CSV files
    result_df = read_csv_files(data_directory, column_names)

    # Define the directory path to store the Parquet files
    parquet_dir = "./data/parquet_files/unfiltered_data"

    # Create the directory if it doesn't exist
    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir)

    # Define the path to save the Parquet file
    parquet_file_path = os.path.join(parquet_dir, "data.parquet")
    
    # Save the DataFrame to a Parquet file
    result_df.to_parquet(parquet_file_path)
    print(f"Resulting DataFrame saved to '{parquet_file_path}'")
