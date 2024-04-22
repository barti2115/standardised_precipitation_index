import pandas as pd
import geopy.geocoders
import ssl
import certifi
import pyarrow.parquet as pq
import os
import geopandas as gpd
from shapely.geometry import Point
import urllib.request

def filter_original_data(original_df, filtered_dict):
    print("Filtering original data frame...")
    filtered_df = original_df[original_df["Nazwa Stacji"].isin(filtered_dict.keys())]
    print("Original data frame filtered.")
    return filtered_df

def read_parquet(parquet_file_path):
    print("Reading Parquet file...")
    table = pq.read_table(parquet_file_path, use_threads=True)
    df = table.to_pandas()
    print("Parquet file read successfully.")
    return df

def get_unique_stations(parquet_file_path):
    df = read_parquet(parquet_file_path)
    unique_stations = df["Nazwa Stacji"].unique()
    print("Unique stations retrieved.")
    return unique_stations

def get_station_localization(unique_stations):
    print("Initializing GeoPy geolocator...")
    ctx = ssl.create_default_context(cafile=certifi.where())
    geopy.geocoders.options.default_ssl_context = ctx
    geolocator = geopy.geocoders.Nominatim(user_agent="my_geocoder")
    print("GeoPy geolocator initialized.")
    
    localization_dict = {}
    
    print("Geocoding station names...")
    for station in unique_stations:
        print(f"Geocoding station: {station}")
        location = geolocator.geocode(station)
        if location is not None:
            localization_dict[station] = (location.longitude, location.latitude)
            print(f"Coordinates found for {station}: Longitude={location.longitude}, Latitude={location.latitude}")
        else:
            print(f"Coordinates not found for {station}.")
    
    print("Geocoding completed.")
    return localization_dict

def save_localization_data(localization_dict, output_parquet_path):
    print(f"Saving localization data to Parquet file: {output_parquet_path}")
    localization_df = pd.DataFrame(localization_dict.items(), columns=["Nazwa Stacji", "Coordinates"])
    localization_df.to_parquet(output_parquet_path)
    print("Localization data saved successfully.")

def load_filtered_localization(filtered_output_parquet_path):
    print(f"Reading filtered localization data from Parquet file: {filtered_output_parquet_path}")
    table = pq.read_table(filtered_output_parquet_path, use_threads=True)
    filtered_localization_df = table.to_pandas()
    filtered_localization_dict = dict(zip(filtered_localization_df["Nazwa Stacji"], filtered_localization_df["Coordinates"]))
    return filtered_localization_dict

def filter_data_and_save(parquet_file_path, filtered_localization_dict, filtered_df_parquet_path):
    df = read_parquet(parquet_file_path)
    filtered_df = filter_original_data(df, filtered_localization_dict)
    if not os.path.exists(os.path.dirname(filtered_df_parquet_path)):
        os.makedirs(os.path.dirname(filtered_df_parquet_path))
        print(f"Created directory: {os.path.dirname(filtered_df_parquet_path)}")
    filtered_df.to_parquet(filtered_df_parquet_path)
    print("Data filtered by localization has been saved successfully.")

if __name__ == "__main__":
    parquet_file_path = "./data/parquet_files/unfiltered_data/data.parquet"
    output_parquet_path = "./data/localization/unfiltered_localization_data.parquet"
    filtered_output_parquet_path = "./data/localization/filtered_localization_data.parquet"
    filtered_df_parquet_path = "./data/parquet_files/filtered_data/data.parquet"
    
    print(f"Processing Parquet file: {parquet_file_path}")
    unique_stations = get_unique_stations(parquet_file_path)
    
    if not os.path.exists(filtered_output_parquet_path):
        if not os.path.exists(os.path.dirname(output_parquet_path)):
            os.makedirs(os.path.dirname(output_parquet_path))
        localization_dict = get_station_localization(unique_stations)
        save_localization_data(localization_dict, output_parquet_path)

        ctx = ssl.create_default_context(cafile=certifi.where())
        url = "https://raw.githubusercontent.com/ppatrzyk/polska-geojson/master/wojewodztwa/wojewodztwa-medium.geojson"
        with urllib.request.urlopen(url, context=ctx) as response:
            voivodeships_gdf = gpd.read_file(response)
        podkarpackie_border = voivodeships_gdf[voivodeships_gdf["nazwa"] == "podkarpackie"]
        podkarpackie_polygon = podkarpackie_border.geometry.unary_union
        filtered_localization_dict = {station: coordinates for station, coordinates in localization_dict.items() if Point(coordinates).within(podkarpackie_polygon)}
        save_localization_data(filtered_localization_dict, filtered_output_parquet_path)
    else:
        filtered_localization_dict = load_filtered_localization(filtered_output_parquet_path)
    
    filter_data_and_save(parquet_file_path, filtered_localization_dict, filtered_df_parquet_path)
