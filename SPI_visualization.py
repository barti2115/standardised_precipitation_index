import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import webbrowser
from branca.colormap import LinearColormap
import os


spi_adjusted_data = pd.read_parquet('data/parquet_files/spi_data.parquet', engine='pyarrow')
station_data = pd.read_parquet('data/localization/filtered_localization_data.parquet', engine='pyarrow')


def station_summary(station_name, spi_data):
    #print("SPI ADJUSTED DATA: ",spi_data)
    data = spi_data.loc[station_name]
    print(data.describe())

    spi_types = spi_data.columns
    fig, axes = plt.subplots(nrows=len(spi_types), figsize=(12, 6 * len(spi_types)))

    for ax, spi_type in zip(axes, spi_types):
        ax.plot(data.index.to_timestamp(), data[spi_type], label=spi_type)
        ax.set_title(f'{spi_type} Time Series for {station_name}')
        ax.set_ylabel(f'{spi_type} Value')
        ax.set_xlabel('Date')
        ax.legend()

    plt.suptitle(f'Correlation Between SPI Variants for {station_name}')
    sns.pairplot(data.dropna())
    plt.show()


def plot_all_stations(spi_data):
    grouped_data = spi_data.groupby(level=1)
    mean_spi = grouped_data.mean()

    spi_types = mean_spi.columns
    fig, axes = plt.subplots(nrows=len(spi_types), figsize=(12, 6 * len(spi_types)))

    for ax, spi_type in zip(axes, spi_types):
        ax.plot(mean_spi.index.to_timestamp(), mean_spi[spi_type], label=spi_type)
        ax.set_title(f'Mean {spi_type} Line Plot for All Stations')
        ax.set_ylabel(f'Mean {spi_type} Value')
        ax.set_xlabel('Date')
        ax.legend()

    plt.tight_layout()
    plt.show()




def calculate_average_spi(spi_data):
    average_spi = spi_data.mean()
    print("Average SPI", average_spi)
    return average_spi

def parse_coordinates(coord):
    if isinstance(coord, str):
        return [float(c) for c in coord.strip('[]').split(',')]
    elif isinstance(coord, list):
        return coord
    else:

        return coord.tolist()


def draw_spi_map(year, month, spi_index, spi_data, station_data):
    filtered_data = spi_data[
        (spi_data.index.get_level_values('Date').year == year) &
        (spi_data.index.get_level_values('Date').month == month)
        ]

    station_data[['longitude', 'latitude']] = station_data['Coordinates'].apply(parse_coordinates).tolist()

    merged_data = pd.merge(station_data, filtered_data[[spi_index]], on='Nazwa Stacji', how='right')
    print("MEREGERD",merged_data )
    # Initialize the Folium map centered around the mean location
    m = folium.Map(location=[merged_data['latitude'].mean(), merged_data['longitude'].mean()], zoom_start=6)

    spi_values = filtered_data[spi_index].dropna().values
    min_val, max_val = min(spi_values), max(spi_values)
    clm = plt.get_cmap('plasma')
    colors = [clm(i) for i in range(clm.N)]
    folium_color_map = LinearColormap(colors, vmin=min_val, vmax=max_val).to_step(n=10)
    m.add_child(folium_color_map)

    for idx, row in merged_data.iterrows():
        value = row[spi_index]
        color = folium_color_map(value)
        folium.CircleMarker(
            location=(row['latitude'], row['longitude']),
            radius=5,
            weight=1,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=1,
            popup=f"{row['Nazwa Stacji']}: {spi_index} {row[spi_index]}"
        ).add_to(m)

    map_path = 'data/spi_map.html'
    m.save(map_path)
    auto_open(map_path)

    return m


def auto_open(path):
    try:
        webbrowser.open('file://' + os.path.realpath(path))
    except webbrowser.Error:
        print(f"Failed to automatically open the web browser. Please open the file manually at: {path}")


def main(spi_data, station_data):
    station_summary('CISNA', spi_adjusted_data)
    plot_all_stations(spi_adjusted_data)
    calculate_average_spi(spi_adjusted_data)
    while True:
        try:
            years = sorted(spi_data.index.get_level_values('Date').year.unique())
            months = sorted(spi_data.index.get_level_values('Date').month.unique())
            spi_indices = ['SPI-1', 'SPI-3', 'SPI-12']

            print("Available years:", years)
            year = input("Enter a year for SPI visualization or type 'exit' to quit: ")
            if year == 'exit':
                break
            year = int(year)
            print("Available months:", months)
            month = int(input("Enter a month for SPI visualization: "))
            print("Available SPI indices:", spi_indices)
            spi_index = input("Enter SPI index for visualization (SPI-1, SPI-3, SPI-12): ")

            if year not in years or month not in months or spi_index not in spi_indices:
                print("Invalid input. Please try again.")
                continue

            m = draw_spi_map(year, month, spi_index, spi_data, station_data)
        except Exception as e:
            print(f"An error occurred: {e}")



if __name__ == "__main__":
    main(spi_adjusted_data, station_data)


