# Project: SPI Calculation for Stations in Podkarpackie Voivodeship

## Purpose
The purpose of this project is to calculate Standardized Precipitation Index (SPI) for meteorological stations located in the Podkarpackie Voivodeship, Poland.

## Setup

### Installation
1. Clone the repository:
    ```bash
    git clone <repository_url>
    cd <repository_name>
    ```

2. Create a virtual environment (recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # Activate virtual environment (Linux/macOS)
    # OR
    .\venv\Scripts\activate   # Activate virtual environment (Windows)
    ```

3. Install required packages using pip:
    ```bash
    pip install -r requirements.txt
    ```

### Data Preparation
1. Run the `prepare_data.sh` script:
    ```bash
    bash prepare_data.sh
    ```

### Generated Data
- **Filtered Localization Data**: 
    - Location: `data/localization`
    - Contains filtered (Podkarpackie) and unfiltered (all stations) data in a Parquet file format. Each record includes the station name and its coordinates.

- **Filtered Data for Podkarpackie Voivodeship**: 
    - Location: `data/parquet_files/filtered_data`
    - Contains combined data for all stations in the Podkarpackie Voivodeship in Parquet format.

- **Unfiltered Data**: 
    - Location: `data/parquet_files/unfiltered_data`
    - Contains combined data for all stations included in the dataset in Parquet format.

- **Station Time Ranges**:
    - Location: `data/parquet_files/station_time_ranges`
    - Contains time ranges for every station's data in the Podkarpackie Voivodeship in Parquet format.

- **Station Data**: 
    - Location: `data/parquet_files/station_data`
    - Contains data from every station in the Podkarpackie Voivodeship, stored in different Parquet files.

- **Precipitation Data**:
    - Location: `data/precipation`
    - Contains CSV files downloaded from [IMGW](https://danepubliczne.imgw.pl/) website, organized into directories named after the year of the data.
### EDA and SPI
1. Run the `analysis.sh` script:
    ```bash
    bash analysis.sh
    ```
    Run scripts in Python performing EDA analysis, SPI calculation and visualization of results.