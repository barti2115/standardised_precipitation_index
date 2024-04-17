#!/bin/bash

# Run download.py
python3 download.py
echo "Stage Download completed!"

# Run read_data.py
python3 read_data.py
echo "Stage Read Data completed!"

# Run filter_and_map_stations.py
python3 filter_and_map_stations.py
echo "Stage Filter and Map Stations completed!"

# Run process_filtered_stations_data.py
python3 process_filtered_stations_data.py
echo "Stage Process Filtered Stations Data completed!"
