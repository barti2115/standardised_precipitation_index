import os
import urllib.request
import zipfile
from bs4 import BeautifulSoup
import certifi
import ssl

def get_links(url):
    # Using certifi to provide trusted certificate authorities
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    response = urllib.request.urlopen(url, context=ssl_context)
    soup = BeautifulSoup(response.read(), 'html.parser')
    links = [link.get("href") for link in soup.find_all("a")]
    return links

def download(url, save_dir="."):
    # Using certifi to provide trusted certificate authorities
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(url, context=ssl_context) as response:
        if response.getcode() == 200:
            filename = os.path.basename(url)
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'wb') as f:
                f.write(response.read())
            print(f"File '{filename}' downloaded successfully.")
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(save_dir)
            print("ZIP file contents extracted successfully.")
            os.remove(filepath)
            print(f"File '{filename}' removed successfully.")
        else:
            print("Failed to download the file.")

if __name__ == "__main__":
    data_url = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/opad/"
    links = get_links(data_url)
    output_data_path = "data/precipitation"
    print(links)
    os.makedirs(output_data_path, exist_ok=True)

    for link in links:
        if link[:4].isdigit():
            temp = data_url + link
            zip_files_by_year = get_links(temp)
            
            for zip_link in zip_files_by_year:
                if zip_link.endswith(".zip"):
                    zip_url = temp + zip_link
                    year = link[:4]
                    year_path = os.path.join(output_data_path, year)

                    if not os.path.exists(year_path):
                        os.makedirs(year_path)

                    print(zip_url)
                    try:
                        download(zip_url, year_path)
                    except zipfile.BadZipFile:
                        print(f"Error: Bad CRC-32 for file '{zip_link}'. Skipping.")

