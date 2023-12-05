import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

def scrape_digital_coast_repo():
    """
    Scrapes the digital coast repository and saves the data as a Parquet file.
    """
    url = "https://coast.noaa.gov/htdata/lidar1_z/"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html5lib")

    # Generate time
    update_text = soup.find('p', text=lambda t: t and t.startswith('Table updated')).string
    # remove EST or EDT reference
    date = update_text.split('Table updated ')[1].replace(' EDT', '').replace(' EST', '').replace('\n', '')
    date_object = datetime.strptime(date, '%a %b %d %H:%M:%S %Y')

    # output filename
    parquet = f'data/noaa_dav_{date_object.date()}.parquet'

    if Path(parquet).is_file():
        return pd.read_parquet(parquet)
    else:
        table = soup.find_all("table")[0]
        headers = [th.text for th in table.find_all("th")]

        rows = []
        for tr in table.find_all("tr")[1:]:
            row = [td for td in tr.find_all("td")]
            for i in range(12):
                if row[i].text.strip():
                    if i in [0, 1, 2, 4, 11]:
                        row[i] = row[i].text.strip()
                    else:
                        row[i] = row[i].find('a').get('href')
            rows.append(row)

        digital_coast_repo = pd.DataFrame(rows, columns=headers)

        digital_coast_repo['Metadata'] = digital_coast_repo['Metadata'].astype(str)
        digital_coast_repo['EPT'] = digital_coast_repo['EPT'].astype(str)
        digital_coast_repo['Potree'] = digital_coast_repo['Potree'].astype(str)
        digital_coast_repo['footprint'] = digital_coast_repo['footprint'].astype(str)
        digital_coast_repo['Tile Index'] = digital_coast_repo['Tile Index'].astype(str)

        digital_coast_repo = digital_coast_repo.replace(to_replace=[r'b?\'?<td>.*?</td>\'?', r'<td>.*?</td>'], value=pd.NA, regex=True)

        digital_coast_repo.to_parquet(parquet)
        return digital_coast_repo
