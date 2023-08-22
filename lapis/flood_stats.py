import pandas as pd
import numpy as np
import requests

def format_rating_data(df):
    df = df[0]
    rename_columns = {
        'INDEP': 'stage_ft',
        'DEP': 'flow_cfs'
    }
    print(f"columns renamed: {rename_columns}")
    df = df.rename(columns=rename_columns)
    return df

class PeakFlowStatistics:
    def __init__(self, gage_id):
        self.gage_id = gage_id

    def generate_url_string(self):
        """
        Generate URL strings for HTTP requests to SS API.
        """
        static = "https://streamstats.usgs.gov/gagestatsservices/statistics"

        station_id = f"stationIDOrCode={self.gage_id}"
        stats = f"statisticGroups=2"

        url = f"{static}?{station_id}&{stats}"

        return url

    def send_http_request(self):
        """
        Send HTTP requests to SS API and get JSON responses.
        """

        info = requests.get(self.url)

        if info.status_code == 200:
            return info.json()
        elif info.status_code == 400:
            raise Exception(f"Request failed. {info.status_code} - Bad request")
        elif info.status_code == 401:
            raise Exception(f"Request failed. {info.status_code} - Unauthorized")
        elif info.status_code == 500:
            raise Exception(f"Request failed. {info.status_code} - Internal error")

    def process(self):
        self.url = self.generate_url_string()
        self.json = self.send_http_request()

        df = pd.json_normalize(self.json)

        rename_columns = {
            'regressionType.code': 'pfs_aep_code',
            'regressionType.name': 'pfs_aep_name',
            'value' : 'pfs_flow_cfs',
        }

        print(f'Columns rename: {rename_columns}')

        df = df.rename(columns=rename_columns)

        return df
        
class FlowToStage :
    def __init__(self, peak_flow_stats, rating_curve):
        self.peak_flow_stats = peak_flow_stats
        self.rating_curve = rating_curve

    def _find_closest_flow(self, value):
        # use index of flow value with smallest absolute difference
        closest = (self.rating_curve['flow_cfs']-value).abs().idxmin()
        return self.rating_curve.loc[closest, ['stage_ft', 'flow_cfs']]

    def find_stage_associated_with_aep_flow(self):
        self.peak_flow_stats[['rc_stage_ft', 'rc_flow_cfs']] = self.peak_flow_stats['pfs_flow_cfs'].apply(self._find_closest_flow)

        self.peak_flow_stats['pfs_flow_cms'] = self.peak_flow_stats['pfs_flow_cfs'] / 35.3147 # cubic feet to cubic meters
        self.peak_flow_stats['rc_flow_cms'] = self.peak_flow_stats['rc_flow_cfs'] / 35.3147 # cubic feet to cubic meters

        self.peak_flow_stats['rc_stage_m'] = self.peak_flow_stats['rc_stage_ft'] * 0.3048 # feet to meters
        
        return self.peak_flow_stats
