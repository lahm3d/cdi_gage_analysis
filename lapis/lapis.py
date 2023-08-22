import geopandas as gpd
import pandas as pd
import numpy as np
import re
from shapely.geometry import shape, Point, Polygon
import requests
import json
import subprocess
import pyproj
import pdal

class SearchLidarInventory:
    """
    A class for searching lidar inventory using various locations.

    Args:
        locations (list): A list of dictionaries representing different locations.

    Attributes:
        locations (list): A list of dictionaries representing different locations.
        wgs (int): EPSG code for the WGS84 coordinate system.
        albers (int): EPSG code for the Albers Equal Area coordinate system.
        mercator (int): EPSG code for the Mercator coordinate system.

    """

    def __init__(self, locations):
        """
        Initialize the SearchLidarInventory object.

        Args:
            locations (list): A list of dictionaries representing different locations.

        """
        self.locations = locations
        self.wgs = 4326
        self.albers = 5070
        self.mercator = 3857

    def convert_to_esri_geometry(self, geom):
        """
        Convert a Shapely geometry object to Esri geometry JSON format.

        Args:
            geom (shapely.geometry): A Shapely geometry object.

        Returns:
            esri (str): The Esri geometry JSON representation.

        """
        rings = None
        # test for polygon type
        if geom.geom_type == 'MultiPolygon':
            rings = []
            for pg in geom.geoms:
                rings += [list(pg.exterior.coords)] + [list(interior.coords) for interior in pg.interiors]
        elif geom.geom_type == 'Polygon':
            rings = [list(geom.exterior.coords)] + [list(interior.coords) for interior in geom.interiors]
        else:
            print("Shape is not a polygon")
            return None

        # convert to Esri geometry JSON
        esri = json.dumps({'rings': rings})
        return esri

    def generate_geometries(self):
        """
        Generate GeoDataFrames with geometries based on the provided locations.

        Returns:
            gdf_mercator (GeoDataFrame): A GeoDataFrame containing the generated geometries in the Mercator projection.

        """

        gdfs = []
        for location in self.locations:
            if location['data_type'] == 'coords':
                attributes = {key: location[key] for key in ['name', 'buffer']}
                gdf = gpd.GeoDataFrame([attributes], geometry=[Point(location['value'])], crs=f'EPSG:{self.wgs}')
                try:
                    buffer = attributes['buffer']
                except KeyError:
                    # Buffer key error handling
                    buffer = 50  # If no buffer specified, assume 50 meters as default

                # Convert to Albers for buffering
                gdf_albers = gdf.to_crs(self.albers)

                # Generate 50-meter radius buffer
                gdf_albers['geometry'] = gdf_albers['geometry'].buffer(buffer)

                gdf = gdf_albers.to_crs(self.wgs)
                gdfs.append(gdf)
            elif location['data_type'] == 'file':
                gdf = gpd.read_file(location['value'])
                gdf['name'] = location['name']
                gdfs.append(gdf[['name', 'geometry']])
            elif location['data_type'] == 'bbox':
                bbox = location['value']
                attributes = {key: location[key] for key in ['name']}
                geometry = [Polygon([(bbox[0], bbox[1]), (bbox[2], bbox[1]), (bbox[2], bbox[3]), (bbox[0], bbox[3])])]
                gdf = gpd.GeoDataFrame([attributes], geometry=geometry, crs=self.wgs)
                gdfs.append(gdf)

        gdf_wgs = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
        gdf_mercator = gdf_wgs.to_crs(self.mercator)

        # Force single-part geometries
        gdf_mercator = gdf_mercator.explode(index_parts=False)

        # pass single part geometries to wgs gdf
        gdf_wgs = gdf_mercator.to_crs(self.wgs)

        gdf_mercator['esri_geometry'] = gdf_mercator['geometry'].apply(self.convert_to_esri_geometry)


        return gdf_wgs, gdf_mercator

    def make_post_request(self, gdf):
        """
        Make POST requests to NOAA's USIEIv2 MapServer to retrieve lidar collections.

        Args:
            gdf (GeoDataFrame): A GeoDataFrame containing the generated geometries.

        Returns:
            gdf_collection (GeoDataFrame): A GeoDataFrame containing the lidar collections.

        Raises:
            Exception: If the HTTP request fails with a status code indicating an error.

        """

        locations = gdf[['name', 'esri_geometry']].to_dict(orient='records')
        payload = {
            "outFields": "*",
            "geometry": "",
            "geometryType": "esriGeometryPolygon",
            "spatialRel": "esriSpatialRelIntersects",
            "returnGeometry": 'true',
            'returnIdsOnly': 'false',
            "f": "geojson"
        }

        urls = [
            "https://coast.noaa.gov/arcgis/rest/services/USInteragencyElevationInventory/USIEIv2/MapServer/2/query",
            "https://coast.noaa.gov/arcgis/rest/services/USInteragencyElevationInventory/USIEIv2/MapServer/0/query",
        ]

        gdfs = []

        for url in urls:
            for location in locations:
                payload.update({'geometry': f"{location['esri_geometry']}"})
                response = requests.post(url, data=payload)

                # Check the response status
                if response.status_code == 200:
                    # J = response.json()
                    # G = gpd.GeoDataFrame.from_features(J['features'])
                    # print(dir(response))
                    json = response.json()
                    gdf = gpd.GeoDataFrame.from_features(json['features'])
                    if not gdf.empty:
                        gdf = gdf.set_geometry(gdf['geometry'].apply(shape))
                        gdf.crs = f"EPSG:{self.wgs}"
                        gdf['name'] = location['name']
                        gdfs.append(gdf)
                elif response.status_code == 400:
                    raise Exception(f"Request failed. {response.status_code} - Bad request")
                elif response.status_code == 401:
                    raise Exception(f"Request failed. {response.status_code} - Unauthorized")
                elif response.status_code == 500:
                    raise Exception(f"Request failed. {response.status_code} - Internal error")

        gdf_collection = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=self.wgs)

        return gdf_collection

    def get_lidar_collections(self):
        """
        Get lidar collections based on the provided locations.

        Returns:
            location_gdf (GeoDataFrame): A GeoDataFrame containing the generated geometries.
            lidar_collections (GeoDataFrame): A GeoDataFrame containing the lidar collections.

        """

        locs_wgs, locs_mercator = self.generate_geometries()
        lidar_collections = self.make_post_request(locs_mercator)

        return locs_wgs, lidar_collections

class ProcessLidarCollections:
    """
    A class to process lidar collections.

    Args:
        collections (DataFrame): The lidar collections data.
        noaa_dav_gdf (str): The file path to the digital coast repository Parquet file.

    Attributes:
        collections (DataFrame): The lidar collections data.
        noaa_dav_gdf (DataFrame): The digital coast repository data loaded as a DataFrame.

    """

    def __init__(self, collections, locations, noaa_dav_gdf):
        self.collections = collections
        self.locations = locations
        self.noaa_dav_gdf = noaa_dav_gdf

    def _parse_ept_from_usiaei(self, x):
        """
        Parse the EPT URL from a JSON string.

        Args:
            x (str): The JSON string.

        Returns:
            str or np.nan: The extracted EPT URL or np.nan if not found.

        """
        if x is None:
            return None

        try:
            links = json.loads(x)
            values = [l['link'] for l in links['links'] if l.get('linktype') == 'EPT Link']
            if values:
                return values[0]
            else:
                return np.nan
        except (json.JSONDecodeError, TypeError):
            return np.nan

    def _parse_noaa_lidar_id(self, x):
        """
        Parse the NOAA Lidar ID from a JSON string.

        Args:
            x (str): The JSON string.

        Returns:
            str or np.nan: The extracted NOAA Lidar ID or np.nan if not found.

        """
        if x is None:
            return None

        try:
            links = json.loads(x)
            values = [l['link'] for l in links['links'] if l['label'] == "NOAA Digital Coast" and l['linktype'] == "Data Access"]
            if values:
                match = re.search(r'ID=(\d+)', values[0])
                if match:
                    return match.group(1)
            else:
                np.nan
        except (json.JSONDecodeError, TypeError):
            return np.nan

    def _get_ept_url(self, x):
        """
        get the EPT URL.

        Args:
            x (dict): The input dictionary containing 'ept_usiaei' and 'EPT' keys.

        Returns:
            str or np.nan: The reconciled EPT URL or np.nan if reconciliation is not possible.

        """
        url = list(set([x['ept_usiaei'], x['EPT']]))
        url = [i for i in url if not pd.isnull(i)]  # Drop np.NaN
        if len(url) == 1:
            return url[0]
        else:
            return np.nan

    def _get_ept_epsg(self, url):
        """
        Get the EPSG code from an EPT URL.

        Args:
            url (str): The EPT URL.

        Returns:
            int: The EPSG code if successful, or 0 if unsuccessful.

        """
        if isinstance(url, str):
            try:
                # Define the command
                cmd = ['pdal', 'info', '--summary', url]

                # Run the command and capture the output
                output = subprocess.check_output(cmd, universal_newlines=True)

                # Parse the JSON output
                metadata = json.loads(output)

                # Extract the WKT string from the metadata
                wkt_string = metadata['summary']['srs']['wkt']

                # Convert the WKT string to EPSG code
                if wkt_string:
                    epsg = pyproj.CRS.from_string(wkt_string).to_epsg()
                    return int(epsg)
            except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError):
                pass

        return 4326

    def _add_ept_geometries_as_gdf(self):
        """
        Adds the reprojected location geometry to the 'ept_gdf' column in the collections GeoDataFrame based on a common 'name' field.

        Args:
            collections (GeoDataFrame): The collections GeoDataFrame.
            locations (GeoDataFrame): The locations GeoDataFrame.

        Returns:
            GeoDataFrame: The updated collections GeoDataFrame with the 'ept_gdf' column.

        """
        self.collections['ept_gdf'] = ""
        names = self.locations.name.values

        for name in names:
            location = self.locations[self.locations.name == name]
            location = location[['geometry']]
            mask = self.collections.name == name
            subset = self.collections[mask]
            crss = subset.ept_crs.to_list()
            
            ept_gdfs = [location.to_crs(crs) for crs in crss]
            subset['ept_gdf'] = ept_gdfs
            self.collections.loc[mask] = subset

        return self.collections

    def execute(self):
        """
        Process the lidar collection data.

        Returns:
            DataFrame: The processed data as a DataFrame.

        """
        self.collections['ept_usiaei'] = self.collections['Links'].apply(self._parse_ept_from_usiaei)
        self.collections['noaa_id'] = self.collections['Links'].apply(self._parse_noaa_lidar_id)
        self.collections = pd.merge(self.collections, self.noaa_dav_gdf[['ID #', "EPT"]], left_on="noaa_id", right_on="ID #", how='left')
        self.collections['ept'] = self.collections.apply(self._get_ept_url, axis=1)
        self.collections.rename(columns={'EPT': 'ept_noaa'}, inplace=True)
        self.collections['ept_crs'] = self.collections['ept'].apply(self._get_ept_epsg)
        self.collections = self._add_ept_geometries_as_gdf()

        return self.collections


def fetch_lidar(gdf, geometry_method, write=False, out_las="demo_data/demo_lidar.las"):
    """
    Fetches lidar data using an EPT URL from a GeoDataFrame.

    Args:
        gdf (GeoDataFrame): GeoDataFrame containing the EPT URL and EPT GeoDataFrame.
        geometry_method (str): Method for defining the region of interest: 'bounds' or 'polygon'.
        write (bool, optional): Whether to write the lidar data to a LAS file. Defaults to False.
        out_las (str, optional): Output LAS file name if write is True. Defaults to "demo_lidar.las".

    Returns:
        tuple: A tuple containing the count, arrays, metadata, and output LAS file name (if write is True) of the lidar data.

    Raises:
        Exception: If the GeoDataFrame does not have an EPT URL.

    """

    ept_url = gdf.ept.to_list()

    if not ept_url:
        raise Exception("The collection does not have an EPT url. Exiting...")
        return
    else:
        ept_url = ept_url[0]
        pipeline = {
            "type": "readers.ept",
            "filename": ept_url,
            "tag": "readdata",
        }

        # unnest the ept_gdf
        ept_gdf = gdf.ept_gdf.item()

        if geometry_method == 'bounds':
            minx, miny, maxx, maxy = ept_gdf.total_bounds.tolist()
            pipeline['bounds'] = f"([{minx}, {maxx}], [{miny}, {maxy}])"
        elif geometry_method == 'polygon':
            pipeline['polygon'] = ept_gdf.geometry.to_wkt()[0]


        if write:
            write_las_pipeline = {
                "type": "writers.las",
                "filename": out_las,
            }
            pipeline = [pipeline, write_las_pipeline]
            print(f'LAS write output enabled. Saving file under {out_las}')
        else:
            pipeline = [pipeline]

        pipeline = pdal.Pipeline(json.dumps(pipeline))
        count = pipeline.execute()
        arrays = pipeline.arrays
        metadata = pipeline.metadata

        return count, arrays, metadata, out_las