#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 02 18:40:21 2024

@author: johnomole
"""
from syte_pipeline.settings import Settings
import os
import duckdb
import shutil
import logging
import geopandas as gpd

logger = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

conn = duckdb.connect()
settings = Settings()


class Transformer:
    def read_shapefiles_file(self, filename: str):
        """

        Parameters
        ----------
        filename : filename
            Str.

        Returns
        -------
        df_wg4326 : gpd.GeoDataFrame
            GeoDataFrame containing the data.

        """
        gpd_df = gpd.read_file(filename)
        df_wg4326 = self.convert_crs(gpd_df)
        return df_wg4326

    @staticmethod
    def convert_crs(df):
        """
        Convert goemetry to 4326
        Parameters
        ----------
        df : gpd.GeoDataFrame
            GeoDataFrame containing the data.

        Returns
        -------
        df_wg4326 : gpd.GeoDataFrame
            A GeoDataFrame converted to epsg=4326.

        """
        df["area"] = df.area
        df_wg4326 = df.to_crs(epsg=4326)
        return df_wg4326

    def spatial_join(
        self, df_building: gpd.GeoDataFrame, df_parcel: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Perform a spatial join between building and parcel GeoDataFrames.
        Parameters
        ----------
        df_building : gpd.GeoDataFrame
            GeoDataFrame containing building data.
        df_parcel : gpd.GeoDataFrame
            GeoDataFrame containing parcel data.

        Returns
        -------
        df : TYPE
            A GeoDataFrame resulting from the spatial join with renamed columns.

        """
        try:
            gdf_join_df = gpd.sjoin(
                df_building, df_parcel, how="inner", predicate="within"
            )
        except Exception as e:
            LOG.error(f" Error during spatial join: {e}")
            raise
        try:
            df = gdf_join_df[
                [
                    "OID_left",
                    "OID_right",
                    "geometry",
                    "area_left",
                    "ANZAHLGS",
                    "IDFLURST",
                    "FLAECHE",
                    "LAGEBEZTXT_right",
                    "FLSTKENNZ",
                    "GEMARKUNG",
                    "GEMEINDE",
                    "AKTUALIT_left",
                    "FUNKTION",
                ]
            ].copy()
            df = df.rename(
                columns={
                    "OID_left": "building_identifier",
                    "geometry": "geometry",
                    "area_left": "building_area",
                    "ANZAHLGS": "num_floors",
                    "IDFLURST": "on_parcel",
                    "OID_right": "parcel_identifier",
                    "geometry": "geometry",
                    "FLAECHE": "parcel_area",
                    "LAGEBEZTXT_right": "location_text",
                    "FLSTKENNZ": "cadastral_identifier",
                    "GEMARKUNG": "district",
                    "GEMEINDE": "municipal",
                    "AKTUALIT_left": "building_date",
                    "FUNKTION": "type",
                }
            )
        except KeyError as e:
            print(f"Column error during renaming: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise
            return df

    def transform(self, file_map) -> None:
        output_dir = os.path.join(settings.prepared_dir, "day=20240801")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        for k, file_paths in file_map.items():
            dfs = {}
            for file_path in file_paths:
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                if file_name in ["GebaeudeBauwerk", "Flurstueck"]:
                    try:
                        dfs[file_name] = self.read_shapefiles_file(file_path)
                    except Exception as e:
                        LOG.error(f"Error reading shapefile {file_path}: {e}")
                    continue

            df_building = dfs.get("GebaeudeBauwerk")
            df_parcel = dfs.get("Flurstueck")
            if df_building is not None and df_parcel is not None:
                try:
                    df_spatial = self.spatial_join(df_building, df_parcel)
                    self.to_parquet(df_spatial, output_dir)
                except Exception as e:
                    LOG.error(f"Error during spatial join or parquet conversion: {e}")

    def to_parquet(self, df_spatial: gpd.GeoDataFrame, output_dir: str) -> None:
        """
        Save spatial data to parquet files, one per district.
        Parameters
        ----------
        df_spatial : gpd.GeoDataFrame
            GeoDataFrame containing spatial data.
        output_dir : str
            Directory where parquet files will be saved.

        Returns
        -------
        None

        """

        try:
            for district, group in df_spatial.groupby("district"):

                output_file = os.path.join(output_dir, f"{district}.parquet")

                group.to_parquet(output_file, engine="pyarrow", index=False)

                LOG.info(f"Saved partition for district {district} to {output_file}")
        except Exception as e:
            LOG.error(f"Error saving parquet files: {e}")
