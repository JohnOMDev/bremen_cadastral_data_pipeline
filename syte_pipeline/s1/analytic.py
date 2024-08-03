#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 02 15:15:30 2024

@author: johnomole
"""

from fastapi import APIRouter, status
from concurrent.futures import ThreadPoolExecutor
from syte_pipeline.settings import Settings, DBCredentials
import os
import glob
import duckdb
from os.path import join
from syte_pipeline.src.ingestion import Extraction
from syte_pipeline.src.transformation import Transformer
from syte_pipeline.src.data_loader import DataLoader
import logging


logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("bremen state")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))


conn = duckdb.connect()
settings = Settings()

v1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Something is wrong with the request"
        },
    },
    prefix="/api/v1",
    tags=["v1"],
)
extraction_handler = Extraction()
transform_handler = Transformer()
db_credentials = DBCredentials()


default_db = f"user={db_credentials.user} host={db_credentials.host} password={db_credentials.password} port=5432"
data_loader_handler = DataLoader(default_db)


@v1.post("/cadastral/download")
async def download_bremen_state_data() -> str:
    """

    Returns
    -------
    str
        suuccess or error.

    """
    download_dir = os.path.join(settings.raw_dir, "day=20240801")
    os.makedirs(download_dir, exist_ok=True)
    zip_url = [
        "https://gdi2.geo.bremen.de/inspire/download/ADV-Shape/data/ALKIS_AdV_SHP_2024_04_HB.zip",
        "https://gdi2.geo.bremen.de/inspire/download/ADV-Shape/data/ALKIS_AdV_SHP_2024_04_BHV.zip",
    ]

    # We use parallelization to avoid wasting time
    with ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(extraction_handler.extract_specific_files, zip_url)

    return "OK"


@v1.post("/cadastral/prepare")
async def prepare_data() -> str:
    """

    Returns
    -------
    str
        suuccess or error.

    """
    raw_dir = os.path.join(settings.raw_dir, "day=20240801")
    file_map = {}
    for root, _, filenames in os.walk(raw_dir):
        sub_dir = os.path.basename(root)
        shp_files = [
            os.path.join(root, name)
            for name in filenames
            if os.path.splitext(name)[1].lower() == ".shp"
        ]

        if shp_files:
            file_map[sub_dir] = shp_files

    transform_handler.transform(file_map)
    return "OK"


@v1.post("/cadastral/analytics")
async def prepare_analytics() -> str:
    """

    Returns
    -------
    str
        suuccess or error.

    """
    prepared_file = os.path.join(settings.prepared_dir, "day=20240801")
    prepared_filenames = glob.glob(f"{prepared_file}/*.parquet")
    batch_size = 10
    batches = [
        (prepared_filenames[i : i + batch_size])
        for i in range(0, len(prepared_filenames), batch_size)
    ]

    data_loader_handler.create_db_objects()
    for batch in batches:
        data_loader_handler.export_building_parcel_data_to_psql(batch)
    logging.info("export ended")

    return "OK"

def read_prepared_sql() -> str:
    dir = join(settings.prepared_dir, "*", "*.parquet")
    return f"read_parquet('{dir}', hive_partitioning = 1, hive_types_autocast = 0)"

@v1.get("/cadastral/")
def list_cadastral(num_results: int = 100, page: int = 0) -> list[dict]:
    """
    List all the available cadastral, building and parcel order by district
    Parameters
    ----------
    num_results : int, optional
        DESCRIPTION. The default is 100.
    page : int, optional
        DESCRIPTION. The default is 0.

    Returns
    -------
    list[dict]
        DESCRIPTION.

    """
    res = duckdb.sql(
        f"""
    INSTALL spatial;
    LOAD spatial;
    INSTALL parquet;
    LOAD parquet;
    SET memory_limit = '5GB';
    SET threads TO 8;

    SELECT DISTINCT
        building_identifier,
        ST_AsText(ST_GeomFromWKB(geometry)) AS geometry,
        building_area,
        num_floors,
        on_parcel,
        type,
        building_date,
        parcel_identifier,
        location_text,
        parcel_area,
        cadastral_identifier,
        municipal,
        district
    FROM {read_prepared_sql()}
    ORDER BY district LIMIT {num_results} OFFSET {num_results * page}
    """,
    )
    return [{"building_identifier": building_identifier, "geometry": geometry, 
             "building_area": building_area,"num_floors": num_floors, 
             "n_parcel": n_parcel, "type": type,
             "building_date": building_date, "parcel_identifier": parcel_identifier, 
             "location_text": location_text, "parcel_area": parcel_area,
             "municipal": municipal, "district": district
             } for building_identifier, geometry, building_area,
            num_floors, n_parcel, type, building_date, parcel_identifier, location_text, parcel_area, cadastral_identifier,
            municipal, district in res.fetchall()]


@v1.get("/cadastral/{district}/building")
async def get_district_potential_building(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    pass


@v1.get("/cadastral/{icao}/stats")
async def get_aircraft_statistics(icao: str) -> dict:
    pass


@v1.get("/cadastral/{district}/building")
async def get_district_potential_building(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    pass


@v1.get("/cadastral/{icao}/stats")
async def get_aircraft_statistics(icao: str) -> dict:
    pass

