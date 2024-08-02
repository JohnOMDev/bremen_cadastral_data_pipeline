#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 15:15:30 2023

@author: johnomole
"""

from fastapi import APIRouter, status
from concurrent.futures import ThreadPoolExecutor
from syte_pipeline.settings import Settings, DBCredentials
import os
import glob
import duckdb
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


@v1.post("/shapefiles/download")
async def download_bremen_state_data() -> str:
    """ """
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


@v1.post("/shapefiles/prepare")
async def prepare_data() -> str:
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


@v1.post("/shapefiles/analytics")
async def prepare_analytics() -> str:
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
