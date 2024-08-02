#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 18:40:21 2023

@author: johnomole
"""
from syte_pipeline.settings import Settings
import requests
import os
import zipfile
import io
import duckdb
import logging

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

conn = duckdb.connect()
settings = Settings()


class Extraction:

    def extract_shapefiles__zip(self, url):
        """
        Download the ZIP file from a URL.

        :param url: URL of the ZIP file
        :return: BytesIO object containing the ZIP file content
        """
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return io.BytesIO(response.content)

    def extract_specific_files(
        self,
        url,
        download_dir=os.path.join(settings.raw_dir, "day=20240801"),
        file_prefix={"Flurstueck", "GebaeudeBauwerk"},
    ):
        """
        Extract specific files from a ZIP archive given as a BytesIO object.

        :param zip_file_like: BytesIO object containing the ZIP file
        :param extract_to: Directory where files will be extracted
        :param file_names: Set of filenames to extract
        """
        zip_file_like = self.extract_shapefiles__zip(url)
        with zipfile.ZipFile(zip_file_like, "r") as zip_ref:

            for file_info in zip_ref.infolist():

                file_name, file_ext = os.path.splitext(
                    os.path.basename(file_info.filename)
                )

                if file_name in file_prefix:

                    zip_ref.extract(file_info, download_dir)
                    print(f"Extracted: {file_info.filename} inot {download_dir}")
