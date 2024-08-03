#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 02 18:40:21 2024

@author: johnomole
"""
from syte_pipeline.settings import Settings
from typing import Set
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

    def extract_shapefiles__zip(self, url: str)-> io.BytesIO:
        """
        Download the ZIP file from a URL.
        Parameters
        ----------
        url : str
            URL of the ZIP file.

        Returns
        -------
        TYPE
            BytesIO object containing the ZIP file content.

        """
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        return io.BytesIO(response.content)

    def extract_specific_files(
        self,
        url: str,
        download_dir: str = os.path.join(settings.raw_dir, "day=20240801"),
        file_prefix: Set[str] = {"Flurstueck", "GebaeudeBauwerk"},
    ) -> None:
        """
        Extract specific files from a ZIP archive given as a URL.
        Parameters
        ----------
        url : str
            URL pointing to the ZIP file.
        download_dir : str, optional
            Directory where files will be extracted. The default is os.path.join(settings.raw_dir, "day=20240801").
        file_prefix : Set[str], optional
            Set of filenames (without extension) to extract. The default is {"Flurstueck", "GebaeudeBauwerk"}.
        Returns
        -------
        None
        """
        try:
            zip_file_like = self.extract_shapefiles__zip(url)
            with zipfile.ZipFile(zip_file_like, "r") as zip_ref:
                for file_info in zip_ref.infolist():
                    file_name, file_ext = os.path.splitext(
                        os.path.basename(file_info.filename)
                    )

                    if file_name in file_prefix:
                        try:
                            zip_ref.extract(file_info, download_dir)
                            print(
                                f"Extracted: {file_info.filename} into {download_dir}"
                            )
                        except Exception as e:
                            print(f"Error extracting {file_info.filename}: {e}")

        except Exception as e:
            print(f"Error processing ZIP file from URL {url}: {e}")
