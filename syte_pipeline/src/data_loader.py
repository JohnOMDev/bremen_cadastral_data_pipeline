from syte_pipeline.settings import Settings
import os
import psycopg
import duckdb
import logging
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

conn = duckdb.connect()
settings = Settings()


class DataLoader:

    def __init__(self, default_db):
        self.db_config = default_db

    def get_pg_conn(self):

        conn = psycopg.connect(self.db_config)
        cur = conn.cursor()
        return conn, cur

    def create_db_objects(self) -> None:
        """

        Parameters
        ----------
        default_db : str
            DESCRIPTION.

        Returns
        -------
        None
            DESCRIPTION.

        """
        conn, cur = self.get_pg_conn()
        try:
            cur.execute(
                """
                CREATE EXTENSION IF NOT EXISTS postgis;

                CREATE TABLE IF NOT EXISTS buildings (
                    identifier VARCHAR PRIMARY KEY,
                    geometry GEOMETRY(MULTIPOLYGON, 4326),
                    area FLOAT,
                    num_floors INTEGER,
                    on_parcel VARCHAR,
                    type VARCHAR,
                    building_date date,
                    fast_api_sync timestamp without time zone default (now() at time zone 'utc')
                );

                CREATE TABLE IF NOT EXISTS parcels (
                    identifier VARCHAR PRIMARY KEY,
                    geometry GEOMETRY(MULTIPOLYGON, 4326),
                    area FLOAT,
                    location_text VARCHAR,
                    cadastral_identifier VARCHAR,
                    district VARCHAR,
                    municipal VARCHAR,
                    fast_api_sync timestamp without time zone default (now() at time zone 'utc')
                );

            """
            )
            conn.commit()
        except BaseException:
            conn.rollback()

    def insert_data_into_buildings(self, building_data):
        try:
            conn, cur = self.get_pg_conn()

            insert_building_query = """
                INSERT INTO buildings (identifier, geometry, area, num_floors, on_parcel, type, building_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (identifier)
                DO UPDATE SET
                    geometry = EXCLUDED.geometry,
                    area = EXCLUDED.area,
                    num_floors = EXCLUDED.num_floors,
                    on_parcel = EXCLUDED.on_parcel,
                    type = EXCLUDED.type,
                    building_date = EXCLUDED.building_date,
                    fast_api_sync = current_timestamp;
            """

            if building_data:
                cur.executemany(insert_building_query, building_data)
                LOG.info(f"Inserting {len(building_data)} rows into buildings.")
                conn.commit()
                LOG.info("Buildings data successfully committed.")

        except Exception as e:
            LOG.error(f"Error inserting buildings data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def insert_data_into_parcels(self, parcel_data):
        try:
            conn, cur = self.get_pg_conn()

            insert_parcel_query = """
                INSERT INTO parcels (identifier, geometry, area, location_text, cadastral_identifier, district, municipal)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (identifier)
                DO UPDATE SET
                    geometry = EXCLUDED.geometry,
                    area = EXCLUDED.area,
                    location_text = EXCLUDED.location_text,
                    cadastral_identifier = EXCLUDED.cadastral_identifier,
                    district = EXCLUDED.district,
                    municipal = EXCLUDED.municipal,
                    fast_api_sync = current_timestamp;
            """

            if parcel_data:
                cur.executemany(insert_parcel_query, parcel_data)
                LOG.info(f"Inserting {len(parcel_data)} rows into parcels.")
                conn.commit()
                LOG.info("Parcels data successfully committed.")
        except Exception as e:
            LOG.error(f"Error inserting parcels data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def export_building_parcel_data_to_psql(self, _file_dir: str) -> None:

        try:
            LOG.info(f"Processing file: {_file_dir}")

            # Read parquet data using DuckDB
            data = duckdb.sql(
                f"""
                INSTALL spatial;
                LOAD spatial;
                INSTALL parquet;
                LOAD parquet;
                SET memory_limit = '25GB';
                SET threads TO 8;

                SELECT
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
                FROM read_parquet({_file_dir});
                """
            ).fetchall()

            LOG.info("Data successfully fetched from parquet.")

            if not data:
                LOG.warning("No data fetched from DuckDB.")
                return

            # Prepare data for insertion
            building_data = [
                (row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in data
            ]
            parcel_data = [
                (row[7], row[1], row[9], row[8], row[10], row[11], row[12])
                for row in data
            ]

            # Insert data into buildings table
            self.insert_data_into_buildings(building_data)

            # Insert data into parcels table
            self.insert_data_into_parcels(parcel_data)

        except Exception as e:
            LOG.error(f"An error occurred: {e}")
