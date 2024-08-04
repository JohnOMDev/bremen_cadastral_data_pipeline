# bremen_cadastral_data_pipeline
[![Fast API test and lint](https://github.com/JohnOMDev/bremen_cadastral_data_pipeline/actions/workflows/fastapi-app.yml/badge.svg)](https://github.com/JohnOMDev/bremen_cadastral_data_pipeline/actions/workflows/fastapi-app.yml)
[![Security: Bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Geospatial Data Pipeline

This project is a pipeline for ingesting, transforming, and visualizing geospatial data. It reads data from shapefiles, processes it, exports it to both a database and the file system for analysis, calculates the potential for new buildings, and visualizes the results.

## Project Structure

- `.github`: Contains workflows for continous integration (CI).
- `docker`: Directory with the Dockerfile for building the container image, Docker Compose files, and environment variables.
- `scripts`: Contains SQL queries for creating objects required for analytical processing.
- `syte_pipeline`: Main project directory housing the application code, including ingestion, transformation, and data loader in src, and analytics in s1.
- `test`: Contains unit tests and other test-related files.
- `poetry.lock`: Lock file for managing project dependencies.
- `Makefile`: Defines commands for automating tasks.
- `pyproject.toml`: Configuration file for project dependencies and settings.
- `README.md`: Provides an overview and documentation for the project.

## Setup

### Prerequisites

- Docker
- Docker Compose

### Installation

1. **Use Python 3.11**:
   Ensure you have Python 3.11 installed.

2. **Install `pipx`**:
   Follow the instructions [here](https://github.com/pypa/pipx#install-pipx) to install `pipx`.

3. **Install Poetry**:
   Install Poetry (dependency manager) using:
   ```sh
   pip install poetry
    ```
4. **Clone the Repository:**

    ```sh
    git clone https://github.com/JohnOMDev/bremen_cadastral_data_pipeline.git
    cd geospatial-data-pipeline
    ```
5. **change directory**:
   ```sh
   cd bremen_cadastral_data_pipeline
    ```
6. **Install the dependencies**:
   Install dependency manager using:
   ```sh
   poetry update
    ```
7. **Activate the vitual enviroment**:
   Activate Poetry (dependency manager) using:
   ```sh
   poetry shell
    ```
8. **docker volume create syte_data**

9. **Create the environment varaible in `docker` folder (name it .env)**:
	Create a .env file: Place the below configuration in a file named .env in your `docker` directory.
   ```sh
    filename : .env
    ```
    Update the values: Replace syte_dbname, syte_db_user, and syte_db_password with your actual prefer values.
    ```sh
    syte_db_password=syte
    syte_db_user=syte
    syte_dbname=syte
    ```
10. **Build and Run the Docker Containers and start the application:**

    ```sh
    make all
    ```

This command will build the Docker images and start the services defined in `docker-compose.yml`.

## Usage

### Pipeline Execution

The pipeline reads geospatial data from Parquet files, performs necessary transformations, and saves the results to a PostgreSQL database with PostGIS extension.

### Query and Visualization

After the data has been processed, you can query and visualize the results.

1. **Access Fast API UI:**
    FastAPI is used for accessing, controlling, and testing the pipeline. The FastAPI dashboard for the data pipeline can be accessed at http://0.0.0.0:8000/docs or http://localhost:8000/docs.

1. **Access the Database:**

   You can connect to the PostgreSQL database using a client tool like `psql` or any GUI client. Use the credentials provided in `docker-compose.yml`.

2. **Run SQL Queries:**

   Use SQL queries to aggregate and analyze the data. For example, to get the most popular land type in each district:

    ```sql
    WITH parcel_counts AS (
        SELECT
            district,
            type,
            SUM(building_area) AS total_building_area
        FROM parcels
        GROUP BY district, type
    )
    SELECT
        district,
        type,
        total_building_area
    FROM parcel_counts
    ORDER BY district, total_building_area DESC;
    ```

## Configuration
The database credentials and other configurations can be set in the `.env` file under the docker folder. Adjust these settings as needed:
- `syte_dbname`: Database name
- `syte_db_user`: Database user
- `syte_db_password`: Database password
- `syte_db_host`: Database host

## Testing



## Performance Optimization

I consider the following optimization strategies considering the time and resources available:

- **Data Chunking:** Divide data into manageable partitions to prevent memory overload.
- **Efficient Queries:** Optimize SQL queries to enhance performance.
- **Parallel Processing:** Accelerate data ingestion using parallel processing techniques.
- **Columnar File Format and In-Memory Processing** Leverage DuckDB and Parquet’s columnar file format for efficient in-memory data processing.

## Documentation

For detailed documentation on geospatial data handling and the usage of libraries like DuckDB, GeoPandas, and PostGIS, refer to their official documentation:

- [DuckDB Documentation](https://duckdb.org/docs/)
- [GeoPandas Documentation](https://geopandas.org/)
- [PostGIS Documentation](https://postgis.net/docs/)
- [Poetry for python dependency management](https://python-poetry.org/)

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

For any questions or issues, please contact [Your Name](mailto:contact@johnomole.me).
