# dbt Flights Project Documentation

## Overview
This project transforms raw flight data from multiple sources (Amadeus and Skyscanner) into a unified dimensional model suitable for analytics. The project follows a modern data stack approach using **dbt** for transformation, adhering to **Kimball's dimensional modeling** principles.

## Modeling Strategy

### Architecture
The project is organized into three main layers:
1.  **Staging (`models/staging`)**: Raw data 1:1 with source, with standardized column names (e.g., `origin_airport_code`, `price_amount`).
2.  **Transformed (`models/transformed`)**: Intermediate views that align schemas and data types from different sources to prepare them for unioning.
3.  **Marts (`models/marts`)**: The final presentation layer containing Dimensions and Facts.

### "Union and Aggregate" Pattern
One of the core challenges in this project is that dimensional data (Airlines, Airports, Flight Attributes) is scattered across multiple transactional sources, often with varying levels of completeness.

To solve this, we implemented the **Union and Aggregate** pattern for generating dimensions:
1.  **Union**: We select all distinct dimensional keys and attributes from *all* available sources (e.g., extracting airports from both origin and destination fields in both Amadeus and Skyscanner models).
2.  **Aggregate**: We group by the natural key (e.g., `airport_code`, `flight_code`) and apply aggregation functions to consolidate attributes.
    *   **Prioritization**: We use functions like `MAX()` on text fields or specific `COALESCE` logic to prioritize the source with richer metadata (e.g., prioritizing Skyscanner for City/Country names).
    *   **Boolean Merging**: logical `OR` (`BOOL_OR`) is used to merge flags; if a flight is flagged as "blacklisted" in *any* source, it remains blacklisted in the dimension.

---

## Data Models (`models/marts`)

### Dimensions

#### `dim_airport`
*   **Description**: Contains unique airport codes and their metadata (City, Country).
*   **Generation Logic**:
    *   Unions `origin_airport` and `destination_airport` from Skyscanner.
    *   Unions `departure_airport` and `arrival_airport` from Amadeus.
    *   **Source Priority**: Prioritizes Skyscanner data for `city` and `country` because Amadeus data lacks geographical context for these fields.

#### `dim_airline`
*   **Description**: Normalized list of airlines, including marketing and operating carriers.
*   **Generation Logic**: Unions carrier codes from both sources. It handles the resolution of `airline_name` and `operating_airline_name` by taking the best available non-null value.

#### `dim_flight_attributes` (Junk Dimension)
*   **Description**: A "Junk Dimension" that handles low-cardinality boolean flags to keep the Fact table clean.
*   **Content**: Contains flags like `is_self_transfer`, `blacklisted_in_eu`, `is_cancellation_allowed`.
*   **Granularity**: Keyed by `flight_code`.
*   **Logic**: Aggregates boolean flags across sources. If a flag is true in one source, it is captured as true in the dimension.

### Fact Tables

#### `fact_flights`
*   **Description**: The central incremental fact table containing transactional flight data (prices, distinct legs, timings).
*   **Granularity**: One row per flight observation per source.
*   **Partitioning**:
    *   `partition_by=['departure_date', 'origin_airport', 'arrival_airport']`
    *   **Why?**: These columns are the most common filter predicates for flight search queries. Partitioning by them significantly reduces scanning costs and improves query performance in downstream engines like AWS Athena or Presto.
*   **Incremental Strategy**:
    *   **Strategy**: `insert_overwrite` (optimized for partitions).
    *   **Watermark**: Uses `ingestion_dt` to identify new records.
    *   **Logic**: The model filters for records where `ingestion_dt` is greater than the maximum `ingestion_dt` already present in the target table, ensuring only new data is processed during incremental runs.

## Project Configuration
*   **Materialization**: All models in `marts` are materialized as **Tables** (Parquet/Iceberg format compliant) for performance, while `transformed` and `staging` layers are typically views to reduce storage redundancy.


