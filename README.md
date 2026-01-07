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

-- 

# Reporting Queries Examples

Here are some example SQL queries demonstrating how to leverage the Dimensional Model to extract business insights.

## 1. Average Price by Airline
Analyze which airlines offer the most competitive pricing.

```sql
SELECT 
    da.airline_name,
    COUNT(f.flight_id) as total_flights,
    AVG(f.total_price) as avg_price,
    MIN(f.total_price) as min_price,
    MAX(f.total_price) as max_price
FROM {{ ref('fact_flights') }} f
JOIN {{ ref('dim_airline') }} da 
    ON f.airline_key = da.airline_key
GROUP BY 
    da.airline_name
ORDER BY 
    avg_price ASC;
```

**Example Result:**

| airline_name | total_flights | avg_price | min_price | max_price |
| :--- | :--- | :--- | :--- | :--- |
| RyanAir | 1250 | 45.50 | 19.99 | 120.00 |
| Vueling | 980 | 65.20 | 35.00 | 150.50 |
| Iberia | 1500 | 140.00 | 80.00 | 450.00 |
| British Airways | 2100 | 180.75 | 95.00 | 800.00 |
| Emirates | 300 | 850.00 | 400.00 | 2500.00 |

## 2. Top Popular Routes (Origin -> Destination)
Identify the busiest routes based on flight volume and average duration.

```sql
SELECT 
    org.city as origin_city,
    dst.city as destination_city,
    COUNT(f.flight_id) as flight_count,
    AVG(f.duration_minutes) as avg_duration_mins
FROM {{ ref('fact_flights') }} f
JOIN {{ ref('dim_airport') }} org 
    ON f.origin_airport_key = org.airport_key
JOIN {{ ref('dim_airport') }} dst 
    ON f.arrival_airport = dst.airport_code -- Note: Using Code if key join logic differs, or join on generated key if available for arrival
    -- Ideally: JOIN {{ ref('dim_airport') }} dst ON f.arrival_airport = dst.airport_code
    -- Since we didn't generate a specific arrival_airport_key in fact (only origin), we join by code or add the key to fact.
    -- Assuming dim_airport is unique by airport_code:
    AND dst.airport_key IS NOT NULL
GROUP BY 
    org.city, 
    dst.city
ORDER BY 
    flight_count DESC
LIMIT 10;
```

**Example Result:**

| origin_city | destination_city | flight_count | avg_duration_mins |
| :--- | :--- | :--- | :--- |
| London | New York | 540 | 485.5 |
| Madrid | Barcelona | 420 | 75.0 |
| Paris | Tokyo | 310 | 720.0 |
| Dubai | London | 280 | 450.0 |
| New York | Los Angeles | 250 | 360.5 |

## 3. Analysis of "Self-Transfer" Flights (Junk Dimension)
Compare prices between flights that require self-transfer vs. those that don't.

```sql
SELECT 
    dfa.is_self_transfer,
    COUNT(f.flight_id) as flights_volume,
    AVG(f.total_price) as avg_price,
    AVG(f.duration_minutes) as avg_duration
FROM {{ ref('fact_flights') }} f
JOIN {{ ref('dim_flight_attributes') }} dfa
    ON f.flight_code = dfa.flight_code -- Joining by natural key because dim_flight_attributes granularity is flight_code
    -- Or if using the surrogate key: ON f.flight_code = dfa.flight_code (Fact needs the key or join on natural key)
    -- In our model we didn't explicitly add flight_attributes_key to fact, so we join on flight_code.
GROUP BY 
    dfa.is_self_transfer;
```

**Example Result:**

| is_self_transfer | flights_volume | avg_price | avg_duration |
| :--- | :--- | :--- | :--- |
| false | 8500 | 250.00 | 240.5 |
| true | 1200 | 180.00 | 320.0 |

## 4. Blacklisted Airline Exposure
Monitor how many flights are being sold that are operated by airlines blacklisted in the EU.

```sql
SELECT 
    da.airline_name,
    f.source_channel,
    COUNT(*) as risky_flights
FROM {{ ref('fact_flights') }} f
JOIN {{ ref('dim_flight_attributes') }} dfa 
    ON f.flight_code = dfa.flight_code
JOIN {{ ref('dim_airline') }} da 
    ON f.airline_key = da.airline_key
WHERE 
    dfa.blacklisted_in_eu = true
GROUP BY 
    da.airline_name, 
    f.source_channel;
```

**Example Result:**

| airline_name | source_channel | risky_flights |
| :--- | :--- | :--- |
| Sketchy Air | amadeus | 45 |
| NoSafety Jets | skyscanner | 12 |
| Banned Wings | amadeus | 8 |



