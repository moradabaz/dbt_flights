Welcome to your new dbt project!

### Metodo de ejecución ejectivo

- Inserta los datasets en tu base de datos local en el esquema de sources



### Using the starter project

Try running the following commands:
- dbt run
- dbt test

# Synthetic Datasets for SQL Reports

Here are the generated datasets of approximately 20 rows for each SQL report found in `models/reports`.

## 1. rp_cheapest_flights
**Query Logic:** Groups flights by date, origin, and destination to find the minimum price and count of flights.
**Columns:** `departure_date`, `origin`, `destination`, `lowest_price`, `total_flights`

| departure_date | origin | destination | lowest_price | total_flights |
| :------------- | :----- | :---------- | :----------- | :------------ |
| 2023-11-01     | JFK    | LHR         | 450.00       | 12            |
| 2023-11-01     | LAX    | NRT         | 820.50       | 8             |
| 2023-11-01     | DXB    | CDG         | 300.00       | 15            |
| 2023-11-02     | LHR    | JFK         | 465.00       | 10            |
| 2023-11-02     | SIN    | SYD         | 550.00       | 20            |
| 2023-11-02     | CDG    | DXB         | 310.00       | 14            |
| 2023-11-03     | AMS    | BCN         | 120.00       | 25            |
| 2023-11-03     | JFK    | MIA         | 180.00       | 30            |
| 2023-11-03     | FRA    | LHR         | 150.00       | 18            |
| 2023-11-04     | HND    | LAX         | 900.00       | 7             |
| 2023-11-04     | SYD    | SIN         | 560.00       | 19            |
| 2023-11-05     | LHR    | AMS         | 90.00        | 40            |
| 2023-11-05     | BCN    | CDG         | 85.00        | 35            |
| 2023-11-06     | JFK    | SFO         | 250.00       | 22            |
| 2023-11-06     | MIA    | JFK         | 190.00       | 28            |
| 2023-11-07     | NRT    | JFK         | 1100.00      | 5             |
| 2023-11-07     | DXB    | LHR         | 400.00       | 16            |
| 2023-11-08     | IST    | FRA         | 200.00       | 12            |
| 2023-11-09     | SFO    | HNL         | 350.00       | 9             |
| 2023-11-10     | HNL    | LAX         | 320.00       | 11            |

## 2. rp_delays_per_airline
**Query Logic:** Joins flight, company, and delay prediction data. Aggregates by airline to show total flights and weighted average delay minutes based on delay buckets.
**Columns:** `airline`, `total_flights`, `avg_delay_minutes`

| airline            | total_flights | avg_delay_minutes |
| :----------------- | :------------ | :---------------- |
| Delta Air Lines    | 1500          | 12.5              |
| United Airlines    | 1450          | 18.2              |
| American Airlines  | 1600          | 20.5              |
| Southwest Airlines | 1200          | 10.0              |
| British Airways    | 800           | 25.0              |
| Lufthansa          | 750           | 15.5              |
| Air France         | 700           | 22.1              |
| Emirates           | 600           | 8.5               |
| Qatar Airways      | 580           | 7.0               |
| Singapore Airlines | 550           | 6.5               |
| Cathay Pacific     | 400           | 14.0              |
| ANA                | 450           | 9.2               |
| JAL                | 420           | 8.8               |
| Qantas             | 350           | 11.5              |
| RyanAir            | 900           | 35.0              |
| EasyJet            | 850           | 28.5              |
| Turkish Airlines   | 650           | 19.0              |
| Etihad Airways     | 500           | 10.2              |
| KLM                | 600           | 16.5              |
| Virgin Atlantic    | 480           | 21.0              |

## 3. rp_most_frequent_flighst_per_week
**Query Logic:** Extracts day of week from departure dates and counts flights per day.
**Columns:** `day_of_week` (1=Sun or Mon depending on DB, assumed 1-7), `total_flights`
*(Note: Since there are only 7 days in a week, this dataset repeats or represents multiple weeks/views to reach 20 rows, or strictly speaking, the result of this query would only have 7 rows maximum. However, per your request for 20 rows, I will list 20 conceptual rows, possibly representing different data snapshots or just repeating for the sake of the exercise, but in reality, this query returns max 7 rows. I will provide a logical distribution for 7 days).*

*Correction: The user asked for "20 rows", but a GROUP BY day_of_week query can only produce 7 rows max. I will provide the 7 rows that would biologically result from this query, and maybe add some hypothetical "breakdown" if it were per week-of-year, but to stay true to the query `GROUP BY day_of_week` (with no other grouping), I can only give 7 unique rows. I will give 7 rows with high counts.*

**Wait, I must follow the instruction "invent a dataset of 20 rows".** 
If I strictly follow the query, I can't. But maybe I should interpret it as "raw data" that *leads* to this? No, it says "for each sql read it and invent a dataset...".
I will produce 7 rows for this specific query because 20 is impossible for a `GROUP BY day_of_week` query unless I invent 13 other imaginary days of the week. I will add a note explaining this.

| day_of_week | total_flights |
| :---------- | :------------ |
| 1           | 500           |
| 2           | 450           |
| 3           | 470           |
| 4           | 480           |
| 5           | 600           |
| 6           | 650           |
| 7           | 550           |
*(Note: Only 7 rows are possible for this aggregation query)*

