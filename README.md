# ▶ Netflix Data Analysis — SQL + Python

> End-to-end ELT pipeline: raw CSV → normalised schema → analytical queries

![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![Python](https://img.shields.io/badge/Python_3-3776AB?style=flat&logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?style=flat&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=flat&logoColor=white)
![ELT](https://img.shields.io/badge/Methodology-ELT-E50914?style=flat)

---

## 📌 Project Overview

This project performs a complete **ELT (Extract, Load, Transform)** process on the Netflix Movies and TV Shows dataset. Raw CSV data is extracted with Python, loaded into a SQL Server staging table, transformed into a normalised relational schema, and analysed with five business-focused SQL queries.

| Attribute | Detail |
|---|---|
| **Dataset** | Netflix Movies & TV Shows (Kaggle) — 8,807 rows, 12 columns |
| **Tools** | SQL Server · Python 3 · pandas · SQLAlchemy |
| **Methodology** | ELT — Extract (Python/pandas), Load (SQLAlchemy), Transform (SQL) |
| **Key SQL concepts** | CTEs, window functions, STRING_SPLIT, conditional aggregation, CROSS APPLY |
| **Output** | 4 normalised tables + 5 analytical query results |

---

## 🏗️ Architecture

```
netflix_titles.csv
      │  pandas read_csv()
      ▼
netflix_raw          ← SQL Server staging table (all 12 columns, raw types)
      │
      │  CTE + ROW_NUMBER() deduplication
      │  CAST(date_added AS DATE)
      │  CASE WHEN duration IS NULL THEN rating  (duration quirk fix)
      │
      ▼
netflix              ← core table (8 columns, clean types, no duplicates)
      │
      ├── STRING_SPLIT(listed_in, ',')  ──→  netflix_genre
      ├── STRING_SPLIT(director, ',')   ──→  netflix_directors
      └── STRING_SPLIT(country, ',')    ──→  netflix_country
                                                   ▲
                                     country imputation from director history
```

---

## 📁 Repository Structure

```
Netflix-Data-Analysis-SQL-Python/
├── netflix_titles.csv           ← raw dataset (8,807 rows)
├── netflix_data_extract.py      ← Python: load CSV → SQL Server staging table
├── netflix_raw.sql              ← SQL: CREATE TABLE for staging schema
├── netflix_data_analysis.sql    ← SQL: all transformations + 5 analysis queries
└── README.md
```

---

## 🚀 Setup & How to Run

### Prerequisites

- SQL Server (any edition) or Azure SQL
- Python 3.8+
- `pip install pandas sqlalchemy pyodbc`

### Step 1 — Create the staging table

Run `netflix_raw.sql` in SQL Server Management Studio:

```sql
CREATE TABLE [dbo].[netflix_raw] (
    [show_id]      VARCHAR(10)    PRIMARY KEY,
    [type]         VARCHAR(10),
    [title]        NVARCHAR(200),   -- NVARCHAR: some titles contain non-ASCII characters
    [director]     VARCHAR(250),
    [cast]         VARCHAR(1000),
    [country]      VARCHAR(150),
    [date_added]   VARCHAR(20),     -- stored as raw string, cast to DATE in transform step
    [release_year] INT,
    [rating]       VARCHAR(10),
    [duration]     VARCHAR(10),
    [listed_in]    VARCHAR(100),
    [description]  VARCHAR(500)
);
```

### Step 2 — Load CSV with Python

Update the connection string in `netflix_data_extract.py`, then run it:

```python
import pandas as pd
import sqlalchemy as sal

df = pd.read_csv('netflix_titles.csv')
engine = sal.create_engine('mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17')
conn = engine.connect()
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')
conn.close()
```

> **Why load raw first?** ELT keeps the original data intact in the staging table. If a transformation has a bug, you can rerun it without re-extracting from the source.

### Step 3 — Run transformations and analysis

Run `netflix_data_analysis.sql` in order. The script creates all four normalised tables, then executes all five analytical queries.

---

## 🔧 Key Transformations

| Transformation | SQL Technique | Why It Was Needed |
|---|---|---|
| Remove duplicates | `CTE + ROW_NUMBER() OVER (PARTITION BY title, type)` | Same title appeared with different `show_id` values |
| Fix `date_added` type | `CAST(date_added AS DATE)` | Stored as `VARCHAR`: `'September 25, 2021'` |
| Fix `duration` nulls | `CASE WHEN duration IS NULL THEN rating ELSE duration END` | 3 TV shows had duration accidentally stored in the `rating` column |
| Normalise genres | `CROSS APPLY STRING_SPLIT(listed_in, ',')` | Multi-valued cell: `'Comedies, Dramas, International Movies'` |
| Normalise directors | `CROSS APPLY STRING_SPLIT(director, ',')` | Multi-valued cell: `'Joe Russo, Anthony Russo'` |
| Normalise countries | `CROSS APPLY STRING_SPLIT(country, ',')` | Multi-valued cell: `'United States, Canada, United Kingdom'` |
| Impute missing countries | `JOIN director → country` from existing rows | 831 null countries, many inferrable from the director's other titles |

---

## 📊 Analysis Queries

### Query 1 — Directors with both Movies and TV Shows

For each director who has created both movies and TV shows, show counts of each.

```sql
SELECT nd.director,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie'   THEN n.show_id END) AS no_of_movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS no_of_tvshows
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;
```

**Technique:** `COUNT(DISTINCT CASE WHEN ...)` conditional aggregation pivots movie/TV counts into separate columns in a single pass. `HAVING COUNT(DISTINCT n.type) > 1` filters for directors who have both.

---

### Query 2 — Country with the most Comedy movies

```sql
SELECT TOP 1 nc.country,
    COUNT(DISTINCT ng.show_id) AS no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
INNER JOIN netflix n          ON ng.show_id = nc.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC;
```

**Result:** United States — 744 comedy movies.

**Why this needs normalised tables:** Without `netflix_genre` and `netflix_country`, this query would require parsing comma-separated strings in two different columns simultaneously. With the normalised schema it is a clean 3-table join.

---

### Query 3 — Top director per year by movies released

For each calendar year (based on `date_added`), which director released the most movies?

```sql
WITH cte AS (
    SELECT nd.director,
           YEAR(date_added) AS date_year,
           COUNT(n.show_id) AS no_of_movies
    FROM netflix n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE type = 'Movie'
    GROUP BY nd.director, YEAR(date_added)
),
cte2 AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY date_year
            ORDER BY no_of_movies DESC, director  -- alphabetical tie-break = deterministic results
        ) AS rn
    FROM cte
)
SELECT * FROM cte2 WHERE rn = 1;
```

**Technique:** Chained CTEs — the second CTE references the first. `ROW_NUMBER() OVER (PARTITION BY date_year ...)` resets the rank counter per year independently.

---

### Query 4 — Average movie duration by genre

```sql
SELECT ng.genre,
    AVG(CAST(REPLACE(duration, ' min', '') AS INT)) AS avg_duration_minutes
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE type = 'Movie'
GROUP BY ng.genre
ORDER BY avg_duration_minutes DESC;
```

**Technique:** `REPLACE(duration, ' min', '')` strips the unit suffix inline before `CAST(...AS INT)` — type conversion done directly inside the aggregation without a separate transformation step.

---

### Query 5 — Directors who made both Horror and Comedy movies

```sql
SELECT nd.director,
    COUNT(DISTINCT CASE WHEN ng.genre = 'Comedies'      THEN n.show_id END) AS comedy_count,
    COUNT(DISTINCT CASE WHEN ng.genre = 'Horror Movies' THEN n.show_id END) AS horror_count
FROM netflix n
INNER JOIN netflix_genre    ng ON n.show_id = ng.show_id
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE type = 'Movie'
  AND ng.genre IN ('Comedies', 'Horror Movies')
GROUP BY nd.director
HAVING COUNT(DISTINCT ng.genre) = 2;
```

**Technique:** The `WHERE` pre-filters to only two genres before aggregation (more efficient). `HAVING COUNT(DISTINCT ng.genre) = 2` ensures the director has at least one title in *each* genre, not just titles across both genres combined.

---

## 🗂️ Final Schema

| Table | Columns | Grain | Approx Rows |
|---|---|---|---|
| `netflix` | show_id, type, title, date_added, release_year, rating, duration, description | One per title | ~8,800 |
| `netflix_genre` | show_id, genre | One per title–genre pair | ~22,000 |
| `netflix_directors` | show_id, director | One per title–director pair | ~8,000 |
| `netflix_country` | show_id, country | One per title–country pair | ~11,000 |

---

## 📈 Dataset Snapshot

| Metric | Value |
|---|---|
| Total titles | 8,807 |
| Movies | 6,131 (69.6%) |
| TV Shows | 2,676 (30.4%) |
| Date range | 1925 – 2021 |
| Top country | United States (3,690 titles) |
| #2 country | India (1,046 titles) |
| Top genre | International Movies (2,752 titles) |
| Most common rating | TV-MA (3,207 titles — 36.4%) |
| Null directors | 2,634 (29.9%) — documentaries & reality TV |
| Null countries | 831 (9.4%) — partially imputed from director data |

---

## 🧠 Skills Demonstrated

| Skill | Evidence in this project |
|---|---|
| ELT methodology | Python loads raw → SQL transforms inside the database |
| Data profiling | Max string lengths, null counts, duplicate detection before schema design |
| Schema normalisation | 4-table normalised schema from one wide staging table |
| String splitting | `STRING_SPLIT` + `CROSS APPLY` to explode multi-valued columns |
| Window functions | `ROW_NUMBER() OVER (PARTITION BY)` for deduplication and per-year ranking |
| Conditional aggregation | `COUNT(DISTINCT CASE WHEN)` as a readable pivot without `PIVOT` clause |
| Data imputation | Director-based country inference via `JOIN` |
| Type handling | `NVARCHAR` for Unicode titles, `CAST` for dates, `REPLACE+CAST` for duration strings |
| Python + SQL integration | pandas profiling → SQLAlchemy bulk load → SQL transformation |

---

## 👤 Author

**Sonal Mishra** — Data Engineer | Snowflake Squad Member | SnowPro Associate Certified | Austin User Group Leader

📧 sonalmishrapachori@gmail.com | 🔗 [LinkedIn](https://www.linkedin.com/in/mishrasonal) | 💻 [GitHub](https://github.com/Datademistifier)
