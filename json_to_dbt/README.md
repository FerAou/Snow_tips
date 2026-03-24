# Nested Data, Sprawling Schemas: How Cortex Code Brings Order to Chaos

**Author:** Ferhat AOUAGHZENE — Snowflake Certified & Squad Member, dbt Developer Certified

---

## Context and Problem Statement

Data migrations involving semi-structured data — often originating from complex API response ingestion — pose a major challenge: structures so deeply nested, VARIANTs hiding within VARIANTs, that flattening them manually becomes a real test of patience. Some tables easily reach 3,000 columns.

Using an external LLM would be the ideal solution, but security and confidentiality constraints make it entirely impossible. What's needed is an **internal, robust, and secure** solution capable of handling this complexity.

**Cortex Code** answers this need: data never leaves Snowflake — everything runs in a controlled, secure environment that follows best practices.

---

## What Is a Cortex Code Skill?

A **skill** is a set of formalized tasks and rules designed to automate what was previously done manually. The `flatten-variant` skill chains the following tasks:

1. Search for tables containing VARIANT columns
2. Identify the VARIANT types (scalar, nested object, array)
3. Apply the appropriate dbt macros to flatten the data
4. Create a complete dbt project (staging → intermediate → marts)
5. Create quality tests (`not_null`, `unique`)
6. Run `dbt compile`, `dbt run`, `dbt test`

This skill uses macros such as `flatten_json_column`, `flatten_napta_json`, `flatten_array`, `flatten_whoz_array` — all generated with Cortex Code.

---

## Cortex Code Agent

Cortex Code automatically executes the tasks defined in skills by relying on the prompt stored in the `AGENTS.md` file, enabling full process orchestration without manual intervention.

To launch this skill, simply type in Cortex Code:

```
$flatten-variant
```

---

## Project Architecture

```
json_to_dbt/
├── .cortex/skills/flatten-variant/SKILL.md   # Skill definition
├── AGENTS.md                                  # Reusable prompts
├── dbt_project.yml                            # dbt configuration
├── profiles.yml                               # Snowflake connection
├── packages.yml                               # Dependencies
├── macros/
│   ├── flatten_json_column.sql                # Generic auto-detection
│   ├── flatten_napta_json.sql                 # Configurable LATERAL FLATTEN
│   ├── flatten_array.sql                      # Indexed arrays
│   ├── flatten_whoz_json.sql                  # Whoz JSON
│   └── flatten_whoz_array.sql                 # Whoz arrays
├── models/
│   ├── staging/                               # Views — VARIANT flattening
│   │   ├── _sources.yml                       # RAW source declarations
│   │   ├── _schema.yml                        # Staging tests
│   │   ├── stg_events.sql                     # Flatten nested objects
│   │   ├── stg_events_speakers.sql            # LATERAL FLATTEN speakers array
│   │   ├── stg_events_sponsors.sql            # LATERAL FLATTEN sponsors array
│   │   ├── stg_speakers.sql                   # Flatten scalars
│   │   ├── stg_sponsors.sql                   # Flatten scalars
│   │   └── stg_tickets.sql                    # Flatten scalars
│   ├── intermediate/                          # Tables — staging joins
│   │   ├── _schema.yml
│   │   ├── int_events_with_speakers.sql
│   │   └── int_events_with_sponsors.sql
│   └── marts/                                 # Tables — dims & facts
│       ├── _schema.yml
│       ├── dim_events.sql
│       ├── dim_speakers.sql
│       ├── dim_sponsors.sql
│       └── fct_tickets.sql
```

---

## dbt Configuration

### dbt_project.yml — Materialization per Layer

```yaml
models:
  json_to_dbt:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: table
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
```

### profiles.yml — Snowflake Connection

```yaml
json_to_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: ANALYTICS
      schema: PUBLIC
      threads: 8
```

---

## Data Sources

The skill scanned the `RAW.PUBLIC` schema and identified 4 tables with VARIANT columns:

| Table | VARIANT Column | Structure |
|---|---|---|
| `RAW_EVENTS` | `DATA` | Nested objects (location, organizer) + arrays (speakers, sponsors) |
| `RAW_SPEAKERS` | `SPEAKER_DATA` | Simple scalars (name, topic, duration) |
| `RAW_SPONSORS` | `SPONSOR_DATA` | Simple scalars (event_name, sponsor_name) |
| `RAW_TICKETS` | `TICKET_DATA` | Simple scalars (prices, capacity) |

### JSON Structure Example — RAW_EVENTS.DATA

```json
{
  "eventName": "Tech Summit 2025",
  "eventDate": "2025-06-15",
  "location": {
    "city": "Paris",
    "country": "France",
    "venue": "Palais des Congres",
    "capacity": 500
  },
  "organizer": {
    "name": "DevoteamEvents",
    "email": "events@devoteam.com"
  },
  "speakers": [
    { "name": "Alice Dupont", "topic": "Data Engineering", "duration": 45 },
    { "name": "Bob Martin", "topic": "Cloud Architecture", "duration": 30 }
  ],
  "sponsors": ["Snowflake", "AWS", "Google Cloud"],
  "tickets": { "standard": 150, "student": 50, "vip": 350 }
}
```

---

## Staging Layer — VARIANT Flattening

Staging models source **exclusively** from RAW tables via `{{ source() }}` and are materialized as **views**.

### Pattern 1: Nested Objects — Dot Notation

```sql
-- models/staging/stg_events.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
)

SELECT
    id AS event_id,
    data:eventName::STRING AS event_name,
    data:eventDate::DATE AS event_date,
    data:location.city::STRING AS city,
    data:location.country::STRING AS country,
    data:location.venue::STRING AS venue,
    data:location.capacity::NUMBER AS capacity,
    data:organizer.name::STRING AS organizer_name,
    data:organizer.email::STRING AS organizer_email,
    data:tickets.standard::NUMBER AS ticket_price_standard,
    data:tickets.student::NUMBER AS ticket_price_student,
    data:tickets.vip::NUMBER AS ticket_price_vip
FROM source
```

### Pattern 2: LATERAL FLATTEN — Array Explosion

```sql
-- models/staging/stg_events_speakers.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
)

SELECT
    id AS event_id,
    data:eventName::STRING AS event_name,
    f.value:name::STRING AS speaker_name,
    f.value:topic::STRING AS topic,
    f.value:duration::NUMBER AS duration_minutes
FROM source,
    LATERAL FLATTEN(input => data:speakers) f
```

### Pattern 3: Simple Scalar VARIANTs

```sql
-- models/staging/stg_tickets.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_tickets') }}
)

SELECT
    id AS ticket_id,
    event_id,
    ticket_data:event_name::STRING AS event_name,
    ticket_data:event_date::DATE AS event_date,
    ticket_data:city::STRING AS city,
    ticket_data:capacity::NUMBER AS capacity,
    ticket_data:standard_price::NUMBER AS standard_price,
    ticket_data:student_price::NUMBER AS student_price,
    ticket_data:vip_price::NUMBER AS vip_price
FROM source
```

---

## Intermediate Layer — Joins and Enrichment

Intermediate models source **exclusively** from staging models via `{{ ref('stg_...') }}` and are materialized as **tables**.

```sql
-- models/intermediate/int_events_with_speakers.sql
WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

speakers AS (
    SELECT * FROM {{ ref('stg_speakers') }}
)

SELECT
    events.event_id,
    events.event_name,
    events.event_date,
    events.city,
    events.venue,
    speakers.speaker_id,
    speakers.speaker_name,
    speakers.topic,
    speakers.duration_minutes
FROM events
INNER JOIN speakers
    ON events.event_id = speakers.event_id
```

---

## Marts Layer — Dimensions and Facts

Marts models are the final tables ready for analytical consumption.

### Dimension

```sql
-- models/marts/dim_events.sql
SELECT
    event_id, event_name, event_date, city, country, venue,
    capacity, organizer_name, organizer_email,
    ticket_price_standard, ticket_price_student, ticket_price_vip
FROM {{ ref('stg_events') }}
```

### Fact

```sql
-- models/marts/fct_tickets.sql
SELECT
    t.ticket_id,
    t.event_id,
    e.event_name,
    e.event_date,
    e.city,
    t.capacity,
    t.standard_price,
    t.student_price,
    t.vip_price,
    (t.standard_price + t.student_price + t.vip_price) AS total_price_all_tiers
FROM {{ ref('stg_tickets') }} t
LEFT JOIN {{ ref('stg_events') }} e
    ON t.event_id = e.event_id
```

---

## Layer Architecture Rule

```
sources (RAW tables) → staging → intermediate → marts
```

| Layer | Sources from | Via | Materialization |
|---|---|---|---|
| **Staging** | RAW tables | `{{ source('...', '...') }}` | view |
| **Intermediate** | staging models | `{{ ref('stg_...') }}` | table |
| **Marts** | intermediate models | `{{ ref('int_...') }}` | table |

---

## Quality Tests

Each layer has a `_schema.yml` file with `not_null` and `unique` tests on key columns. Total: **34 tests** covering all 3 layers.

---

## Available Macros

| Macro | Usage |
|---|---|
| `flatten_json_column` | Generic auto-detection of scalar, object, and array keys |
| `flatten_napta_json` | Configurable LATERAL FLATTEN with scalars, nested, and arrays |
| `flatten_array` | Indexed arrays with sub-fields |
| `flatten_whoz_array` | Whoz arrays with sub-fields and configurable prefix |

---

## Execution Results

| Command | Result |
|---|---|
| `dbt compile` | 14 models, 34 tests, 4 sources |
| `dbt run` | **14/14 PASS** — 7 views (staging) + 7 tables (intermediate + marts) |
| `dbt test` | **34/34 PASS** — not_null + unique across all layers |

### Materialized Objects in Snowflake (ANALYTICS database)

| Schema | Models | Type |
|---|---|---|
| `PUBLIC_STAGING` | stg_events, stg_speakers, stg_sponsors, stg_tickets, stg_events_speakers, stg_events_sponsors | VIEW |
| `PUBLIC_INTERMEDIATE` | int_events_with_speakers, int_events_with_sponsors | TABLE |
| `PUBLIC_MARTS` | dim_events, dim_speakers, dim_sponsors, fct_tickets | TABLE |

---

## How to Use

1. Open Cortex Code in Snowsight
2. Type `$flatten-variant`
3. Cortex Code scans the schema, analyzes VARIANTs, generates dbt models, and runs the build

To customize, adapt the prompt:

```
Scan the DSI schema of the DEVOTEAM_AOUAGHZENE database and create a complete dbt project
from the RAW_EVENTS, RAW_SPONSORS, RAW_TICKETS, RAW_SPEAKERS tables
```

---

## Conclusion

By combining these skills with Cortex Code, tedious manual work is transformed into a fully automated process. In just a few minutes, you get a complete, consistent, and ready-to-use dbt project — with no manual intervention. Most importantly, the data never leaves Snowflake: everything runs in a controlled, secure environment that follows best practices. Development is accelerated without compromising governance, confidentiality, or data integrity, allowing teams to focus entirely on delivering business value.
