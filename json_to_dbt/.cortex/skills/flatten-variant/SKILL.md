---
name: flatten-variant
description: Scans a Snowflake schema to find VARIANT columns and automatically generates a complete dbt project (staging, intermediate, marts) with LATERAL FLATTEN
tools:
- snowflake_sql_execute
- snowflake_object_search
---

# When to Use

- The user asks to flatten VARIANT columns in a schema
- The user wants to create dbt staging models from tables containing JSON/VARIANT data
- The user wants to explore and flatten Snowflake semi-structured data
- The user wants to create a complete dbt project (staging → intermediate → marts) from VARIANT tables
- Keywords: flatten, variant, json, semi-structured, staging, intermediate, marts, dbt project

# What This Skill Provides

This skill automatically detects VARIANT columns in the tables of a given Snowflake schema, analyzes their JSON structure, and generates a complete 3-layer dbt project:

- **Staging**: flatten VARIANT columns using project macros, sourced directly from RAW tables
- **Intermediate**: enrichment and joins between staging models (sourced only from staging)
- **Marts**: dimensions and facts ready for consumption (sourced only from intermediate)

# Instructions

## Step 1: Identify tables with VARIANT columns

Run the following query to find VARIANT columns in the target schema:

```sql
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}'
  AND DATA_TYPE = 'VARIANT'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
```

## Step 2: Analyze the structure of VARIANT columns

For each VARIANT column found, analyze its keys and types:

```sql
SELECT
    f.key AS key_name,
    TYPEOF(f.value) AS value_type,
    COUNT(*) AS occurrences
FROM {database}.{schema}.{table},
     LATERAL FLATTEN(input => {variant_column}, RECURSIVE => FALSE) f
GROUP BY f.key, TYPEOF(f.value)
ORDER BY f.key;
```

For ARRAY type keys, determine the max_index:

```sql
SELECT MAX(ARRAY_SIZE({variant_column}:{array_key})) AS max_size
FROM {database}.{schema}.{table};
```

For OBJECT type keys, explore the sub-keys:

```sql
SELECT DISTINCT f2.key AS sub_key, TYPEOF(f2.value) AS sub_type
FROM {database}.{schema}.{table},
     LATERAL FLATTEN(input => {variant_column}:{object_key}) f2
ORDER BY f2.key;
```

## Step 3: Generate the dbt staging model

Use the existing project macros depending on the case:

### Case 1: Simple JSON with LATERAL FLATTEN (`flatten_napta_json` macro)

```sql
{{ flatten_napta_json(
    source_ref=source('{source_name}', '{table_name}'),
    json_col='{variant_column}',
    scalar_cols=[
        {'path': 'key1', 'type': 'STRING', 'alias': 'key1'},
        {'path': 'key2', 'type': 'NUMBER', 'alias': 'key2'}
    ],
    nested_cols=[
        {'path': 'parent.child', 'type': 'STRING', 'alias': 'parent_child'}
    ],
    array_cols=[
        {'path': 'items', 'alias_prefix': 'item', 'max_index': 5, 'type': 'STRING'}
    ]
) }}
```

### Case 2: Arrays with sub-fields (`flatten_array_test` macro)

```sql
{{ flatten_array_test(
    prefix='array_name',
    array_path='array_name',
    max_index=10,
    fields=[
        {'name': 'field1', 'type': 'STRING', 'alias': 'field1'},
        {'name': 'field2', 'type': 'NUMBER', 'alias': 'field2'}
    ],
    value_prefix='f.value'
) }}
```

### Case 3: Generic auto-detection (`flatten_json_column` macro)

```sql
{{ flatten_json_column(
    source_relation=source('{source_name}', '{table_name}'),
    json_column='{variant_column}',
    id_columns=['{pk_column}'],
    exclude_keys=[]
) }}
```

## Best Practices

- Always retrieve non-VARIANT (scalar) columns in addition to flattened columns
- Explicitly cast types: `::STRING`, `::NUMBER`, `::DATE`, `::TIMESTAMP`
- For arrays, use a `max_index` based on the actual `MAX(ARRAY_SIZE(...))` from the data
- Add a `LOADTIME` column with `{{ dbt_date.now() }}` if it is a project convention
- Name aliases in UPPER_CASE separated by underscores
- Place staging models in `models/staging/`
- Place intermediate models in `models/intermediate/`
- Place marts models in `models/marts/`

## dbt Layer Architecture Rule

**IMPORTANT: Strictly follow this dependency chain:**

```
sources (RAW tables) → staging → intermediate → marts
```

- **Staging**: `{{ source('...', '...') }}` — sourced only from RAW tables via `source()`
- **Intermediate**: `{{ ref('stg_...') }}` — sourced **only** from staging models via `ref()`
- **Marts**: `{{ ref('int_...') }}` — sourced **only** from intermediate models via `ref()`

A marts model must NEVER directly reference a staging model or a source.
An intermediate model must NEVER directly reference a source.

## Step 4: Generate intermediate models

Intermediate models enrich staging data by performing joins and aggregations.
They source **exclusively** from staging models via `{{ ref('stg_...') }}`.

### Enrichment pattern: staging join + aggregations

```sql
-- models/intermediate/int_{entity}_enriched.sql
WITH main_entity AS (
    SELECT * FROM {{ ref('stg_{main_table}') }}
),

related_entity AS (
    SELECT
        {foreign_key},
        COUNT(*) AS NB_{related},
        SUM({metric_col}) AS TOTAL_{metric}
    FROM {{ ref('stg_{related_table}') }}
    GROUP BY {foreign_key}
)

SELECT
    m.*,
    COALESCE(r.NB_{related}, 0) AS NB_{related},
    COALESCE(r.TOTAL_{metric}, 0) AS TOTAL_{metric}
FROM main_entity m
LEFT JOIN related_entity r ON m.{pk} = r.{foreign_key}
```

### Enriched detail pattern: simple join

```sql
-- models/intermediate/int_{detail_entity}_enriched.sql
WITH detail AS (
    SELECT * FROM {{ ref('stg_{detail_table}') }}
),

parent AS (
    SELECT * FROM {{ ref('stg_{parent_table}') }}
)

SELECT
    d.{pk},
    d.{foreign_key},
    d.{detail_columns},
    p.{parent_columns}
FROM detail d
LEFT JOIN parent p ON d.{foreign_key} = p.{pk}
```

### Configuration in dbt_project.yml

```yaml
models:
  {project_name}:
    intermediate:
      +schema: {schema}
      +materialized: table
```

## Step 5: Generate marts models

Marts models are the final tables ready for consumption.
They source **exclusively** from intermediate models via `{{ ref('int_...') }}`.

### Dimension pattern (dim_)

```sql
-- models/marts/dim_{entity}.sql
SELECT
    {pk},
    {descriptive_columns}
FROM {{ ref('int_{entity}_enriched') }}
```

### Fact pattern (fct_)

```sql
-- models/marts/fct_{entity}.sql
SELECT
    {pk},
    {foreign_keys},
    {measures}
FROM {{ ref('int_{entity}_enriched') }}
```

### Aggregated fact pattern (fct_{entity}_summary)

```sql
-- models/marts/fct_{entity}_summary.sql
SELECT
    {pk},
    {descriptive_columns},
    {aggregated_measures}
FROM {{ ref('int_{entity}_enriched') }}
```

### Configuration in dbt_project.yml

```yaml
models:
  {project_name}:
    marts:
      +schema: {schema}
      +materialized: table
```

## Step 6: Add schema.yml files

Each layer must have a `_schema.yml` file with tests:

```yaml
# models/{layer}/_schema.yml
version: 2

models:
  - name: {model_name}
    description: "{description}"
    columns:
      - name: {pk_column}
        tests:
          - not_null
          - unique
```

## Common Patterns

### Pattern 1: Table with a single VARIANT column (API type)

The table has an ID + a VARIANT column containing complex JSON with nested arrays.
Use `flatten_array_test for arrays and extract scalars manually.

### Pattern 2: Table with a text JSON column

The table has a VARCHAR column containing JSON (not native VARIANT).
Use `flatten_json_column` which automatically applies `PARSE_JSON()`.

### Pattern 3: Table with LATERAL FLATTEN on a root array

The VARIANT is directly an array of objects.
Use `flatten_napta_json` with `LATERAL FLATTEN(input => column)`.

# Examples

## Example 1: Scan a schema

User: $flatten-variant Scan the RAW schema and flatten all VARIANT columns
Assistant:
1. Runs the INFORMATION_SCHEMA query to find VARIANT columns
2. For each table/column, analyzes the JSON structure
3. Generates dbt staging files with the appropriate macros

## Example 2: Flatten a specific table

User: $flatten-variant Flatten the DATA column of the USERS table
Assistant:
1. Analyzes the keys of the DATA column
2. Chooses the appropriate macro (flatten_json_column for auto-detection)
3. Generates the staging model

## Example 3: Complete dbt project with 3 layers

User: $flatten-variant Create a complete dbt project from the RAW_EVENTS, RAW_SPONSORS, RAW_TICKETS, RAW_SPEAKERS tables
Assistant:
1. Analyzes the VARIANT columns of each source table
2. Generates staging models (stg_events, stg_speakers, stg_sponsors, stg_tickets) with VARIANT flattening
3. Generates intermediate models (int_events_enriched, int_speakers_enriched, etc.) sourced from staging
4. Generates marts models (dim_events, dim_speakers, dim_sponsors, fct_tickets, fct_event_summary) sourced from intermediate
5. Adds _sources.yml and _schema.yml files with not_null and unique tests
6. Compiles and runs `dbt run` then `dbt test`
