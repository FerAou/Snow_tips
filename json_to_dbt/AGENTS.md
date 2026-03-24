# Cortex Code - Reusable Prompts

## $flatten-variant

**Usage:** Copy-paste the prompt below to scan a Snowflake schema containing tables with VARIANT columns and automatically generate a complete dbt project (staging → intermediate → marts).

**Prompt:**

```
Scan the DSI schema of the RAW database and create a complete dbt project from the RAW_EVENTS, RAW_SPONSORS, RAW_TICKETS, RAW_SPEAKERS tables
```

**What this prompt does:**

1. DESCRIBE each table to identify VARIANT columns
2. Samples the VARIANT data (OBJECT_KEYS + SELECT LIMIT 2) to discover the JSON structure
3. Generates 3 dbt layers:
   - **staging/**: views that flatten VARIANT columns into typed columns (::STRING, ::NUMBER, ::DATE)
   - **intermediate/**: enriched tables with joins between entities
   - **marts/**: dimensions (dim_*) and facts (fct_*) ready for analytics
4. Generates _sources.yml and _schema.yml files with not_null + unique tests
5. Runs `dbt compile`, `dbt run`, and `dbt test` to materialize and test all models

**Customization:** Replace the database name, schema, and tables according to your context.
