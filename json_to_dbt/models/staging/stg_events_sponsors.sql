WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
)

SELECT
    id AS event_id,
    data:eventName::STRING AS event_name,
    f.value::STRING AS sponsor_name
FROM source,
    LATERAL FLATTEN(input => data:sponsors) f
