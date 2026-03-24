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
