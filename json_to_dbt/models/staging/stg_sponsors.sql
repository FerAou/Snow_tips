WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_sponsors') }}
)

SELECT
    id AS sponsor_id,
    event_id,
    sponsor_data:event_name::STRING AS event_name,
    sponsor_data:event_date::DATE AS event_date,
    sponsor_data:sponsor_name::STRING AS sponsor_name
FROM source
