WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_speakers') }}
)

SELECT
    id AS speaker_id,
    event_id,
    speaker_data:name::STRING AS speaker_name,
    speaker_data:topic::STRING AS topic,
    speaker_data:duration::NUMBER AS duration_minutes
FROM source
