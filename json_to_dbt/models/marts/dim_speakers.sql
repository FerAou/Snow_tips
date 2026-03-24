SELECT
    speaker_id,
    speaker_name,
    topic,
    duration_minutes
FROM {{ ref('stg_speakers') }}
