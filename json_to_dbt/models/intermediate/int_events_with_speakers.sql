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
