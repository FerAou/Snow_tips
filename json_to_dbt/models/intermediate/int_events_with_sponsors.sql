WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

sponsors AS (
    SELECT * FROM {{ ref('stg_sponsors') }}
)

SELECT
    events.event_id,
    events.event_name,
    events.event_date,
    events.city,
    events.venue,
    sponsors.sponsor_id,
    sponsors.sponsor_name
FROM events
INNER JOIN sponsors
    ON events.event_id = sponsors.event_id
