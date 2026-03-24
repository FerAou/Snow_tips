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
