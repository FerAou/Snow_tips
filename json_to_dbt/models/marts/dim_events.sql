SELECT
    event_id,
    event_name,
    event_date,
    city,
    country,
    venue,
    capacity,
    organizer_name,
    organizer_email,
    ticket_price_standard,
    ticket_price_student,
    ticket_price_vip
FROM {{ ref('stg_events') }}
