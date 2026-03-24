SELECT
    t.ticket_id,
    t.event_id,
    e.event_name,
    e.event_date,
    e.city,
    t.capacity,
    t.standard_price,
    t.student_price,
    t.vip_price,
    (t.standard_price + t.student_price + t.vip_price) AS total_price_all_tiers
FROM {{ ref('stg_tickets') }} t
LEFT JOIN {{ ref('stg_events') }} e
    ON t.event_id = e.event_id
