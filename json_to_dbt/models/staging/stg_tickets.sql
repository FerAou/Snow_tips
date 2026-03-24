WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_tickets') }}
)

SELECT
    id AS ticket_id,
    event_id,
    ticket_data:event_name::STRING AS event_name,
    ticket_data:event_date::DATE AS event_date,
    ticket_data:city::STRING AS city,
    ticket_data:capacity::NUMBER AS capacity,
    ticket_data:standard_price::NUMBER AS standard_price,
    ticket_data:student_price::NUMBER AS student_price,
    ticket_data:vip_price::NUMBER AS vip_price
FROM source
