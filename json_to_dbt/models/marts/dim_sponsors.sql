SELECT DISTINCT
    sponsor_name
FROM {{ ref('stg_sponsors') }}
ORDER BY sponsor_name
