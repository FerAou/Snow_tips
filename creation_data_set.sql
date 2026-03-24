CREATE OR REPLACE TABLE  RAW_EVENTS (
    ID NUMBER AUTOINCREMENT,
    DATA VARIANT
);

INSERT INTO RAW_EVENTS (DATA)
SELECT PARSE_JSON(column1) FROM VALUES
('{"eventName":"Tech Summit 2025","eventDate":"2025-06-15","location":{"venue":"Palais des Congres","city":"Paris","country":"France","capacity":500},"organizer":{"name":"DevoteamEvents","email":"events@devoteam.com"},"speakers":[{"name":"Alice Dupont","topic":"Data Engineering","duration":45},{"name":"Bob Martin","topic":"Cloud Architecture","duration":30},{"name":"Carla Lopez","topic":"Digital Marketing","duration":30}],"sponsors":["Snowflake","AWS","Google Cloud"],"tickets":{"standard":150,"vip":350,"student":50}}'),
('{"eventName":"Data Meetup Lyon","eventDate":"2025-09-20","location":{"venue":"Salle Bellecour","city":"Lyon","country":"France","capacity":100},"organizer":{"name":"DataCommunity","email":"contact@datacommunity.fr"},"speakers":[{"name":"David Chen","topic":"Kubernetes at Scale","duration":60}],"sponsors":["Datadog","Confluent"],"tickets":{"standard":0,"vip":50,"student":0}}'),
('{"eventName":"AI Conference","eventDate":"2025-11-10","location":{"venue":"Centre de Conferences","city":"Marseille","country":"France","capacity":300},"organizer":{"name":"AI France","email":"info@aifrance.org"},"speakers":[{"name":"Eva Schmidt","topic":"LLMs in Production","duration":45},{"name":"Frank Rossi","topic":"Computer Vision","duration":45},{"name":"Grace Kim","topic":"MLOps Best Practices","duration":30},{"name":"Henri Blanc","topic":"Ethics in AI","duration":30}],"sponsors":["NVIDIA","Microsoft","Mistral AI","Hugging Face"],"tickets":{"standard":200,"vip":500,"student":75}}')


; 



CREATE OR REPLACE TABLE  RAW_SPONSORS (
    ID NUMBER AUTOINCREMENT,
    EVENT_ID NUMBER,
    SPONSOR_DATA VARIANT
);

INSERT INTO RAW_SPONSORS (EVENT_ID, SPONSOR_DATA)
SELECT 
    re.ID,
    OBJECT_CONSTRUCT('sponsor_name', sp.VALUE::STRING, 'event_name', re.DATA:eventName::STRING, 'event_date', re.DATA:eventDate::STRING)
FROM RAW_EVENTS re,
LATERAL FLATTEN(input => re.DATA:sponsors) sp

; 


select * from  RAW_SPONSORS;


CREATE OR REPLACE TABLE  RAW_SPEAKERS (
    ID NUMBER AUTOINCREMENT,
    EVENT_ID NUMBER,
    SPEAKER_DATA VARIANT
);

INSERT INTO  RAW_SPEAKERS (EVENT_ID, SPEAKER_DATA)
SELECT 
    re.ID,
    OBJECT_CONSTRUCT(
        'speaker_name', sp.VALUE:name::STRING,
        'topic', sp.VALUE:topic::STRING,
        'duration', sp.VALUE:duration::NUMBER,
        'event_name', re.DATA:eventName::STRING,
        'event_date', re.DATA:eventDate::STRING,
        'city', re.DATA:location.city::STRING
    )
FROM  RAW_EVENTS re,
LATERAL FLATTEN(input => re.DATA:speakers) sp;


CREATE OR REPLACE TABLE  RAW_TICKETS (
    ID NUMBER AUTOINCREMENT,
    EVENT_ID NUMBER,
    TICKET_DATA VARIANT
);

INSERT INTO  RAW_TICKETS (EVENT_ID, TICKET_DATA)
SELECT 
    re.ID,
    OBJECT_CONSTRUCT(
        'event_name', re.DATA:eventName::STRING,
        'event_date', re.DATA:eventDate::STRING,
        'city', re.DATA:location.city::STRING,
        'standard_price', re.DATA:tickets.standard::NUMBER,
        'vip_price', re.DATA:tickets.vip::NUMBER,
        'student_price', re.DATA:tickets.student::NUMBER,
        'capacity', re.DATA:location.capacity::NUMBER
    )
FROM  RAW_EVENTS re; ; 


CREATE OR REPLACE TABLE  RAW_SPEAKERS (
    ID NUMBER AUTOINCREMENT,
    EVENT_ID NUMBER,
    SPEAKER_DATA VARIANT
);

INSERT INTO  RAW_SPEAKERS (EVENT_ID, SPEAKER_DATA)
SELECT 
    re.ID,
    s.VALUE
FROM  RAW_EVENTS re,
LATERAL FLATTEN(input => re.DATA:speakers) s