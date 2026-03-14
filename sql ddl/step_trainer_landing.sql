DROP TABLE IF EXISTS stedi.step_trainer_landing;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject smallint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-2026/step_trainer_landing/';

SELECT * FROM stedi.step_trainer_landing LIMIT 10;