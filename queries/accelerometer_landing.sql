DROP TABLE IF EXISTS stedi.accelerometer_landing;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user string,
    timestamp bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-2026/accelerometer_landing/';

SELECT * FROM stedi.accelerometer_landing LIMIT 10;