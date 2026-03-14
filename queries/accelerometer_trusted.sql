DROP TABLE IF EXISTS stedi.accelerometer_trusted;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_trusted (
    user string,
    timestamp bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-2026/accelerometer_trusted/';

SELECT * FROM stedi.accelerometer_trusted;