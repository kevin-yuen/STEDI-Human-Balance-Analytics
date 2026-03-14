DROP TABLE IF EXISTS stedi.machine_learning_curated;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.machine_learning_curated (
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject smallint,
    z double,
    birthDay string,
    shareWithPublicAsOfDate bigint,
    shareWithResearchAsOfDate bigint,
    registrationDate bigint,
    customerName string,
    user string,
    y double,
    shareWithFriendsAsOfDate bigint,
    x double,
    timestamp bigint,
    email string,
    lastUpdateDate bigint,
    phone string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-2026/machine_learning_curated/';

SELECT * FROM stedi.machine_learning_curated;
