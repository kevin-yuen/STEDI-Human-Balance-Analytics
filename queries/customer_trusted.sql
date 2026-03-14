DROP TABLE IF EXISTS stedi.customer_trusted;

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_trusted (
    customerName string,
    email string,
    phone string,
    birthDay string,
    serialNumber string,
    registrationDate bigint,
    lastUpdateDate bigint,
    shareWithResearchAsOfDate bigint,
    shareWithPublicAsOfDate bigint,
    shareWithFriendsAsOfDate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-2026/customer_trusted/';

SELECT * FROM stedi.customer_trusted;