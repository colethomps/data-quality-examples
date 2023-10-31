
--#2
CREATE TABLE user_devices_cumulated
(
    device_id                bigint,
    device_activity_datelist date[],
    browser_type             TEXT,
    device_type              TEXT,
        PRIMARY KEY (device_id,browser_type,device_type,device_activity_datelist)
)

--#3
INSERT INTO user_devices_cumulated
SELECT
    d.device_id,
    array_agg(date(event_time)) as device_activity_datelist,
    d.browser_type,
    d.device_type
FROM events e
LEFT JOIN devices d ON e.device_id = d.device_id
WHERE e.device_id IS NOT NULL
GROUP BY d.device_id, d.device_type, d.browser_type
