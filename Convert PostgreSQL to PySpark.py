from pyspark.sql import SparkSession

def create_user_devices_cumulated(spark):
    # Query 1 - Create user_devices_cumulated
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW user_devices_cumulated AS
    SELECT
        device_id,
        collect_list(device_activity_datelist) AS device_activity_datelist,
        browser_type,
        device_type
    FROM events e
    LEFT JOIN devices d ON e.device_id = d.device_id
    WHERE e.device_id IS NOT NULL
    GROUP BY device_id, device_type, browser_type
    """)

def insert_user_devices_cumulated(spark):
    # Query 2 - Insert data into user_devices_cumulated
    spark.sql("""
    INSERT INTO user_devices_cumulated
    SELECT
        d.device_id,
        collect_list(event_time) AS device_activity_datelist,
        d.browser_type,
        d.device_type
    FROM events e
    LEFT JOIN devices d ON e.device_id = d.device_id
    WHERE e.device_id IS NOT NULL
    GROUP BY d.device_id, d.device_type, d.browser_type
    """)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserDevicesCumulated").getOrCreate()
    create_user_devices_cumulated(spark)
    insert_user_devices_cumulated(spark)
    spark.stop()
