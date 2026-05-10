SELECT
    CASE
        WHEN etl_datetime IS NULL THEN FALSE
        ELSE TRUE
    END AS flow_exists
FROM `common_property.processing_status`
WHERE data_flow_name = 'DATA_FLOW_NAME'