 SELECT
        PARSE_DATE('%Y%m%d', date) AS page_event_date,
        visitStartTime as timestamp,
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 60)  as specialtyID,
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 11) as locationID, 
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 5) as professionalID, 
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 42) as session_id,
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 61) as persistent_session_id,
        (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 9) as sales_region_id,
        count(distinct (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 134)) as phoneAction,
        count(*) as altphoneAction
      FROM
        `seventh-circle-461.75615261.ga_sessions_*`,
        UNNEST(hits) AS h
      WHERE
        _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
        AND ((SELECT value FROM UNNEST(h.customDimensions) WHERE index = 97) like "%ad_click_type=phone%"
            OR (SELECT value FROM UNNEST(h.customDimensions) WHERE index = 97) like "%tel:%")
       group by page_event_date,timestamp,specialtyID,locationID,professionalID,session_id,persistent_session_id,sales_region_id