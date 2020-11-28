SELECT
  FROM_UNIXTIME(
    FLOOR(
      TO_UNIXTIME(
        DATE_PARSE("datetime", '%Y-%m-%dT%H:%i:%S')
      ) / 600
    ) * 600
  ) AS ten_minute_window,
  COUNT(1) AS event_count
FROM
  dwh.web_logs
WHERE
  DATE_PARSE("datetime", '%Y-%m-%dT%H:%i:%S') > DATE_PARSE('2020-10-30', '%Y-%m-%d')
GROUP BY
  1
ORDER BY
  ten_minute_window
