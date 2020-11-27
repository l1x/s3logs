SELECT
  FROM_UNIXTIME(
    FLOOR(
      TO_UNIXTIME(
        DATE_PARSE("datetime", '%Y-%m-%dT%H:%i:%S')
      ) / 300
    ) * 300
  ) AS five_minute_window,
  COUNT(*) AS event_count
FROM
  "l1x_logs"."dev"
GROUP BY
  1
