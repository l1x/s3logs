SELECT
  FROM_UNIXTIME(
    FLOOR(
      TO_UNIXTIME(
        DATE_PARSE("datetime", '%Y-%m-%dT%H:%i:%S')
      ) / 600
    ) * 600
  ) AS ten_minute_window,
  COUNT(*) AS event_count
FROM
  "l1x_logs"."dev"
WHERE
  DATE_PARSE("datetime", '%Y-%m-%dT%H:%i:%S') > DATE_PARSE('2020-10-30', '%Y-%m-%d')
GROUP BY
  1
ORDER BY
  ten_minute_window
