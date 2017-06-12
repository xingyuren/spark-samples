USE hr;

-----TV总览——地区分布 月
--缓存终端数信息
CACHE TABLE tracker_total_tv_overview_partition_cache AS
SELECT DATE AS t_today,
'monthly' AS t_datetype,
province AS t_province,
behavior_type AS t_name,
SUM(terminal_cnt) AS t_acnt
FROM hr.tracker_total_tv_overview_partition
WHERE period='monthly'
AND DATE BETWEEN timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, -65, 'yyyy-MM-dd') AND timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, 0, 'yyyy-MM-dd')
GROUP BY DATE,
behavior_type,
province;

-- 缓存一个月的日期
CACHE TABLE dateinfo_cache AS
SELECT
  today,
  MONTH,
  YEAR
FROM hr.dateinfo
WHERE YEAR = '2016'
      AND MONTH = '12';

--结果
--开关机

SELECT
  alltime.t_today                                      AS t_today,
  alltime.t_datetype                                   AS t_datetype,
  '未查询'                                                AS t_eara,
  alltime.t_province                                   AS t_province,
  '智能电视开机'                                             AS t_name,
  allterminal.t_acnt,
  ROUND(alltime.t_tcnt / 3600, 2)                      AS t_tcnt,
  alltime.t_ucnt,
  ROUND(alltime.t_tcnt / 3600 / allterminal.t_acnt, 2) AS t_tcntavgclient
FROM
  (SELECT
     octime2.t_today     AS t_today,
     'monthly'           AS t_datetype,
     terminal.province   AS t_province,
     '智能电视开机'            AS t_name,
     SUM(octime2.t_ucnt) AS t_ucnt,
     SUM(octime2.t_tcnt) AS t_tcnt
   FROM
     (SELECT
        sn,
        MIN(DATE)                              AS t_today,
        'monthly'                              AS t_datetype,
        '智能电视开机'                               AS t_name,
        SUM(COALESCE(cnt, 0))                  AS t_ucnt,
        SUM(COALESCE(power_on_length * 60, 0)) AS t_tcnt
      FROM
        (SELECT sn,
         DATE,
        SUM(cnt) AS cnt,
                 SUM(power_on_length) AS power_on_length
                                      FROM hr.tracker_total_oc_fact_partition
                                      WHERE DATE BETWEEN timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, -65, 'yyyy-MM-dd') AND timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, 0, 'yyyy-MM-dd')
         GROUP BY sn,
         DATE) octime
        INNER JOIN dateinfo_cache ON (octime.date = dateinfo_cache.today)
      GROUP BY sn,
        dateinfo_cache.year,
        dateinfo_cache.month) octime2
     INNER JOIN
     (SELECT DISTINCT
        sn,
        province
      FROM terminal_partition) terminal ON (octime2.sn = terminal.sn)
   GROUP BY t_today,
     terminal.province) alltime
  JOIN
  (SELECT
     t_today,
     t_datetype,
     t_province,
     t_name,
     t_acnt
   FROM tracker_total_tv_overview_partition_cache
   WHERE t_name = '智能电视开机') allterminal ON (alltime.t_today = allterminal.t_today
                                            AND alltime.t_province = allterminal.t_province
                                            AND alltime.t_name = allterminal.t_name
                                            AND alltime.t_datetype = allterminal.t_datetype)
UNION ALL --直播

SELECT
  alltime.t_today                                      AS t_today,
  alltime.t_datetype                                   AS t_datetype,
  '未查询'                                                AS t_eara,
  alltime.t_province                                   AS t_province,
  '直播端'                                                AS t_name,
  allterminal.t_acnt,
  ROUND(alltime.t_tcnt / 3600, 2)                      AS t_tcnt,
  alltime.t_ucnt,
  ROUND(alltime.t_tcnt / 3600 / allterminal.t_acnt, 2) AS t_tcntavgclient
FROM
  (SELECT
     livetime2.t_today     AS t_today,
     'monthly'             AS t_datetype,
     terminal.province     AS t_province,
     '直播'                  AS t_name,
     SUM(livetime2.t_ucnt) AS t_ucnt,
     SUM(livetime2.t_tcnt) AS t_tcnt
   FROM
     (SELECT
        dim_sn,
        MIN(DATE)                          AS t_today,
        'monthly'                          AS t_datetype,
        '直播'                               AS t_name,
        SUM(COALESCE(fact_cnt, 0))         AS t_ucnt,
        SUM(COALESCE(fact_time_length, 0)) AS t_tcnt
      FROM
        (SELECT dim_sn,
         DATE,
        SUM(fact_cnt) AS fact_cnt,
                      SUM(fact_time_length) AS fact_time_length
                                            FROM hr.tracker_total_live_fact_partition
                                            WHERE DATE BETWEEN timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, -65, 'yyyy-MM-dd') AND timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, 0, 'yyyy-MM-dd')
         GROUP BY dim_sn,
         DATE) livetime
        INNER JOIN dateinfo_cache ON (livetime.date = dateinfo_cache.today)
      GROUP BY dim_sn,
        dateinfo_cache.year,
        dateinfo_cache.month) livetime2
     INNER JOIN
     (SELECT DISTINCT
        sn,
        province
      FROM terminal_partition) terminal ON (livetime2.dim_sn = terminal.sn)
   GROUP BY t_today,
     terminal.province) alltime
  JOIN
  (SELECT
     t_today,
     t_datetype,
     t_province,
     t_name,
     t_acnt
   FROM tracker_total_tv_overview_partition_cache
   WHERE t_name = '直播') allterminal ON (alltime.t_today = allterminal.t_today
                                        AND alltime.t_province = allterminal.t_province
                                        AND alltime.t_name = allterminal.t_name
                                        AND alltime.t_datetype = allterminal.t_datetype)
UNION ALL --apk

SELECT
  alltime.t_today                                             AS t_today,
  alltime.t_datetype                                          AS t_datetype,
  '未查询'                                                       AS t_eara,
  alltime.t_province                                          AS t_province,
  'OTT端'                                                      AS t_name,
  allterminal.t_acnt,
  ROUND(alltime.t_tcnt * 1.84 / 3600, 2)                      AS t_tcnt,
  alltime.t_ucnt * 1.84,
  ROUND(alltime.t_tcnt * 1.84 / 3600 / allterminal.t_acnt, 2) AS t_tcntavgclient
FROM
  (SELECT
     MIN(DATE)                       AS t_today,
     'monthly'                       AS t_datetype,
     terminal.province               AS t_province,
     'OTT'                           AS t_name,
     SUM(COALESCE(fact_cnt, 0))      AS t_ucnt,
     SUM(COALESCE(fact_duration, 0)) AS t_tcnt
   FROM
     (SELECT dim_sn,
      DATE,
     SUM(fact_cnt) AS fact_cnt,
                   SUM(fact_duration) AS fact_duration
                                      FROM hr.tracker_total_apk_fact_partition
                                      WHERE DATE BETWEEN timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, -65, 'yyyy-MM-dd') AND timeFormat(#{execDate}, 'yyyy-MM-dd', 0, 0, 0, 'yyyy-MM-dd')
      GROUP BY dim_sn,
      DATE) octime
     JOIN dateinfo_cache ON (octime.date = dateinfo_cache.today)
     JOIN
     (SELECT DISTINCT
        sn,
        province
      FROM terminal_partition) terminal ON (octime.dim_sn = terminal.sn)
   GROUP BY terminal.province,
     dateinfo_cache.year,
     dateinfo_cache.month) alltime
  JOIN
  (SELECT
     t_today,
     t_datetype,
     t_province,
     t_name,
     t_acnt
   FROM tracker_total_tv_overview_partition_cache
   WHERE t_name = 'OTT') allterminal ON (alltime.t_today = allterminal.t_today
                                         AND alltime.t_province = allterminal.t_province
                                         AND alltime.t_name = allterminal.t_name
                                         AND alltime.t_datetype = allterminal.t_datetype);