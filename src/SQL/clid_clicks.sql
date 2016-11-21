SET start_date='2016-11-15';
SET end_date=  '2016-11-15';

SELECT
  clid,
  sum(price*30/100) AS clicks_price,      -- в рублях
  sum(offer_price) AS offers_price, -- в рублях (в теории...)
  COUNT(*) AS num
FROM robot_market_logs.clicks
WHERE
  day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  AND nvl(filter, 0) = 0 -- не накрутка
  AND state = 1 -- убираем клики сотрудников яндекса
  AND clid > 0 -- указан ID партнёра
  AND distr_type = 2 -- партнёр (4,5 - советник; 1 - дистрибуция)
GROUP BY clid
ORDER BY num DESC

--- xxx

SET start_date='2016-11-15';
SET end_date=  '2016-11-15';

SELECT
  clid,
  hyper_cat_id,
  hyper_id,
  price*30/100 AS clicks_price,      -- в рублях
  offer_price AS offers_price, -- в рублях (в теории...)
FROM robot_market_logs.clicks
WHERE
  day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  AND nvl(filter, 0) = 0 -- не накрутка
  AND state = 1 -- убираем клики сотрудников яндекса
  AND clid > 0 -- указан ID партнёра
  AND distr_type = 2 -- партнёр (4,5 - советник; 1 - дистрибуция)
