set start_date='2016-10-01';
set end_date=  '2016-10-31';

SELECT
  clid,
  sum(price) AS clicks_price_fishki,
  sum(price*30/100) AS clicks_price_RUB,      -- в рублях
  sum(offer_price) AS offers_price -- в рублях (в теории...)
  count(*) as num
FROM robot_market_logs.clicks
WHERE
  day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  AND nvl(filter, 0) = 0 -- не накрутка
  AND state = 1 -- убираем клики сотрудников яндекса
  AND clid IN (137897, 150644) -- исследуемые clid'ы
  AND distr_type = 2 -- партнёр (4,5 - советник; 1 - дистрибуция)
GROUP BY
  clid