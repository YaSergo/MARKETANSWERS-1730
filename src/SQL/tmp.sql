set start_date='2016-11-15';
set end_date=  '2016-11-15';

SELECT
  cpc_clicks.clid,
  cpc_clicks.hyper_cat_id,
  categories_details.cpa_type,
  cpc_clicks.hyper_id,
  cpc_clicks.ware_md5,
  cpc_clicks.price*30/100 AS clicks_price,      -- в рублях
  cpc_clicks.offer_price AS offers_price -- в рублях (в теории...)
FROM robot_market_logs.clicks as cpc_clicks LEFT JOIN dictionaries.categories as categories_details
  ON cpc_clicks.hyper_cat_id = categories_details.hyper_id
WHERE
  day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  AND nvl(filter, 0) = 0 -- не накрутка
  AND state = 1 -- убираем клики сотрудников яндекса
  AND clid > 0 -- указан ID партнёра
  AND distr_type = 2 -- партнёр (4,5 - советник; 1 - дистрибуция)