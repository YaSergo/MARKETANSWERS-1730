set start_date='2016-11-15';
set end_date=  '2016-11-15';

-- предварительные данные для расчёта средних fee
WITH cpa_offers AS (
  SELECT DISTINCT
    day,
    category_id AS hyper_cat_id,
    model_id AS hyper_id,
    binary_ware_md5 AS ware_md5,
    fee/10000 AS fee
  FROM dictionaries.offers
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    -- если два следующих условия не выполняются, считаю, что предложения некорректные и исключаю их из анализа
    AND binary_price_price IS NOT NULL
    AND fee >= 200
    AND is_cpa = True  -- нас интересуют только CPA предложения

-- расчёт средних fee
), avg_fees_by_hyper_id AS (
  SELECT -- только по оплаченным заказам
    model_hid AS hyper_cat_id,
    model_id AS hyper_id,
    sum(offer_fee * item_count) / sum(item_count) AS avg_fee
  FROM
    analyst.orders_dict
  WHERE
    creation_day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    AND order_is_billed = 1
    AND NOT order_is_fake AND NOT buyer_is_fake AND NOT shop_is_fake -- устраняем фейки из данных
    AND model_id > 0
  GROUP BY
    model_hid,
    model_id

), avg_fees_by_hyper_cat_id AS (
  SELECT
    model_hid AS hyper_cat_id,
    -1 AS hyper_id,
    sum(offer_fee * item_count) / sum(item_count) AS avg_fee
  FROM
    analyst.orders_dict
  WHERE
    creation_day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    AND order_is_billed = 1
    AND NOT order_is_fake AND NOT buyer_is_fake AND NOT shop_is_fake -- устраняем фейки из данных
  GROUP BY
    model_hid

-- клики от партнёров
), parther_clicks AS (
  SELECT
    day,
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
    AND geo_id = 213 -- Москва

-- клики от партнёров расширенные информацией о среднем fee
), parther_clicks_with_fee AS (
  SELECT
    parther_clicks.clid,
    parther_clicks.hyper_cat_id,
    parther_clicks.cpa_type,
    parther_clicks.hyper_id,
    parther_clicks.clicks_price,
    parther_clicks.offers_price,

    cpa_offers.fee AS fee_from_offer,
    avg_fees_by_hyper_id.avg_fee AS avg_fee_by_hyper_id,
    -- чтобы была колонка, на случай, если не получилось привязать avg_fee по hyper_id
    avg_fees_by_hyper_cat_id.avg_fee AS avg_fee_by_hyper_cat_id,
    -- alg1: берём fee для оффера, иначе среднее fee для hyper_id, иначе среднее для категории, иначе fee ставим 0.02
    nvl(nvl(nvl(cpa_offers.fee, avg_fees_by_hyper_id.avg_fee), avg_fees_by_hyper_cat_id.avg_fee), 0.02) AS avg_fee_alg1,
    -- alg2: берём fee для оффера, ставим 0.02 -- предложил Антон
    nvl(cpa_offers.fee, 0.02) AS avg_fee_alg2
  FROM parther_clicks LEFT JOIN avg_fees_by_hyper_id
    ON parther_clicks.hyper_cat_id = avg_fees_by_hyper_id.hyper_cat_id
    AND parther_clicks.hyper_id = avg_fees_by_hyper_id.hyper_id
  LEFT JOIN avg_fees_by_hyper_cat_id
    ON parther_clicks.hyper_cat_id = avg_fees_by_hyper_cat_id.hyper_cat_id
  LEFT JOIN cpa_offers  -- подтягиваем точные значения fee
    ON parther_clicks.ware_md5 = cpa_offers.ware_md5
      AND parther_clicks.day = cpa_offers.day

-- таблица с конверсией партнёров за октябрь 2016 года
-- выгружена из statface: https://nda.ya.ru/3SDjW6
), parthers_conversion AS (
  SELECT
    clid,
    num_purchases,
    conversion/100 AS conversion
  FROM medintsev.MA1730_partners_conversion
  WHERE clid IS NOT NULL

), parther_clicks_with_fee_and_after_results AS (
  SELECT
  -- id партнёра
  parther_clicks_with_fee.clid,
  parthers_conversion.num_purchases,
  parthers_conversion.conversion,

  -- before
  SUM(clicks_price) AS before_cpc_clicks_price,
  SUM(offers_price) AS before_offers_price,
  -- количество кликов
  COUNT(*) AS before_cpc_clicks_num,
  
  -- after
  SUM(IF(cpa_type = 'cpc_and_cpa', clicks_price, 0)) AS after_cpc_clicks_price,
  -- эту часть ещё нужно умножить на конверсию партнёра (clid)
  SUM(IF(cpa_type <> 'cpc_and_cpa', offers_price*avg_fee_alg1, 0)) AS after_offers_price_dot_fee_alg1,
  SUM(IF(cpa_type <> 'cpc_and_cpa', offers_price*avg_fee_alg2, 0)) AS after_offers_price_dot_fee_alg2

  FROM parther_clicks_with_fee LEFT JOIN parthers_conversion
    ON parther_clicks_with_fee.clid = parthers_conversion.clid
  GROUP BY
    parther_clicks_with_fee.clid,
    parthers_conversion.num_purchases,
    parthers_conversion.conversion
)

SELECT
  clid,
  num_purchases, -- количество покупок при расчёте конверсии, не путать с количеством cpc кликов у clid
  conversion,

  before_cpc_clicks_price,
  before_offers_price,
  before_cpc_clicks_num,

  after_cpc_clicks_price,
  after_offers_price_dot_fee_alg1,
  after_offers_price_dot_fee_alg2,

  after_cpc_clicks_price + after_offers_price_dot_fee_alg1 * conversion AS after_total_alg1,
  after_cpc_clicks_price + after_offers_price_dot_fee_alg2 * conversion AS after_total_alg2
FROM parther_clicks_with_fee_and_after_results



