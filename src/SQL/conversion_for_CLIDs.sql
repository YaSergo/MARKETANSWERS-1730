-- запрос для ClickHouse
-- некорректный, разбираемся в https://st.yandex-team.ru/MARKETANSWERS-1842
SELECT
  CLID,
  SUM(Sign) AS visits,
  SUM(Sign*arrayExists(x -> x IN _goals,Goals.ID)) AS goal_visits
FROM visits_all
WHERE
  CounterID in _counters AND
  StartDate BETWEEN '2016-10-15' AND '2016-11-15' AND
  IsRobot = 0 AND
  CLID > 0
GROUP BY
  CLID