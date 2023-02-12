/*Give the count of the minimum number of days for the time when temperature reduced*/

WITH cte AS (
  SELECT date, temperature, 
         LAG(temperature) OVER (ORDER BY date) AS prev_temp
  FROM weather.weather_project
)
SELECT COUNT(date) 
FROM cte
WHERE temperature < prev_temp;

/*Find the temperature as Cold / hot by using the case and avg of values of the given data set*/

select date,
case when (temperature>avg_temp) then "hot" else "cold" end as "Cold/hot"
from
(
SELECT date,temperature,avg(temperature) as avg_temp
FROM weather.weather_project
group by date
)x
;

/*Can you check for all 4 consecutive days when the temperature was below 30 Fahrenheit*/

WITH cte AS (
  SELECT date,  Minimum_temperature, 
         ROW_NUMBER() OVER (ORDER BY date) AS row_num
  FROM weather.weather_project
)
SELECT MIN(date) AS start_date, MAX(date) AS end_date
FROM (
  SELECT 
    date, Minimum_temperature, row_num,
    SUM(CASE WHEN Minimum_temperature < 30 THEN 1 ELSE 0 END) 
      OVER (ORDER BY row_num ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS count_below_30
  FROM cte
) t
WHERE count_below_30 = 4
group by date;


/*Can you find the maximum number of days for which temperature dropped*/

WITH temperatures AS (
  SELECT date, temperature, 
         LAG(temperature) OVER (ORDER BY date) AS prev_temp,
         ROW_NUMBER() OVER (ORDER BY date) AS row_num
  FROM weather.weather_project
), drops AS (
  SELECT date, temperature, prev_temp, row_num,
         SUM(CASE WHEN temperature < prev_temp THEN 1 ELSE 0 END) 
           OVER (ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_drops
  FROM temperatures
)
SELECT MAX(count_drops)
FROM drops;

/*Can you find the average of average humidity from the dataset*/

select date,round(avg(average_humidity),2)
from weather.weather_project
group by date
order by date;

/*If the maximum gust speed increases from 55mph, fetch the details for the next 4 days*/


WITH wind AS (
  SELECT date,  Maximum_gust_speed, 
         LAG( Maximum_gust_speed) OVER (ORDER BY date) AS prev_max_gust_speed,
         ROW_NUMBER() OVER (ORDER BY date) AS row_num
  FROM weather.weather_project
)
SELECT date,  Maximum_gust_speed
FROM wind
WHERE  Maximum_gust_speed > 55 
  AND prev_max_gust_speed <= 55
;

/*Find the number of days when the temperature went below 0 degrees Celsius */

select count(date)
from weather.weather_project
where Minimum_temperature<0;

