-- I'm creating a view called fact_trips in the gold schema where I'm  generating a highly denormalised table which will have all the columns from all the tables
-- I'm doing a join between these three tables
-- 

CREATE OR REPLACE VIEW transportation.gold.fact_trips
AS (
  SELECT
  t.id,
  t.business_date,
  t.city_id,
  c.city_name,
  t.passenger_category,
  t.distance_km,
  t.sales_amt,
  t.passenger_rating,
  t.driver_rating,
  ca.month,
  ca.day_of_month,
  ca.day_of_week,
  ca.month_name,
  ca.month_year,
  ca.quarter,
  ca.quarter_year,
  ca.week_of_year,
  ca.is_weekday,
  ca.is_weekend,
  ca.is_holiday as national_holiday
  FROM
  transportation.silver.trips t
  JOIN transportation.silver.city c ON t.city_id = c.city_id
  JOIN transportation.silver.calendar ca ON t.business_date = ca.date
  );
