----------------------------------
-- PARAMS
----------------------------------

WITH PARAMS AS (
    SELECT 
        60 AS days_rolling_window
        ,7 AS days_forecast_range
        ,2 AS years_lookback
),

----------------------------------
-- GENERATE DATES
----------------------------------

DATETIMES AS (
    SELECT 
        generate_series(
            (current_date - (p.years_lookback || ' years')::INTERVAL - (p.days_rolling_window || ' days')::INTERVAL)::TIMESTAMP,
            (current_date + (p.days_rolling_window || ' days')::INTERVAL)::DATE,
            INTERVAL '1 hour'
        )::TIMESTAMP AS datetime
    FROM PARAMS p
),

ROLLING_DATETIMES AS (
    SELECT 
        
        -- YEAR
        EXTRACT(YEAR FROM datetime) AS year
        ,CONCAT(EXTRACT(YEAR FROM datetime), '-', EXTRACT(MONTH FROM datetime)) as year_month

        -- SUMMER / WINTER
        ,CASE
            WHEN EXTRACT(month from datetime) in (11, 12, 1, 2, 3) then 'WINTER'
            WHEN EXTRACT(month from datetime) in (4, 5, 6, 7, 8, 9, 10) then 'SUMMER'
            ELSE NULL
        END AS summer_winter
        ,CASE
            WHEN EXTRACT(month from datetime) in (11, 12, 1, 2, 3) then 'XH-' || right(EXTRACT(year from datetime)::text, 2)
            WHEN EXTRACT(month from datetime) in (4, 5, 6, 7, 8, 9, 10) then 'JV-' || right(EXTRACT(year from datetime)::text, 2)
            ELSE NULL
        END AS summer_winter_yyyy

        -- MONTH
        ,EXTRACT(MONTH FROM datetime) AS month
        ,TO_CHAR(datetime, 'MM-DD') AS mm_dd
        ,(EXTRACT(YEAR FROM current_date)::VARCHAR || '-' || TO_CHAR(datetime::date, 'MM-DD'))::DATE as mm_dd_cy

        -- EIA WEEKS
        ,(datetime::date + 
            CASE 
                WHEN (EXTRACT(DOW FROM datetime::date) - 5) >= 0 
                THEN -(EXTRACT(DOW FROM datetime::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM datetime::date) - 5)
            END * INTERVAL '1 day')::date as eia_storage_week
        ,EXTRACT(WEEK FROM (datetime::date + 
            CASE 
                WHEN (EXTRACT(DOW FROM datetime::date) - 5) >= 0 
                THEN -(EXTRACT(DOW FROM datetime::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM datetime::date) - 5)
            END * INTERVAL '1 day')::date) as eia_storage_week_number

        -- DAILY
        ,datetime
        ,datetime::date as date
        ,EXTRACT(hour from datetime) + 1 as hour_ending
        
        -- WEEKENDS/HOLIDAYS
        ,TRIM(TO_CHAR(datetime::date, 'Day')) AS day_of_week
        ,EXTRACT(DOW FROM datetime::date) AS day_of_week_number

        ,CASE 
            WHEN EXTRACT(DOW FROM datetime::date) IN (0, 6) THEN 1  --'WEEKEND'
            ELSE 0  --'WEEKDAY'
        END AS is_weekend
        ,CASE 
            WHEN datetime::date IN (SELECT nerc_holiday FROM misc.nerc_holidays) THEN 1  --'NERC Holiday'
            ELSE 0  --'No Holiday'
        END AS is_nerc_holiday

    FROM DATETIMES
    CROSS JOIN PARAMS p
    WHERE 
        -- days_rolling_window forward and backward
        TO_CHAR(datetime, 'MM-DD') >= TO_CHAR(current_date - (p.days_rolling_window || ' days')::INTERVAL, 'MM-DD') 
        AND 
        TO_CHAR(datetime, 'MM-DD') <= TO_CHAR(current_date + (p.days_rolling_window || ' days')::INTERVAL, 'MM-DD') 
        -- forecast range constraint
        AND datetime::date <= current_date + p.days_forecast_range
),

DATETIMES_FINAL AS (
    SELECT
        -- YEAR
        year
        ,year_month

        -- SUMMER / WINTER
        ,summer_winter
        ,summer_winter_yyyy

        -- MONTH
        ,month
        ,mm_dd
        ,mm_dd_cy

        -- EIA WEEKS
        ,eia_storage_week
        ,eia_storage_week_number

        -- DAILY
        ,datetime
        ,date
        ,hour_ending
        
        -- WEEKENDS/HOLIDAYS
        ,day_of_week
        ,day_of_week_number
        ,is_weekend
        ,is_nerc_holiday
    
    FROM ROLLING_DATETIMES
),

-- SELECT * FROM DATETIMES_FINAL
-- WHERE mm_dd = TO_CHAR(current_date, 'MM-DD')
-- ORDER BY datetime DESC

----------------------------------
-- ACTUALS
----------------------------------

ACTUALS AS (
    select  
        
        date
        ,hour_ending
    
        ,da_lmp_total_western_hub
        ,rt_lmp_total_western_hub
        ,dart_lmp_total_western_hub

        ,da_lmp_system_energy_price_western_hub
        ,rt_lmp_system_energy_price_western_hub
        ,dart_lmp_system_energy_price_western_hub

        ,da_lmp_congestion_price_western_hub
        ,rt_lmp_congestion_price_western_hub
        ,dart_lmp_congestion_price_western_hub

        ,da_lmp_marginal_loss_price_western_hub
        ,rt_lmp_marginal_loss_price_western_hub
        ,dart_lmp_marginal_loss_price_western_hub
    
        ,next_day_gas_hh
        ,next_day_gas_m3
        ,next_day_gas_z5
        ,next_day_gas_m3_basis
        ,next_day_gas_z5_basis
    
        ,da_load_rto
        ,da_load_west
        ,da_load_midatl
        ,da_load_south
    
        ,rt_load_rto
        ,rt_load_west
        ,rt_load_midatl
        ,rt_load_south

        ,(rt_load_rto - wind - solar) as net_load_rto
    
        ,supply
        ,thermal
        ,renewables
        ,gas_pct_thermal
        ,coal_pct_thermal
        ,coal
        ,gas
        ,hydro
        ,multiple_fuels
        ,nuclear
        ,oil
        ,solar
        ,storage
        ,wind
        ,other
        ,other_renewables
    
        ,tie_flows_actual_pjm_rto
        ,tie_flows_scheduled_pjm_rto
    
        ,total_outages_mw_pjm
        ,planned_outages_mw_pjm
        ,maintenance_outages_mw_pjm
        ,forced_outages_mw_pjm
    
    from pjm.aggregated_western_hub_hourly_v2_2025_oct_15
    WHERE date IN (SELECT date from DATETIMES_FINAL)
),

-- SELECT

--     date
--     ,hour_ending

--     -- TARGET
--     ,da_lmp_total_western_hub
    
--     -- PREDICTORS
--     ,rt_load_rto
--     ,solar
--     ,wind
--     ,total_outages_mw_pjm

-- FROM ACTUALS
-- WHERE date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
-- ORDER BY DATE desc, HOUR_ENDING desc

-- ----------------------------------
-- -- FORECASTS
-- ----------------------------------

FORECASTS AS (
    select  

        forecast_date
        ,hour_ending

        ,rto_combined as rt_load_rto_forecast
        ,mid_atlantic_region as rt_load_west_forecast
        ,western_region as rt_load_midatl_forecast
        ,southern_region as rt_load_south_forecast

        ,net_load_rto as net_load_rto_forecast

        ,solar_forecast as solar_forecast
        ,solar_forecast_btm as solar_btm_forecast
        ,wind_forecast as wind_forecast

        ,total_outages_mw_rto as total_outages_mw_rto_forecast
        ,planned_outages_mw_rto as planned_outages_mw_rto_forecast
        ,maintenance_outages_mw_rto as maintenance_outages_mw_rto_forecast
        ,forced_outages_mw_rto as forced_outages_mw_rto_forecast

    from pjm.forecast_pjm_hourly_v3_2025_oct_16
),

-- SELECT 
    
--     forecast_date
--     ,hour_ending

--     -- PREDICTORS
--     ,rt_load_rto_forecast
--     ,solar_forecast
--     ,wind_forecast
--     ,total_outages_mw_rto_forecast

-- FROM FORECASTS
-- WHERE forecast_date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
-- ORDER BY forecast_date, hour_ending

-- ----------------------------------
-- COMBINED
-- ----------------------------------

COMBINED AS (
    SELECT
        -- YEAR
        dates.year
        ,dates.year_month

        -- SUMMER / WINTER
        ,dates.summer_winter
        ,dates.summer_winter_yyyy

        -- MONTH
        ,dates.month
        ,dates.mm_dd
        ,dates.mm_dd_cy

        -- EIA WEEKS
        ,dates.eia_storage_week
        ,dates.eia_storage_week_number

        -- DAILY
        ,dates.datetime
        ,dates.date
        ,dates.hour_ending
        
        -- WEEKENDS/HOLIDAYS
        ,dates.day_of_week
        ,dates.day_of_week_number
        ,dates.is_weekend
        ,dates.is_nerc_holiday

        -- COMBINED
        ,COALESCE(actuals.da_lmp_total_western_hub, 0.0) as da_lmp_total_western_hub
        ,COALESCE(actuals.rt_lmp_total_western_hub, 0.0) as rt_lmp_total_western_hub
        ,COALESCE(actuals.dart_lmp_total_western_hub, 0.0) as dart_lmp_total_western_hub

        ,COALESCE(actuals.da_lmp_system_energy_price_western_hub, 0.0) as da_lmp_system_energy_price_western_hub
        ,COALESCE(actuals.rt_lmp_system_energy_price_western_hub, 0.0) as rt_lmp_system_energy_price_western_hub
        ,COALESCE(actuals.dart_lmp_system_energy_price_western_hub, 0.0) as dart_lmp_system_energy_price_western_hub

        ,COALESCE(actuals.da_lmp_congestion_price_western_hub, 0.0) as da_lmp_congestion_price_western_hub
        ,COALESCE(actuals.rt_lmp_congestion_price_western_hub, 0.0) as rt_lmp_congestion_price_western_hub
        ,COALESCE(actuals.dart_lmp_congestion_price_western_hub, 0.0) as dart_lmp_congestion_price_western_hub

        ,COALESCE(actuals.da_lmp_marginal_loss_price_western_hub, 0.0) as da_lmp_marginal_loss_price_western_hub
        ,COALESCE(actuals.rt_lmp_marginal_loss_price_western_hub, 0.0) as rt_lmp_marginal_loss_price_western_hub
        ,COALESCE(actuals.dart_lmp_marginal_loss_price_western_hub, 0.0) as dart_lmp_marginal_loss_price_western_hub
    
        ,COALESCE(actuals.next_day_gas_hh, 0.0) as next_day_gas_hh
        ,COALESCE(actuals.next_day_gas_m3, 0.0) as next_day_gas_m3
        ,COALESCE(actuals.next_day_gas_z5, 0.0) as next_day_gas_z5
        ,COALESCE(actuals.next_day_gas_m3_basis, 0.0) as next_day_gas_m3_basis
        ,COALESCE(actuals.next_day_gas_z5_basis, 0.0) as next_day_gas_z5_basis
    
        ,COALESCE(actuals.da_load_rto, 0.0) as da_load_rto
        ,COALESCE(actuals.da_load_west, 0.0) as da_load_west
        ,COALESCE(actuals.da_load_midatl, 0.0) as da_load_midatl
        ,COALESCE(actuals.da_load_south, 0.0) as da_load_south
    
        ,COALESCE(actuals.rt_load_rto, forecasts.rt_load_rto_forecast) as rt_load_rto
        ,COALESCE(actuals.rt_load_west, forecasts.rt_load_west_forecast) as rt_load_west
        ,COALESCE(actuals.rt_load_midatl, forecasts.rt_load_midatl_forecast) as rt_load_midatl
        ,COALESCE(actuals.rt_load_south, forecasts.rt_load_south_forecast) as rt_load_south
    
        ,COALESCE(actuals.net_load_rto, forecasts.net_load_rto_forecast) as net_load_rto

        ,COALESCE(actuals.supply, 0.0) as supply
        ,COALESCE(actuals.thermal, 0.0) as thermal
        ,COALESCE(actuals.renewables, 0.0) as renewables
        ,COALESCE(actuals.gas_pct_thermal, 0.0) as gas_pct_thermal
        ,COALESCE(actuals.coal_pct_thermal, 0.0) as coal_pct_thermal
        ,COALESCE(actuals.coal, 0.0) as coal
        ,COALESCE(actuals.gas, 0.0) as gas
        ,COALESCE(actuals.hydro, 0.0) as hydro
        ,COALESCE(actuals.multiple_fuels, 0.0) as multiple_fuels
        ,COALESCE(actuals.nuclear, 0.0) as nuclear
        ,COALESCE(actuals.oil, 0.0) as oil
        ,COALESCE(actuals.solar, forecasts.solar_forecast) as solar
        ,COALESCE(actuals.storage, 0.0) as storage
        ,COALESCE(actuals.wind, forecasts.wind_forecast) as wind
        ,COALESCE(actuals.other, 0.0) as other
        ,COALESCE(actuals.other_renewables, 0.0) as other_renewables
    
        ,COALESCE(actuals.tie_flows_actual_pjm_rto, 0.0) as tie_flows_actual_pjm_rto
        ,COALESCE(actuals.tie_flows_scheduled_pjm_rto, 0.0) as tie_flows_scheduled_pjm_rto
    
        ,COALESCE(actuals.total_outages_mw_pjm, forecasts.total_outages_mw_rto_forecast) as total_outages_mw_pjm
        ,COALESCE(actuals.planned_outages_mw_pjm, forecasts.planned_outages_mw_rto_forecast) as planned_outages_mw_pjm
        ,COALESCE(actuals.maintenance_outages_mw_pjm, forecasts.maintenance_outages_mw_rto_forecast) as maintenance_outages_mw_pjm
        ,COALESCE(actuals.forced_outages_mw_pjm, forecasts.forced_outages_mw_rto_forecast) as forced_outages_mw_pjm
    
    FROM DATETIMES_FINAL dates
    LEFT JOIN ACTUALS actuals ON dates.date = actuals.date AND dates.hour_ending = actuals.hour_ending
    LEFT JOIN FORECASTS forecasts ON dates.date = forecasts.forecast_date AND dates.hour_ending = forecasts.hour_ending
)

-- SELECT

--     date
--     ,hour_ending

--     -- TARGET
--     ,da_lmp_total_western_hub
    
--     -- PREDICTORS
--     ,rt_load_rto
--     ,rt_load_west
--     ,rt_load_midatl
--     ,rt_load_south

--     ,net_load_rto
--     ,solar
--     ,wind

--     ,total_outages_mw_pjm
--     ,planned_outages_mw_pjm
--     ,maintenance_outages_mw_pjm
--     ,forced_outages_mw_pjm

-- FROM COMBINED
-- WHERE date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE + 1
-- ORDER BY DATE desc, HOUR_ENDING desc

----------------------------------
----------------------------------

SELECT * FROM COMBINED
ORDER BY DATE desc, HOUR_ENDING desc