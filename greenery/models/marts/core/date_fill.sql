{{
  config(
    materialized='table'
  )
}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('01/01/2019', 'mm/dd/yyyy')",
    end_date="to_date('01/01/2027', 'mm/dd/yyyy')"
   )
}} AS date