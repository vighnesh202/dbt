    {{ config(
   materialized = 'incremental',
    pre_hook=[
      "TRUNCATE TABLE {{source('dbt_workdb','recursive_result')}} "
    ]
   ) 
}}

SELECT
    *
  FROM
  {{ ref('recursive_model') }} AS nptrv
  
  
