
-- Welcome to your first dbt model!
-- Did you know that you can also configure models directly within
-- the SQL file? This will override configurations stated in dbt_project.yml

-- Try changing 'view' to 'table', then re-running dbt
{{ config(materialized='table') }}


with
  t1 as (
    select
      a.name account_name
      , case
        when arr___c <= 0
          then '0'
        when arr___c between 1
        and 6000
          then '1-6000'
        when arr___c between 6001
        and 12000
          then '6001-12000'
        when arr___c between 12001
        and 18000
          then '12001-18000'
        when arr___c between 18001
        and 24000
          then '18001-24000'
        when arr___c > 24000
          then '>24000'
      end as arr_band
      , a.account__id___c org_id
      , p.name
    from
      salesforce._account a
      join salesforce._opportunity o on
        a.id = o.account_id
      join salesforce._opportunity_line_item ol on
        o.id = ol.opportunity_id
      join salesforce._product_2 p on
        p.id = ol.product_2_id
      where
        (
          a.type = 'Customer'
          or a.type = 'Free of Charge Customer'
          or a.type = 'Sub-Account'
        )
        and o.contract__start__date___c <= current_date
      and o.contract__end__date___c >= current_date
      and o.is_deleted is false
      and a.is_deleted is false
      and ol.is_deleted is false
      and o.stage_name = '7 - Closed Won'
  )
  , t2 as (
    select
      account_name
      , arr_band
      , org_id
      , name product_name
      , count(name)
    from
      t1
    group by
      1
      , 2
      , 3
      , 4
    order by
      1
  )
  , t3 as (
    select
      t2.account_name
      , t2.org_id
      , t2.arr_band
      , listagg(product_name, ',') within group(order by product_name) over(partition by t2.account_name) product_list
      , row_number() over(partition by t2.account_name)
    from
      t2
  )
select
  *
from
  t3
where
  row_number = 1
