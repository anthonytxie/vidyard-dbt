
{{ config(materialized='table') }}

with
  t1 as (
    select
      a.id account_id
      , a.name account_name
      , a.account__id___c vidyard_account_id
      , o.id opp_id
      , ol.id opp_line_item_id
      , o.contract__start__date___c contract_start_date
      , o.contract__end__date___c contract_end_date
      , a.renewal__date___c renewal_date
      , ae.first_name + ' ' + ae.last_name account_owner
      , csm.first_name + ' ' + csm.last_name csm_owner
      , launch.first_name + ' ' + launch.last_name launch_owner
      , ol.quantity number_of_seats
    from
      salesforce._account a
      join salesforce._opportunity o on
        o.account_id = a.id
      join salesforce._opportunity_line_item ol on
        o.id = ol.opportunity_id
      join salesforce._product_2 p on
        p.id = ol.product_2_id
      left join salesforce._user ae on
        ae.id = a.owner_id
      left join salesforce._user csm on
        csm.id = a.csm__owner___c
      left join salesforce._user launch on
        launch.id = a.launch_manager_c
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
      and p.product_code = 'ENG-101'
  )
  , t2 as (
    select
      account_name
      , account_id
      , vidyard_account_id parent_id
      , max(contract_start_date) contract_start_date
      , max(contract_end_date) contract_end_date
      , max(renewal_date) renewal_date
      , max(account_owner) account_owner
      , max(csm_owner) csm_owner
      , max(launch_owner) launch_owner
      , sum(number_of_seats) seats_purchased
    from
      t1
    group by
      1
      , 2
      , 3
  )
  , t3 as (
    select
      t2.account_id
      , min(o.contract__start__date___c) first_contract_start_date
    from
      salesforce._account a
      join t2 on
        t2.account_id = a.id
      join salesforce._opportunity o on
        o.account_id = a.id
      join salesforce._opportunity_line_item ol on
        o.id = ol.opportunity_id
      join salesforce._product_2 p on
        p.id = ol.product_2_id
    where
      o.is_deleted is false
      and a.is_deleted is false
      and ol.is_deleted is false
      and o.stage_name = '7 - Closed Won'
      and p.product_code = 'ENG-101'
    group by
      1
  )
  
select
  account_name
  , t2.account_id
  , parent_id
  , contract_start_date
  , contract_end_date
  , renewal_date
  , account_owner
  , csm_owner
  , launch_owner
  , seats_purchased
  , first_contract_start_date
from
  t2
  join t3 on
    t2.account_id = t3.account_id
select * from t2
order by 1
