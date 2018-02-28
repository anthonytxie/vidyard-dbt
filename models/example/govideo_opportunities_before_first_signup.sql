
{{ config(materialized='table') }}

with
  t1 as (
    select
      split_part(u.email, '@', '2') as domain
      , o.parent_id
      , min(o.created_at) first_sign_up_date
    from
      viewedit_organizations o
      join viewedit_users u on
        u.id = o.owner_id
    where
      o.org_type = 'self_serve'
      and u.email not like '%vidyard%'
    group by
      1
      , 2
  )
  , t2 as (
    select
      domain
      , min(first_sign_up_date) first_sign_up_date
      , min(o.created_date) first_opp_created_date
    from
      t1
      join salesforce._account a on
        a.website = t1.domain
      join salesforce._opportunity o on
        o.account_id = a.id
      join salesforce._opportunity_line_item ol on
        o.id = ol.opportunity_id
    where
      product_code = 'ENG-101'
      and o.is_deleted is false
      and a.is_deleted is false
      and ol.is_deleted is false
    group by
      1
  )
select
  *
from
  t2
where first_sign_up_date > first_opp_created_date
