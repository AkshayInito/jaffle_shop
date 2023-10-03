with init_data as (
  select 
    cast(a1.app_user_id as string) as iid, 
    cast(m.cust_id as string) as uid, 
    a1.order_number as ooid, 
    created_timestamp, 
    sku, 
    title, 
    price,total_price, 
    financial_status, 
    order_status, 
    quantity 
  from 
    `inito_dev_transformed_ops.fct_shopify_orders` as a1 
    join `inito_dev_transformed_ops.fct_shopify_order_lines` as a2 on a1.order_id = a2.order_id 
    join `inito_dev_transformed_ops.fct_current_order_state` as a3 on a3.order_id = a1.order_id 
    left join (
      select 
        user_id as cust_id, 
        cast(order_id as string) as order_ids 
      from 
        `inito_prod_transformed_analytics.reader_data_user_mapped`
    ) m on order_ids = cast(a1.order_number as string) 
  where 
    1 = 1 
    and currency != 'INR' 
    and title in (
      'Inito Fertility Monitor', 'Fertility Strips'
    ) 
    and shipping_address_country != 'India' 
    and financial_status = 'paid' 
    and order_status != 'CANCELLED'
), 
v1_filter as (
  select 
    distinct coalesce(uid, iid) as cust_id, 
    *, 
    total_price as final_price 
  from 
    init_data 
  where 
    coalesce(uid, iid) is not null
), 
check_multiple_monitor as (
  select 
    cust_id, 
    count(distinct ooid) as users 
  from 
    v1_filter 
  where 
    title = 'Inito Fertility Monitor' 
  group by 
    1 
  having 
    count(distinct ooid) > 1
), 
monitors_one as (
  select 
    a.* 
  from 
    v1_filter as a 
    left join check_multiple_monitor c on c.cust_id = a.cust_id 
  where 
    c.cust_id is null
), 
atleast_one_monitor as (
  select 
    distinct cust_id as users 
  from 
    v1_filter 
  where 
    title = 'Inito Fertility Monitor'
), 
final_p1 as (
  select 
    a1.* 
  from 
    monitors_one as a1 
    join atleast_one_monitor as a2 on a1.cust_id = a2.users 
    join (select cast(id as string) as iiid, role_id from prod_data.users where role_id = 1)  
    on iiid = a1.cust_id
    ), 
first_dt as (
  select 
    cust_id as customer_id, 
    min(created_timestamp) as min_date_time 
  from 
    final_p1 
  group by 
    1
), 

f as 
(
  select * from first_dt 
left join final_p1 on customer_id = cust_id 
where 
    format_date(
    '%Y-%m', 
    date_trunc(min_date_time, month)
  ) = '2022-10' 
)

select cust_id, min_date_time as first_txn_date, ooid as order_number, created_timestamp as txn_time, sku, title, price, total_price from f 

