{{ config(
    alias = 'query49thsol',
    materialized = 'table') }}


WITH 
web_sales_filt AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_quantity,
        ws_net_paid,
        ws_net_profit,
        ws_sold_date_sk
    FROM 
        {{ source('snowflake_sample_data', 'web_sales') }}
),

web_returns_filt AS (
    SELECT 
        wr_order_number,
        wr_item_sk,
        wr_return_quantity,
        wr_return_amt
    FROM 
         {{ source('snowflake_sample_data', 'web_returns') }}
),

catalog_sales_filt AS (
    SELECT 
        cs_item_sk,
        cs_order_number,
        cs_quantity,
        cs_net_paid,
        cs_net_profit,
        cs_sold_date_sk
    FROM 
         {{ source('snowflake_sample_data', 'catalog_sales') }}
),

catalog_returns_filt AS (
    SELECT 
        cr_order_number,
        cr_item_sk,
        cr_return_quantity,
        cr_return_amount
    FROM 
         {{ source('snowflake_sample_data', 'catalog_returns') }}
),

store_sales_filt AS (
    SELECT 
        ss_item_sk,
        ss_ticket_number,
        ss_quantity,
        ss_net_paid,
        ss_net_profit,
        ss_sold_date_sk
    FROM 
         {{ source('snowflake_sample_data', 'store_sales') }}
),

store_returns_filt AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_return_quantity,
        sr_return_amt
    FROM 
         {{ source('snowflake_sample_data', 'store_returns') }}
),
date_dim_filt AS (
    SELECT 
        d_date_sk,
        d_year,
        d_moy
    FROM 
         {{ source('snowflake_sample_data', 'date_dim') }}
),


----- WEBSALES DATA--------
websales_join as 
(
 select * 
 from web_sales_filt ws 
 left outer join  web_returns_filt wr 
 on (ws.ws_order_number = wr.wr_order_number and 
 ws.ws_item_sk = wr.wr_item_sk)
 ,date_dim_filt
 where 
 wr.wr_return_amt > 10000 
 and ws.ws_net_profit > 1
 and ws.ws_net_paid > 0
 and ws.ws_quantity > 0
 and ws_sold_date_sk = d_date_sk
 and d_year = 2001
 and d_moy = 12
 ),
 
in_web as (select ws_item_sk as item
 ,(cast(sum(coalesce(wr_return_quantity,0)) as decimal(15,4))/
 cast(sum(coalesce(ws_quantity,0)) as decimal(15,4) )) as return_ratio
 ,(cast(sum(coalesce(wr_return_amt,0)) as decimal(15,4))/
 cast(sum(coalesce(ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 from websales_join 
 group by ws_item_sk
 ),

web as( select 
 item
 ,return_ratio
 ,currency_ratio
 ,rank() over (order by return_ratio) as return_rank
 ,rank() over (order by currency_ratio) as currency_rank
 from in_web),

web_sale_data as (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from web
 where web.return_rank <= 10 
 or web.currency_rank <= 10
 ),
 
------- CATALOGSALES DATA---------
catalogsales_join as (select *
 from 
 catalog_sales_filt cs 
 left outer join catalog_returns_filt cr
 on (cs.cs_order_number = cr.cr_order_number and 
 cs.cs_item_sk = cr.cr_item_sk)
 ,date_dim_filt
 where 
 cr.cr_return_amount > 10000 
 and cs.cs_net_profit > 1
 and cs.cs_net_paid > 0
 and cs.cs_quantity > 0
 and cs_sold_date_sk = d_date_sk
 and d_year = 2001
 and d_moy = 12
 ),
 in_cat as (select 
 cs_item_sk as item
 ,(cast(sum(coalesce(cr_return_quantity,0)) as decimal(15,4))/
 cast(sum(coalesce(cs_quantity,0)) as decimal(15,4) )) as return_ratio
 ,(cast(sum(coalesce(cr_return_amount,0)) as decimal(15,4))/
 cast(sum(coalesce(cs_net_paid,0)) as decimal(15,4) )) as currency_ratio
 from catalogsales_join 
 group by cs_item_sk
 ) ,

 catalog as (select 
 item
 ,return_ratio
 ,currency_ratio
 ,rank() over (order by return_ratio) as return_rank
 ,rank() over (order by currency_ratio) as currency_rank
 from in_cat),

catalog_sale_data as ( select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from catalog
 where catalog.return_rank <= 10
 or catalog.currency_rank <=10
 ),
 
-- STORESALES DATA
 storesales_join as (select *
 from store_sales_filt sts
 left outer join store_returns_filt sr
 on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
 ,date_dim_filt
 where 
 sr.sr_return_amt > 10000 
 and sts.ss_net_profit > 1
 and sts.ss_net_paid > 0 
 and sts.ss_quantity > 0
 and ss_sold_date_sk = d_date_sk
 and d_year = 2001
 and d_moy = 12
 ),

 in_store as (select ss_item_sk as item
 ,(cast(sum(coalesce(sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(ss_quantity,0)) as decimal(15,4) )) as return_ratio
 ,(cast(sum(coalesce(sr_return_amt,0)) as decimal(15,4))/cast(sum(coalesce(ss_net_paid,0)) as decimal(15,4) )) as currency_ratio
 from storesales_join
 group by ss_item_sk
 ) ,

 store as (select 
 item
 ,return_ratio
 ,currency_ratio
 ,rank() over (order by return_ratio) as return_rank
 ,rank() over (order by currency_ratio) as currency_rank
 from in_store
 ),

 store_sale_data as (select 
 'store' as channel
 ,store.item
 ,store.return_ratio
 ,store.return_rank
 ,store.currency_rank
 from store
 where store.return_rank <= 10
 or store.currency_rank <= 10
 ),
-- final_filtered_data
final_data AS (
    SELECT * FROM web_sale_data
    UNION
    SELECT * FROM catalog_sale_data
    UNION
    SELECT * FROM store_sale_data
)
 
-- Selecting the final result
SELECT channel, item, return_ratio, return_rank, currency_rank 
FROM final_data ORDER BY 1, 4, 5, 2




 
 



 


 

