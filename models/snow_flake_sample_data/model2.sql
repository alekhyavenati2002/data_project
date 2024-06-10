{{ config(
    alias = 'query75thsol',
    materialized = 'table') }}


WITH 
web_sales_filt AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_quantity,
        ws_net_paid,
        ws_net_profit,
        ws_sold_date_sk,
        ws_ext_sales_price
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
        cs_sold_date_sk,
        cs_ext_sales_price
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
        ss_sold_date_sk,
        ss_ext_sales_price
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

item_filt AS (
    SELECT 
        i_item_sk,
        i_brand_id,
        i_class_id,
        i_category_id,
        i_manufact_id,
        i_category
    FROM 
         {{ source('snowflake_sample_data', 'item') }}
),

date_dim_filt AS (
    SELECT 
        d_date_sk,
        d_year
    FROM 
         {{ source('snowflake_sample_data', 'date_dim') }}
),

--- CATALOG DATA---
catalog_join as (
SELECT *
FROM catalog_sales_filt 
JOIN item_filt ON i_item_sk=cs_item_sk
JOIN date_dim_filt ON d_date_sk=cs_sold_date_sk
LEFT JOIN catalog_returns_filt ON (cs_order_number=cr_order_number 
AND cs_item_sk=cr_item_sk)
WHERE i_category='Books'
),
catalog as (
SELECT d_year,
i_brand_id,
i_class_id,
i_category_id,
i_manufact_id,
cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt,
cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
FROM catalog_join
),

--- STORE DATA---
store_join as (
select *
FROM store_sales_filt 
JOIN item_filt ON i_item_sk=ss_item_sk
JOIN date_dim_filt ON d_date_sk=ss_sold_date_sk
LEFT JOIN store_returns_filt ON (ss_ticket_number=sr_ticket_number 
AND ss_item_sk=sr_item_sk)
WHERE i_category='Books'
),

store as (
SELECT d_year,
i_brand_id,
i_class_id,
i_category_id,
i_manufact_id,
ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt,
ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
FROM store_join
),

--- WEB DATA ---
web_join as(
select * 
FROM web_sales_filt
JOIN item_filt ON i_item_sk=ws_item_sk
JOIN date_dim_filt ON d_date_sk=ws_sold_date_sk
LEFT JOIN web_returns_filt ON (ws_order_number=wr_order_number 
AND ws_item_sk=wr_item_sk)
WHERE i_category='Books'
),

web as (
SELECT d_year,
i_brand_id,
i_class_id,
i_category_id,
i_manufact_id,
ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt,
ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
FROM web_join 
),
--- SALES DATA---
sales_detail as(
select *from catalog
union
select * from store
union
select * from web

),
---ALL SALES---
all_sales as(
SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
),
---- FINAL DATA ------
Final_data as (SELECT prev_yr.d_year AS prev_year
                          ,curr_yr.d_year AS year
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM all_sales curr_yr, all_sales prev_yr
 WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
   AND curr_yr.i_class_id=prev_yr.i_class_id
   AND curr_yr.i_category_id=prev_yr.i_category_id
   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
   AND curr_yr.d_year=2002
   AND prev_yr.d_year=2002-1
   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
 ORDER BY sales_cnt_diff,sales_amt_diff
 )
 
SELECT  * from Final_data





