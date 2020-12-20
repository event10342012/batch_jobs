with temp as (
    select datetime,
           commodity_id,
           min(expired_date) as expired_date
    from trading.futures.txn_stage
    where datetime >= now() - interval '30 days' and length(expired_date) = 6
    group by datetime, commodity_id
),
     main as (
         select temp.datetime,
                temp.commodity_id,
                txn_stage.open_price,
                txn_stage.high_price,
                txn_stage.low_price,
                txn_stage.close_price,
                txn_stage.volume
         from temp
                  left join trading.futures.txn_stage
                            on txn_stage.datetime = temp.datetime and txn_stage.commodity_id = temp.commodity_id and
                               txn_stage.expired_date = temp.expired_date
     )

insert
into trading.futures.txn (datetime, commodity_id, open_price, high_price, low_price, close_price, volume)
select datetime, commodity_id, open_price, high_price, low_price, close_price, volume
from main
on conflict on constraint txn_pk do nothing;
