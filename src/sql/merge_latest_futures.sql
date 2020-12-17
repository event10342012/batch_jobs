with temp as (
    select datetime,
           commodity_id,
           min(expired) as expired
    from trading.futures.txn
    where datetime >= now() - interval '30 days'
    group by datetime, commodity_id
),
     main as (
         select temp.datetime,
                temp.commodity_id,
                txn.open_price,
                txn.high_price,
                txn.low_price,
                txn.close_price,
                txn.volume
         from temp
                  left join trading.futures.txn
                            on txn.datetime = temp.datetime and txn.commodity_id = temp.commodity_id and
                               txn.expired = temp.expired
     )

insert
into trading.futures.latest_txn (datetime, commodity_id, open_price, high_price, low_price, close_price, volume)
select datetime, commodity_id, open_price, high_price, low_price, close_price, volume
from main
on conflict on constraint txn_pk do nothing;
