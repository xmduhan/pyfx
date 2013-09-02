
create table duh0829_1a as 
select time, open, close, high, low, volume, ask, bid, rsi, rsi1, 
row_number() over(order by time) n 
from (
select time + 1 / 24 / 60 time, 
       open,close,high,low,volume,ask,bid,rsi,rsi1,n
  from (select a.*,
               (max(time)
                over(order by n rows between 1 following and 1 following) - time) * 24 * 60 m
          from duh0829_1 a)
 where m <> 1 union 
select * from duh0829_1) ; 

