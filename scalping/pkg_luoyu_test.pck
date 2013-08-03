create or replace package pkg_luoyu_test is
  procedure test01;
  procedure test02;
end pkg_luoyu_test;
/
create or replace package body pkg_luoyu_test is
/*
-- Create sequence 

create sequence SEQ_LOG
minvalue 1
maxvalue 999999999999999999999999999
start with 3781
increment by 1
cache 20;

create sequence SEQ_STAT
minvalue 1
maxvalue 999999999999999999999999999
start with 541
increment by 1
cache 20;

-- Create table

create table TB_LUOYU_LOG
(
  LOG_ID   NUMBER,
  LOG_DATE DATE,
  LOG_MSG  VARCHAR2(4000)
);


create table TB_LUOYU_M1
(
  TIME   DATE,
  OPEN   NUMBER,
  CLOSE  NUMBER,
  HIGH   NUMBER,
  LOW    NUMBER,
  VOLUME NUMBER,
  ASK    NUMBER,
  BID    NUMBER,
  RSI    NUMBER,
  N      NUMBER
);
create index TB_LUOYU_M1_I1 on TB_LUOYU_M1 (N);

create table TB_LUOYU_M1_MIX
(
  TIME   DATE,
  OPEN   NUMBER,
  CLOSE  NUMBER,
  HIGH   NUMBER,
  LOW    NUMBER,
  VOLUME NUMBER,
  ASK    NUMBER,
  BID    NUMBER,
  RSI    NUMBER,
  N      NUMBER,
  N1     NUMBER,
  ASK1   NUMBER,
  BID1   NUMBER
);

create table TB_LUOYU_RAW_TRADING
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);
create index TB_LUOYU_TRADING_I1 on TB_LUOYU_RAW_TRADING (N)

create table TB_LUOYU_RAW_TRADING_1
(
  N  NUMBER,
  N1 NUMBER,
  TP NUMBER,
  SL NUMBER,
  CL NUMBER
);
create index LUOYU_0801_1_I1 on TB_LUOYU_RAW_TRADING_1 (N)

create table TB_LUOYU_RAW_TRADING_LONG
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);

create table TB_LUOYU_RAW_TRADING_SHORT
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);

create table TB_LUOYU_RAW_TRADING_STAT
(
  STAT_ID   NUMBER,
  TP_LEVEL  NUMBER,
  SL_LEVEL  NUMBER,
  HH24      VARCHAR2(2),
  TYPE      VARCHAR2(10),
  RSI       NUMBER,
  CNT       NUMBER,
  TP        NUMBER,
  SL        NUMBER,
  CL        NUMBER,
  PROFIT    NUMBER,
  TP_PROFIT NUMBER,
  SL_PROFIT NUMBER,
  CL_PROFIT NUMBER
);

create table TB_LUOYU_SEL_TRADING
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);

create table TB_LUOYU_SEL_TRADING_LONG
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);

create table TB_LUOYU_SEL_TRADING_SHORT
(
  N        NUMBER,
  N1       NUMBER,
  TP       NUMBER,
  SL       NUMBER,
  CL       NUMBER,
  TIME     DATE,
  RSI      NUMBER,
  PROFIT   NUMBER,
  TYPE     VARCHAR2(10),
  TP_LEVEL NUMBER,
  SL_LEVEL NUMBER
);

create table TB_LUOYU_SEL_TRADING_STAT
(
  STAT_ID   NUMBER,
  TP_LEVEL  NUMBER,
  SL_LEVEL  NUMBER,
  HH24      VARCHAR2(2),
  TYPE      VARCHAR2(10),
  RSI       NUMBER,
  CNT       NUMBER,
  TP        NUMBER,
  SL        NUMBER,
  CL        NUMBER,
  PROFIT    NUMBER,
  TP_PROFIT NUMBER,
  SL_PROFIT NUMBER,
  CL_PROFIT NUMBER
);

*/






  procedure log(i_msg varchar2) is
  begin
    insert into tb_luoyu_log
      select seq_log.nextval, sysdate, i_msg from dual;
    commit;
  end;

  procedure get_raw_data_long(i_tp_level number, i_sl_level number, i_period number) is
  begin
    log('get_raw_data_long:开始');
    execute immediate 'truncate table tb_luoyu_raw_trading_1';
    insert into tb_luoyu_raw_trading_1
      select n, min(n1) n1, 1 tp, 0 sl, 0 cl
        from tb_luoyu_m1_mix
       where (bid1 - ask) * 10000 > i_tp_level
       group by n
      union
      select n, min(n1) n1, 0 tp, 1 sl, 0 cl
        from tb_luoyu_m1_mix
       where (bid1 - ask) * 10000 < -i_sl_level
       group by n
      union
      select n, max(n1) n1, 0 tp, 0 sl, 1 cl
        from tb_luoyu_m1_mix
       where n1 - n <= i_period
       group by n;
    commit;
    delete from tb_luoyu_raw_trading_1 a
     where a.n1 <> (select min(b.n1) from tb_luoyu_raw_trading_1 b where b.n = a.n);
    delete from tb_luoyu_raw_trading_1 a
     where a.cl <> (select min(b.cl) from tb_luoyu_raw_trading_1 b where b.n = a.n);
    commit;
  
    execute immediate 'truncate table tb_luoyu_raw_trading_long';
    insert into tb_luoyu_raw_trading_long
      select a.*,
             b.time,
             b.rsi,
             (c.bid - b.ask) * 10000 profit,
             'long' type,
             i_tp_level,
             i_sl_level
        from tb_luoyu_raw_trading_1 a, tb_luoyu_m1 b, tb_luoyu_m1 c
       where a.n = b.n(+)
         and a.n1 = c.n(+);
    commit;
    log('get_raw_data_long:结束');
  end;

  procedure get_raw_data_short(i_tp_level number, i_sl_level number, i_period number) is
  begin
    log('get_raw_data_short:开始');
    -- 
    execute immediate 'truncate table tb_luoyu_raw_trading_1';
    insert into tb_luoyu_raw_trading_1
      select n, min(n1) n1, 1 tp, 0 sl, 0 cl
        from tb_luoyu_m1_mix
       where (bid - ask1) * 10000 > i_tp_level
       group by n
      union
      select n, min(n1) n1, 0 tp, 1 sl, 0 cl
        from tb_luoyu_m1_mix
       where (bid - ask1) * 10000 < -i_sl_level
       group by n
      union
      select n, max(n1) n1, 0 tp, 0 sl, 1 cl
        from tb_luoyu_m1_mix
       where n1 - n <= i_period
       group by n;
    commit;
    delete from tb_luoyu_raw_trading_1 a
     where a.n1 <> (select min(b.n1) from tb_luoyu_raw_trading_1 b where b.n = a.n);
    delete from tb_luoyu_raw_trading_1 a
     where a.cl <> (select min(b.cl) from tb_luoyu_raw_trading_1 b where b.n = a.n);
    commit;
  
    execute immediate 'truncate table tb_luoyu_raw_trading_short';
    insert into tb_luoyu_raw_trading_short
      select a.*,
             b.time,
             b.rsi,
             (b.bid - c.ask) * 10000 profit,
             'short' type,
             i_tp_level,
             i_sl_level
        from tb_luoyu_raw_trading_1 a, tb_luoyu_m1 b, tb_luoyu_m1 c
       where a.n = b.n(+)
         and a.n1 = c.n(+);
    commit;
    log('get_raw_data_short:结束');
  end;

  procedure get_raw_data is
  begin
    log('get_raw_data:开始');
    -- 合并交易数据
    execute immediate 'truncate table tb_luoyu_raw_trading';
    insert into tb_luoyu_raw_trading
      select *
        from tb_luoyu_raw_trading_long
      union
      select * from tb_luoyu_raw_trading_short;
    commit;
    log('get_raw_data:结束');
  end;

  procedure get_raw_stat is
    v_stat_id number;
  begin
    log('get_raw_stat:开始');
    execute immediate 'truncate table tb_luoyu_raw_trading_stat';
    select seq_stat.nextval into v_stat_id from dual;
    insert into tb_luoyu_raw_trading_stat
      select v_stat_id,
             tp_level,
             sl_level,
             to_char(time, 'hh24') hh24,
             type,
             round(rsi) rsi,
             count(*) cnt,
             sum(tp) tp,
             sum(sl) sl,
             sum(cl) cl,
             sum(profit) profit,
             sum(tp * profit) tp_profit,
             sum(sl * profit) sl_profit,
             sum(cl * profit) cl_profit
        from tb_luoyu_raw_trading
       group by tp_level, sl_level, to_char(time, 'hh24'), round(rsi), type;
    commit;
    log('get_raw_stat:结束');
  end;

  procedure get_sel_long_data --
  (i_rsi_low  number, --
   i_rsi_high number) is
    v_n1 number;
  begin
    log('get_sel_long_data:开始');
    v_n1 := -1;
    execute immediate 'truncate table tb_luoyu_sel_trading_long';
    for r1 in (select *
                 from tb_luoyu_raw_trading_long
                where rsi between i_rsi_low and i_rsi_high
                order by n) loop
      if v_n1 < r1.n then
        insert into tb_luoyu_sel_trading_long values r1;
        commit;
        v_n1 := r1.n1;
      end if;
    end loop;
    log('get_sel_long_data:结束');
  end;

  procedure get_sel_short_data --
  (i_rsi_low  number, --
   i_rsi_high number) is
    v_n1 number;
  begin
    log('get_sel_short_data:开始');
    v_n1 := -1;
    execute immediate 'truncate table tb_luoyu_sel_trading_short';
    for r1 in (select *
                 from tb_luoyu_raw_trading_short
                where rsi between i_rsi_low and i_rsi_high
                order by n) loop
      if v_n1 < r1.n then
        insert into tb_luoyu_sel_trading_short values r1;
        commit;
        v_n1 := r1.n1;
      end if;
    end loop;
    log('get_sel_short_data:结束');
  end;

  procedure get_sel_data is
  begin
    execute immediate 'truncate table tb_luoyu_sel_trading';
    insert into tb_luoyu_sel_trading
      select *
        from tb_luoyu_sel_trading_long
      union
      select * from tb_luoyu_sel_trading_short;
    commit;
  end;

  procedure get_sel_stat is
    v_stat_id number;
  begin
    log('get_sel_stat:开始');
    execute immediate 'truncate table tb_luoyu_sel_trading_stat';
    select seq_stat.nextval into v_stat_id from dual;
    insert into tb_luoyu_sel_trading_stat
      select v_stat_id,
             tp_level,
             sl_level,
             to_char(time, 'hh24') hh24,
             type,
             round(rsi) rsi,
             count(*) cnt,
             sum(tp) tp,
             sum(sl) sl,
             sum(cl) cl,
             sum(profit) profit,
             sum(tp * profit) tp_profit,
             sum(sl * profit) sl_profit,
             sum(cl * profit) cl_profit
        from tb_luoyu_sel_trading
       group by tp_level, sl_level, to_char(time, 'hh24'), round(rsi), type;
    commit;
    log('get_sel_stat:结束');
  end;

  procedure get_data --
  (i_l_tp_level number,
   i_l_sl_level number,
   i_l_period   number,
   i_l_rsi_low  number,
   i_l_rsi_high number,
   i_s_tp_level number,
   i_s_sl_level number,
   i_s_period   number,
   i_s_rsi_low  number,
   i_s_rsi_high number) is
  begin
    log('get_data():开始');
    get_raw_data_long(i_l_tp_level, i_l_sl_level, i_l_period);
    get_raw_data_short(i_s_tp_level, i_s_sl_level, i_s_period);
    get_raw_data;
    get_raw_stat;
    get_sel_long_data(i_l_rsi_low, i_l_rsi_high);
    get_sel_short_data(i_s_rsi_low, i_s_rsi_high);
    get_sel_data;
    get_sel_stat;
    log('get_data():结束');
  end;

  -- 多空采用相同参数
  procedure get_data --
  (i_tp_level number, i_sl_level number, i_period number) is
  begin
    get_data(i_tp_level, i_sl_level, i_period, 0, 30, --
             i_tp_level, i_sl_level, i_period, 70, 100);
  end;

  procedure test01 is
    i number;
    j number;
  begin
    execute immediate 'truncate table tb_luoyu_log';
    --execute immediate 'truncate table tb_luoyu_raw_trading_stat';    
    --/*
    i := 0.5;
    while i <= 8 loop
      j := 17;
      while j <= 20 loop
        get_data(i, j, 120);
        j := j + 1;
      end loop;
      i := i + 0.5;
    end loop;
    --*/
    --get_data(2., 12, 120);
  end;

  procedure test02 is
  begin
    get_data --
    (2.5, 18, 120, 0, 30, --
     6, 16, 120, 70, 100);
  end;

/*
  
  select tp_level,
       sl_level,
       hh24,
       type,
       sum(cnt) count,
       sum(profit) profit,
       sum(tp_profit) tp_profit,
       sum(sl_profit) sl_profit,
       sum(cl_profit) cl_profit,
       round(sum(profit)/sum(cnt),2) avg_profit,
       round(sum(tp_profit) / decode(sum(tp),0,null,sum(tp)), 2) avg_tp_profit,
       round(sum(sl_profit) / decode(sum(sl),0,null,sum(sl)), 2) avg_sl_profit,
       round(sum(cl_profit) / decode(sum(cl),0,null,sum(cl)), 2) avg_cl_profit,
       round(sum(tp) / sum(cnt) * 100, 1) tp_pct,
       round(sum(sl) / sum(cnt) * 100, 1) sl_pct,
       round(sum(cl) / sum(cnt) * 100, 1) cl_pct
  from tb_luoyu_sel_trading_stat
 where 1 = 1
   --and hh24 in ('23', '00', '01', '02')
   and type = 'short'
   and tp_level = 6
   and sl_level = 16
 group by tp_level, sl_level,type,hh24 --,rsi
 order by profit desc;
 
 
 select tp_level,
       sl_level,
       type,
       sum(cnt) count,
       sum(profit) profit,
       sum(tp_profit) tp_profit,
       sum(sl_profit) sl_profit,
       sum(cl_profit) cl_profit,
       round(sum(profit)/sum(cnt),2) avg_profit,
       round(sum(tp_profit) / decode(sum(tp),0,null,sum(tp)), 2) avg_tp_profit,
       round(sum(sl_profit) / decode(sum(sl),0,null,sum(sl)), 2) avg_sl_profit,
       round(sum(cl_profit) / decode(sum(cl),0,null,sum(cl)), 2) avg_cl_profit,
       round(sum(tp) / sum(cnt) * 100, 1) tp_pct,
       round(sum(sl) / sum(cnt) * 100, 1) sl_pct,
       round(sum(cl) / sum(cnt) * 100, 1) cl_pct
  from tb_luoyu_sel_trading_stat
 where 1 = 1
   and ( type = 'long' and hh24 in ('08', '09', '22', '23','00','02') or 
         type = 'short' and hh24 in ( '00', '22') )
 group by tp_level, sl_level,type
 order by profit desc;
 
 select *
  from tb_luoyu_sel_trading
 where 1 = 1
   and (type = 'long' and
       to_char(time, 'hh24') in ('08', '09', '22', '23', '00', '02') or
       type = 'short' and to_char(time, 'hh24') in ('00', '22'))
       order by n;
 
 
select a.*,sum(a.profit) over(order by n) total_profit
  from tb_luoyu_sel_trading a
 where 1 = 1
   and (type = 'long' and
       to_char(time, 'hh24') in ( '22', '23', '00', '02') or
       type = 'short' and to_char(time, 'hh24') in ('00', '22'))
       order by n

 
 */

end pkg_luoyu_test;
/
