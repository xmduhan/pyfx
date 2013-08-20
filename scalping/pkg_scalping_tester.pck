create or replace package pkg_scalping_tester is

  -- 过程定义
  procedure load_trading_data(i_table_name varchar2, i_len number);
  procedure get_profit_slice(i_tp_level number, i_sl_level number, i_len number);
  procedure get_rsi_trading(i_long_rsi number, i_short_rsi number);
  procedure get_data;
  
  -- 建立相关的数据结构
  /* 
  -- Create sequence 
  create sequence SEQ_LOG
  minvalue 1
  maxvalue 999999999999999999999999999
  start with 1
  increment by 1
  cache 20;
  
  -- Create table
  create table TB_ST_LOG
  (
    LOG_ID   NUMBER,
    LOG_DATE DATE,
    LOG_MSG  VARCHAR2(4000)
  );
  -- Create table
  create table TB_ST_PROFIT_SLICE
  (
    TP_LEVEL NUMBER,
    SL_LEVEL NUMBER,
    LEN      NUMBER,
    TYPE     VARCHAR2(10),
    TIME     DATE,
    N        NUMBER,
    N1       NUMBER,
    TP       NUMBER,
    SL       NUMBER,
    CL       NUMBER,
    PROFIT   NUMBER
  );
  
  create table TB_ST_PROFIT_SLICE_BAK
  (
    TP_LEVEL NUMBER,
    SL_LEVEL NUMBER,
    LEN      NUMBER,
    TYPE     VARCHAR2(10),
    TIME     DATE,
    N        NUMBER,
    N1       NUMBER,
    TP       NUMBER,
    SL       NUMBER,
    CL       NUMBER,
    PROFIT   NUMBER
  );
  
  create table TB_ST_PROFIT_SLICE_LONG
  (
    N  NUMBER,
    N1 NUMBER,
    TP NUMBER,
    SL NUMBER,
    CL NUMBER
  );
  create index TB_LUOYU_PROFIT_SLICE_1_I1 on TB_ST_PROFIT_SLICE_LONG (N);
  
  create table TB_ST_PROFIT_SLICE_SHORT
  (
    N  NUMBER,
    N1 NUMBER,
    TP NUMBER,
    SL NUMBER,
    CL NUMBER
  );
  create index TB_ST_PROFIT_SLICE_SHORT_I1 on TB_ST_PROFIT_SLICE_SHORT (N);
  
  -- Create table
  create table TB_ST_RSI_TRADING
  (
    TIME DATE,
    N    NUMBER,
    TYPE varchar2(10),
    RSI  NUMBER
  );
  
  create table TB_ST_RSI_TRADING_BAK
  (
    TIME DATE,
    N    NUMBER,
    TYPE varchar2(10),
    RSI  NUMBER
  );
  
  -- Create table
  create table TB_ST_TRADING_DATA
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
  create index TB_ST_TRADING_DATA_I1 on TB_ST_TRADING_DATA (N);
  
  create table TB_ST_TRADING_DATA_MIX
  (
    TIME DATE,
    N    NUMBER,
    ASK  NUMBER,
    BID  NUMBER,
    N1   NUMBER,
    ASK1 NUMBER,
    BID1 NUMBER
  ); 

  create table tb_st_rsi_trading_detail
  (
    TP_LEVEL     NUMBER,
    SL_LEVEL     NUMBER,
    LEN          NUMBER,
    TYPE         VARCHAR2(10),
    TIME         DATE,
    N            NUMBER,
    N1           NUMBER,
    TP           NUMBER,
    SL           NUMBER,
    CL           NUMBER,
    PROFIT       NUMBER,
    HH24         VARCHAR2(2),
    RSI          NUMBER,
    G            NUMBER,
    GCNT         NUMBER,
    M            NUMBER,
    TOTAL_PROFIT NUMBER
  )
  create index TB_ST_RSI_TRADING_DETAIL_I1 on TB_ST_RSI_TRADING_DETAIL (G);
  
  create table tb_st_rsi_trading_detail_fit
  (
    G   NUMBER,
    CNT NUMBER,
    B1  NUMBER,
    B0  NUMBER
  );
  
  create table tb_st_rsi_trading_detail_1
  (
    TP_LEVEL             NUMBER,
    SL_LEVEL             NUMBER,
    LEN                  NUMBER,
    TYPE                 VARCHAR2(10),
    TIME                 DATE,
    N                    NUMBER,
    N1                   NUMBER,
    TP                   NUMBER,
    SL                   NUMBER,
    CL                   NUMBER,
    PROFIT               NUMBER,
    HH24                 VARCHAR2(2),
    RSI                  NUMBER,
    G                    NUMBER,
    GCNT                 NUMBER,
    M                    NUMBER,
    TOTAL_PROFIT         NUMBER,
    PREDICT_TOTAL_PROFIT NUMBER,
    DEVIATION            NUMBER
  );
  
  create table tb_st_rsi_trading_stat
  (
    G             NUMBER,
    TP_LEVEL      NUMBER,
    SL_LEVEL      NUMBER,
    LEN           NUMBER,
    TYPE          VARCHAR2(10),
    HH24          VARCHAR2(2),
    RSI           NUMBER,
    CNT           NUMBER,
    TP            NUMBER,
    SL            NUMBER,
    CL            NUMBER,
    PROFIT        NUMBER,
    TP_PROFIT     NUMBER,
    SL_PROFIT     NUMBER,
    CL_PROFIT     NUMBER,
    TP_PCT        NUMBER,
    SL_PCT        NUMBER,
    CL_PCT        NUMBER,
    DEVIATION     NUMBER,
    AVG_DEVIATION NUMBER,
    R1            NUMBER,
    R2            NUMBER,
    R3            NUMBER
  );
  
  
  */

end pkg_scalping_tester;
/
create or replace package body pkg_scalping_tester is

  /*
    存在问题：
    1. 不支持信号生成器使用结束头寸信号
    2. 不能限制头寸重入
    -- 以上两点困难只要修改get_rsi_trading，做适当完善可以解决
    -- 如果解决了以上两个问题，则形成一个比较完整的交易系统分析方法。  
  */ 

  procedure log(i_msg varchar2) is
  begin
    insert into tb_st_log
      select seq_log.nextval, sysdate, i_msg from dual;
    commit;
  end;

  procedure clear_log is
  begin
    execute immediate 'truncate table tb_st_log';
  end;

  --  导入交易数据
  procedure load_trading_data(i_table_name varchar2, i_len number) is
  begin
    log('load_trading_data(' || i_table_name || ',' || to_char(i_len) || '):开始');
    execute immediate 'truncate table tb_st_trading_data';
    execute immediate '
    insert into tb_st_trading_data
      select * from ' || i_table_name;
    commit;
    -- 将数据与之后的特定长度周期的数据进行关联
    execute immediate 'truncate table tb_st_trading_data_mix';
    insert into tb_st_trading_data_mix
      select a.time, a.n, a.ask, a.bid, b.n n1, b.ask ask1, b.bid bid1
        from tb_st_trading_data a, tb_st_trading_data b
       where a.n < b.n
         and b.n <= a.n + i_len;
    commit;
    log('load_trading_data:结束');
  end;

  -- 生成盈利切片
  procedure get_profit_slice(i_tp_level number, i_sl_level number, i_len number) is
  begin
    log('get_profit_slice(' || to_char(i_tp_level) || ',' || to_char(i_sl_level) || ',' ||
        to_char(i_len) || '):开始');
    -- 生成做多交易的盈利切片
    execute immediate 'truncate table tb_st_profit_slice_long';
    insert into tb_st_profit_slice_long
      select n, min(n1) n1, 1 tp, 0 sl, 0 cl
        from tb_st_trading_data_mix
       where (bid1 - ask) * 10000 > i_tp_level
       group by n
      union
      select n, min(n1) n1, 0 tp, 1 sl, 0 cl
        from tb_st_trading_data_mix
       where (bid1 - ask) * 10000 < -i_sl_level
       group by n
      union
      select n, max(n1) n1, 0 tp, 0 sl, 1 cl
        from tb_st_trading_data_mix
       where n1 - n <= i_len
       group by n;
    commit;
    delete /*+rule*/
    from tb_st_profit_slice_long a
     where a.n1 <> (select min(b.n1) from tb_st_profit_slice_long b where b.n = a.n);
    delete /*+rule*/
    from tb_st_profit_slice_long a
     where a.cl <> (select min(b.cl) from tb_st_profit_slice_long b where b.n = a.n);
    commit;
  
    -- 生成做空交易的盈利切片
    execute immediate 'truncate table tb_st_profit_slice_short';
    insert into tb_st_profit_slice_short
      select n, min(n1) n1, 1 tp, 0 sl, 0 cl
        from tb_st_trading_data_mix
       where (bid - ask1) * 10000 > i_tp_level
       group by n
      union
      select n, min(n1) n1, 0 tp, 1 sl, 0 cl
        from tb_st_trading_data_mix
       where (bid - ask1) * 10000 < -i_sl_level
       group by n
      union
      select n, max(n1) n1, 0 tp, 0 sl, 1 cl
        from tb_st_trading_data_mix
       where n1 - n <= i_len
       group by n;
    commit;
    delete /*+rule*/
    from tb_st_profit_slice_short a
     where a.n1 <> (select min(b.n1) from tb_st_profit_slice_short b where b.n = a.n);
    delete /*+rule*/
    from tb_st_profit_slice_short a
     where a.cl <> (select min(b.cl) from tb_st_profit_slice_short b where b.n = a.n);
    commit;
  
    -- 合并生成数据
    execute immediate 'truncate table tb_st_profit_slice';
    insert into tb_st_profit_slice
      select i_tp_level,
             i_sl_level,
             i_len,
             'long' type,
             b.time,
             a.n,
             a.n1,
             a.tp,
             a.sl,
             a.cl,
             (c.bid - b.ask) * 10000 profit
        from tb_st_profit_slice_long a, tb_st_trading_data b, tb_st_trading_data c
       where a.n = b.n(+)
         and a.n1 = c.n(+);
    commit;
    insert into tb_st_profit_slice
      select i_tp_level,
             i_sl_level,
             i_len,
             'short' type,
             b.time,
             a.n,
             a.n1,
             a.tp,
             a.sl,
             a.cl,
             (b.bid - c.ask) * 10000 profit
        from tb_st_profit_slice_short a, tb_st_trading_data b, tb_st_trading_data c
       where a.n = b.n(+)
         and a.n1 = c.n(+);
    commit;
  
    log('get_profit_slice:结束');
  end;

  -- RSI交易记录生成器
  procedure get_rsi_trading(i_long_rsi number, i_short_rsi number) is
  begin
    log('get_rsi_trading(' || to_char(i_long_rsi) || ',' || to_char(i_short_rsi) ||
        '):开始');
    execute immediate 'truncate table tb_st_rsi_trading';
    insert into tb_st_rsi_trading
      select a.time, a.n, 'long' type, i_long_rsi
        from tb_st_trading_data a, tb_st_trading_data b
       where a.n = b.n + 1
         and b.rsi <= i_long_rsi
         and a.rsi > i_long_rsi
      union all
      select a.time, a.n, 'short' type, i_short_rsi
        from tb_st_trading_data a, tb_st_trading_data b
       where a.n = b.n + 1
         and b.rsi >= i_short_rsi
         and a.rsi < i_short_rsi;
    commit;
    log('get_rsi_trading:结束');
  end;

  procedure get_rsi_trading_stat is
  begin
    log('get_rsi_trading_stat:开始');
  
    -- 关联交易记录和对应的获利切片,形成交易明细
    execute immediate 'truncate table tb_st_rsi_trading_detail';
    insert into tb_st_rsi_trading_detail
      select a.*,
             to_char(a.time, 'hh24') hh24,
             b.rsi,
             dense_rank() over(order by a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi) g,
             count(*) over(partition by a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi) gcnt,
             row_number() over(partition by a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi order by a.n) m,
             sum(profit) over(partition by a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi order by a.n) total_profit
        from tb_st_profit_slice_bak a, tb_st_rsi_trading_bak b
       where a.n = b.n
         and a.type = b.type;
    commit;
  
    -- 通过一元线性回归拟合出"交易次数--累计利润"的直线方程
    execute immediate 'truncate table tb_st_rsi_trading_detail_fit';
    insert into tb_st_rsi_trading_detail_fit
      select g,
             count(*) cnt,
             sum((x - x0) * (y - y0)) / sum((x - x0) * (x - x0)) b1,
             avg(y) - sum((x - x0) * (y - y0)) / sum((x - x0) * (x - x0)) * avg(x) b0
        from (select g,
                     m x,
                     total_profit y,
                     avg(m) over(partition by g) x0,
                     avg(total_profit) over(partition by g) y0
                from tb_st_rsi_trading_detail
               where gcnt > 1)
       group by g;
    commit;
  
    --  计算每笔交易的离差
    execute immediate 'truncate table tb_st_rsi_trading_detail_1';
    insert into tb_st_rsi_trading_detail_1
      select a.*,
             b0 + b.b1 * a.m predict_total_profit,
             abs(total_profit - (b0 + b.b1 * a.m)) deviation
        from tb_st_rsi_trading_detail a, tb_st_rsi_trading_detail_fit b
       where a.g = b.g(+);
    commit;
  
    -- 对交易记录进行统计
    execute immediate 'truncate table tb_st_rsi_trading_stat';
    insert into tb_st_rsi_trading_stat
      select g,
             tp_level,
             sl_level,
             len,
             type,
             hh24,
             rsi,
             count(*) cnt,
             sum(tp) tp,
             sum(sl) sl,
             sum(cl) cl,
             sum(profit) profit,
             sum(tp * profit) tp_profit,
             sum(sl * profit) sl_profit,
             sum(cl * profit) cl_profit,
             sum(tp) / count(*) tp_pct,
             sum(sl) / count(*) sl_pct,
             sum(cl) / count(*) cl_pct,
             sum(deviation) deviation,
             sum(deviation) / count(*) avg_deviation,
             row_number() over(order by sum(profit) desc) r1,
             row_number() over(order by sum(tp) / count(*) desc) r2,
             row_number() over(order by sum(deviation) / count(*)) r3
        from tb_st_rsi_trading_detail_1
       group by g, tp_level, sl_level, len, type, hh24, rsi;
    commit;
  
    log('get_rsi_trading_stat:结束');
  end;

  procedure get_data is
    i number;
    j number;
  begin
    clear_log;
    log('get_data:开始');
    
    -- 导入建议的原始数据
    load_trading_data('tb_hfmarketsltd_eurcad_m1_real', 120);
    
    -- 生成所有盈利切片数据    
    execute immediate 'truncate table tb_st_profit_slice_bak';
    i := 0.5;
    while i <= 10 loop
      j := 0.5;
      while j <= 30 loop
        get_profit_slice(i, j, 120);
        insert into tb_st_profit_slice_bak
          select * from tb_st_profit_slice;
        commit;
        j := j + .5;
      end loop;
      i := i + .5;
    end loop;
  
    -- 生成所有RSI交易点
    execute immediate 'truncate table tb_st_rsi_trading_bak';
    i := 5;
    while i <= 35 loop
      get_rsi_trading(i, 100 - i);
      insert into tb_st_rsi_trading_bak
        select * from tb_st_rsi_trading;
      commit;
      i := i + 1;
    end loop;
    
    -- 获取交易最终统计结果
    get_rsi_trading_stat;
    
    log('get_data:结束');
  end;

/*
-- 数据检查
select n,type,rsi,count(*) from tb_st_rsi_trading_bak group by n,type,rsi having count(*) > 1;
select tp_level, sl_level, len, type, n, count(*)
  from tb_st_profit_slice_bak
 group by tp_level, sl_level, len, type, n
having count(*) > 1;
*/

/*
-- 统计结果
select a.tp_level,
       a.sl_level,
       a.len,
       a.type,
       to_char(a.time, 'hh24') hh24,
       b.rsi,
       count(*) cnt,
       sum(tp) tp,
       sum(sl) sl,
       sum(cl) cl,
       sum(profit) profit,
       sum(tp * profit) tp_profit,
       sum(sl * profit) sl_profit,
       sum(cl * profit) cl_profit,
       sum(tp)/count(*) tp_pct,
       sum(sl)/count(*) sl_pct,
       sum(cl)/count(*) cl_pct
  from tb_st_profit_slice_bak a, tb_st_rsi_trading_bak b
 where a.n = b.n
   and a.type = b.type
 group by a.tp_level,
          a.sl_level,
          a.len,
          a.type,
          to_char(a.time, 'hh24'),
          b.rsi;
*/

/*
select a.*,
       to_char(a.time, 'hh24') hh24,
       b.rsi,
       dense_rank() over(ORDER BY a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi) g,
       row_number() over(partition by a.tp_level, a.sl_level, a.len, a.type, to_char(a.time, 'hh24'), b.rsi order by a.n) m
  from tb_st_profit_slice_bak a, tb_st_rsi_trading_bak b
 where a.n = b.n
   and a.type = b.type;
*/

/*
# 一元线性回归
x = cars$speed;
y = cars$dist;
# 使用公式手工计算
B1=sum((x-mean(x))*(y-mean(y)))/sum((x-mean(x))^2);
B0=mean(y)-B1*mean(x);
paste("模型 : Y = ",round(B0,3)," + ",round(B1,3),"X",sep = "");
# 使用lm函数计算
lm(y~x);
*/

end pkg_scalping_tester;
/
