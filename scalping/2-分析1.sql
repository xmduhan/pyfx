cl;
set echo on;

drop table duh0722_1;
create table duh0722_1 as 
--select * from tb_hfmarketsltd_eurcad_m1 where to_char(time,'yyyymmdd') >= '20130619'
--select * from TB_HF_EURCAD_M1_95929
select * from TB_HF_EURCAD_M1_524850
;
create index duh0722_1_i1 on duh0722_1(n);


drop table fxtmp001;
create table fxtmp001 as 
select *
  from duh0722_1
 where 1=1
   and to_char(time, 'hh24') in ('23', '00', '01', '02')
   and rsi < 30;
/*   
select to_char(time, 'yyyymmdd-hh24'), count(*)
  from tb_hfmarketsltd_eurcad_m1
 group by to_char(time, 'yyyymmdd-hh24');  
*/ 

drop table fxtmp002;
create table fxtmp002 as 
select a.*,
       (select min(n)
          from duh0722_1 b
         where b.n > a.n
           and b.n < a.n + 240
           and b.bid - a.ask >= 0.0005) n1,
       (select min(n)
          from duh0722_1 b
         where b.n > a.n
           and b.n < a.n + 240
           and b.bid - a.ask <= -0.0015) n2
  from fxtmp001 a;
  
select count(*) from fxtmp002;  

alter table fxtmp002 add(nx number);
update fxtmp002 set nx = n1 where n1 is not null and n2 is null;
update fxtmp002 set nx = n2 where n1 is null and n2 is not null;
update fxtmp002 set nx = n + 240 where n1 is null and n2 is null;
update fxtmp002 set nx = n1  where n1 is not null and n2 is not null and n1<n2;
update fxtmp002 set nx = n2  where n1 is not null and n2 is not null and n1>n2;
commit;

drop table fxtmp003;
create table fxtmp003 as 
select a.*, b.bid - a.ask profit,to_char(a.time,'yyyymmdd-hh24') t1,to_char(a.time,'hh24') hh
  from fxtmp002 a, duh0722_1 b
 where a.nx = b.n;
delete from fxtmp003 a where n <> (select min(n) from fxtmp003 b where b.t1 = a.t1);
commit;
select count(*) from fxtmp003;
 

select hh, count(*), round(avg(profit*10000),2) profit from fxtmp003 group by hh order by 1;

set echo off;
exit;
--select * from fxtmp003 order by n;
--select * from fxtmp003;

 

