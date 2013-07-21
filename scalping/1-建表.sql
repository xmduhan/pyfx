-- Create table
drop table TB_HFMarketsLtd_EURCAD_M1;
create table TB_HFMarketsLtd_EURCAD_M1 
(
  Time   DATE,
  N      NUMBER,
  Ask    NUMBER,
  Bid    NUMBER,
  Volume NUMBER,
  NewBar NUMBER,
  Rsi    NUMBER,
  F8     VARCHAR2(255)
);
create index tb_hfmarketsltd_eurcad_m1_i1 on tb_hfmarketsltd_eurcad_m1(n);


drop table TB_HFMarketsLtd_EURCAD_M1_1;
create table TB_HFMarketsLtd_EURCAD_M1_1 
(
  Time   DATE,
  N      NUMBER,
  Ask    NUMBER,
  Bid    NUMBER,
  Volume NUMBER,
  NewBar NUMBER,
  Rsi    NUMBER,
  F8     VARCHAR2(255)
);
create index tb_hfmarketsltd_eurcad_m1_1_i1 on tb_hfmarketsltd_eurcad_m1_1(n);
