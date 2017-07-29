--sys
create tablespace stock
datafile 'G:/app/datafile/stock.dbf' size 100M  
autoextend on;

--sys
create user stock identified by 123456
default tablespace stock

--sys
grant dba to stock;
grant connect to stock;
grant resource to stock;
grant create table to stock;

--stock
create table stock_basics
(code varchar2(10) primary key,
name varchar2(50),
industry varchar2(50),
area varchar2(50),
pe number(20, 4),
outstanding number(20, 4),
totals number(20, 4),
totalAssets number(20, 4),
liquidAssets number(20, 4),
fixedAssets number(20, 4),
reserved number(20, 4),
reservedPerShare number(20, 4),
esp number(20, 4),
bvps number(20, 4),
pb number(20, 4),
timeToMarket date,
undp number(20, 4),
perundp number(20, 4), 
rev number(20, 4),
profit number(20, 4),
gpr number(20, 4),
npr number(20, 4),
holders number(20, 4)
);

comment on table stock_basics is '上市公司基本信息表';
comment on column stock_basics.code is '股票代码，300 创业板，600 601，602 沪市A股，900 沪市B股，000深市A股，002 中小板，200深市B股';
comment on column stock_basics.name is '股票名称';
comment on column stock_basics.industry is '所属行业';
comment on column stock_basics.area is '地区';
comment on column stock_basics.pe is '市盈率';
comment on column stock_basics.outstanding is '流通股本(亿)';
comment on column stock_basics.totals is '总股本(亿)';
comment on column stock_basics.totalAssets is '总资产(万)';
comment on column stock_basics.liquidAssets is '流动资产';
comment on column stock_basics.fixedAssets is '固定资产';
comment on column stock_basics.reserved is '公积金';
comment on column stock_basics.reservedPerShare is '每股公积金';
comment on column stock_basics.esp is '每股收益';
comment on column stock_basics.bvps is '每股净资';
comment on column stock_basics.pb is '市净率';
comment on column stock_basics.timeToMarket is '上市日期';
comment on column stock_basics.undp is '未分利润';
comment on column stock_basics.perundp is '每股未分配';
comment on column stock_basics.rev is '收入同比(%)';
comment on column stock_basics.profit is '利润同比(%)';
comment on column stock_basics.gpr is '毛利率(%)';
comment on column stock_basics.npr is '净利润率(%)';
comment on column stock_basics.holders is '股东人数';


--获取业绩报告
create table stock_report
(uuid varchar2(100) primary key,
code varchar2(10), 
name varchar2(20),
esp number(20, 4),
eps_yoy number(20, 4),
bvps number(20, 4),
roe number(20, 4),
epcf number(20, 4),
net_profits number(20, 4),
profits_yoy number(20, 4),
distrib varchar2(50),
report_date date,
year varchar2(5),
quarter varchar2(2)
);
comment on table stock_report is '业绩报告表';
comment on column stock_report.uuid is '主键';
comment on column stock_report.code is '代码';
comment on column stock_report.name is '名称';
comment on column stock_report.esp is '每股收益';
comment on column stock_report.eps_yoy is '每股收益同比(%)';
comment on column stock_report.bvps is '每股净资产';
comment on column stock_report.roe is '净资产收益率(%)';
comment on column stock_report.epcf is '每股现金流量(元)';
comment on column stock_report.net_profits is '净利润(万元)';
comment on column stock_report.profits_yoy is '净利润同比(%)';
comment on column stock_report.distrib is '分配方案';
comment on column stock_report.report_date is '发布日期';
comment on column stock_report.year is '年份';
comment on column stock_report.quarter is '季度';



