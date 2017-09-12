create database stock_wangyi default character set utf8 collate utf8_general_ci;

grant all on stock_wangyi.* to 'stock'@'%' identified by '123456';

use stock_wangyi;

create table stock_basics
(code varchar(10) primary key comment '股票代码，300 创业板，600 601，602 沪市A股，900 沪市B股，000深市A股，002 中小板，200深市B股' ,
name varchar(50) comment '股票名称',
industry varchar(50) comment '所属行业',
area varchar(50) comment '地区',
pe decimal(20, 4) comment '市盈率',
outstanding decimal(20, 4) comment '流通股本(亿)',
totals decimal(20, 4) comment  '总股本(亿)' ,
totalAssets decimal(20, 4) comment '总资产(万)' ,
liquidAssets decimal(20, 4) comment '流动资产' ,
fixedAssets decimal(20, 4) comment '固定资产' ,
reserved decimal(20, 4) comment '公积金' ,
reservedPerShare decimal(20, 4) comment '每股公积金',
esp decimal(20, 4) comment '每股收益' ,
bvps decimal(20, 4) comment '每股净资',
pb decimal(20, 4) comment  '市净率' ,
timeToMarket date comment '上市日期' ,
undp decimal(20, 4) comment  '未分利润',
perundp decimal(20, 4) comment '每股未分配' , 
rev decimal(20, 4) comment '收入同比(%)' ,
profit decimal(20, 4) comment '利润同比(%)' ,
gpr decimal(20, 4) comment '毛利率(%)',
npr decimal(20, 4) comment  '净利润率(%)',
holders decimal(20, 4) comment '股东人数' 
) ;



create table stock_report
(uuid varchar(100) primary key comment '主键' ,
code varchar(10) comment  '代码' , 
name varchar(20) comment '名称' ,
esp decimal(20, 4) comment '每股收益' ,
eps_yoy decimal(20, 4) comment '每股收益同比(%)' ,
bvps decimal(20, 4) comment '每股净资产' ,
roe decimal(20, 4) comment '净资产收益率(%)' ,
epcf decimal(20, 4) comment '每股现金流量(元)' ,
net_profits decimal(20, 4) comment '净利润(万元)' ,
profits_yoy decimal(20, 4) comment '净利润同比(%)' ,
distrib varchar(50) comment '分配方案' ,
report_date date comment  '发布日期' ,
year varchar(5) comment '年份' ,
quarter varchar(2)  '季度'
) comment '业绩报告表';




create table stock_profit
(uuid varchar(100) primary key,
code varchar(10) comment 'name' , 
name varchar(20) comment 'code' ,
roe decimal(20, 4) comment  '净资产收益率(%)',
net_profit_ratio decimal(20, 4) comment '净利率(%)' ,
gross_profit_rate decimal(20, 4) comment '毛利率(%)' ,
net_profits decimal(20, 4) comment '净利润(万元)' ,
eps decimal(20, 4) comment '每股收益' , 
business_income decimal(20, 4) comment  '营业收入(百万元)' ,
bips decimal(20, 4) comment  '每股主营业务收入(元)' ,
year varchar(5) comment '年份' ,
quarter varchar(2) comment   '季度' 
) comment '盈利能力表';



create table stock_operation
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
arturnover decimal(20, 4),
arturndays decimal(20, 4),
inventory_turnover decimal(20, 4),
inventory_days decimal(20, 4),
currentasset_turnover decimal(20, 4),
currentasset_days decimal(20, 4),
year varchar(5),
quarter varchar(2)
);




create table stock_growth
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
mbrg decimal(20, 4),
nprg decimal(20, 4),
nav decimal(20, 4),
targ decimal(20, 4),
epsg decimal(20, 4),
seg decimal(20, 4),
year varchar(5),
quarter varchar(2)
);




create table stock_debtpaying
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
currentratio decimal(20, 4),
quickratio decimal(20, 4),
cashratio decimal(20, 4),
icratio decimal(20, 4),
sheqratio decimal(20, 4),
adratio decimal(20, 4),
year varchar(5),
quarter varchar(2)
);


create table stock_cashflow
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
cf_sales decimal(20, 4),
rateofreturn decimal(20, 4),
cf_nm decimal(20, 4),
cf_liabilities decimal(20, 4),
cashflowratio decimal(20, 4),
year varchar(5),
quarter varchar(2)
);


create table stock_profit_data
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
year varchar(5),
report_date date,
divi  decimal(20, 4),
shares decimal(20, 4)
);


create table stock_forecast
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
type varchar(10),
report_date date,
pre_eps decimal(20, 4),
`range` varchar(10),
year varchar(5),
quarter varchar(2)
);



create table stock_xsg
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(20),
lift_date date,
count decimal(20, 4),
ratio decimal(20, 4),
year varchar(5),
month varchar(5)
);


create table stock_fund_holdings
(uuid varchar(100) primary key,
code varchar(10), 
name varchar(50),
report_date date,
nums decimal(20, 4),
nlast decimal(20, 4),
count decimal(20, 4),
clast decimal(20, 4),
amount decimal(20, 4),
ratio decimal(20, 4),
year varchar(5),
quarter varchar(5)
);


create table stock_sh_margins
(uuid varchar(100) primary key,
op_date date,
rzye decimal(20, 4),
rzmre decimal(20, 4),
rqyl decimal(20, 4),
rqylje decimal(20, 4),
rqmcl decimal(20, 4),
rzrqjyzl decimal(20, 4)
);


create table stock_sh_margin_details
(uuid varchar(100) primary key,
op_date date,
code varchar(10), 
name varchar(50), 
rzye decimal(20, 4),
rzmre decimal(20, 4),
rzche decimal(20, 4),
rqyl decimal(20, 4),
rqmcl decimal(20, 4),
rqchl decimal(20, 4)
);


create table stock_sz_margins
(uuid varchar(100) primary key,
op_date date,
rzmre decimal(20, 4),
rzye decimal(20, 4),
rqmcl decimal(20, 4),
rqyl decimal(20, 4),
rqye decimal(20, 4),
rzrqye decimal(20, 4)
);


create table stock_sz_margin_details
(uuid varchar(100) primary key,
op_date date,
code varchar(10), 
name varchar(50), 
rzmre decimal(20, 4),
rzye decimal(20, 4),
rqmcl decimal(20, 4),
rqyl decimal(20, 4),
rqye decimal(20, 4),
rzrqye decimal(20, 4)
);

