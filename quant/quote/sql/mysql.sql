create table if not exists tb_stock_eod_price
(
    id          varchar(36) primary key comment '主键',
    stock_code  varchar(16)      not null comment '股票代码',
    trade_dt    int              not null comment '交易日',
    open        decimal(10, 2)   not null comment '开盘价',
    close       decimal(10, 2)   not null comment '收盘价',
    high        decimal(10, 2)   not null comment '最高价',
    low         decimal(10, 2)   not null comment '最低价',
    volume      bigint           not null comment '成交量',
    amount      decimal(20, 2)   not null comment '成交额',
    amplitude   decimal(10, 2)   not null comment '振幅',
    chg         decimal(10, 2)   not null comment '涨跌额',
    chg_pct     decimal(10, 2)   not null comment '涨跌幅',
    turnover    decimal(10, 2)   not null comment '换手率',
    adjust_type int(2) default 0 not null comment '复权类型1-前复权，2-后复权，0-不复权',
    update_time datetime         not null comment '更新时间',
    unique index stock_dt (stock_code, trade_dt)
);
create table if not exists tb_trade_calendar
(
    id          int primary key comment '主键',
    trade_dt    int      not null comment '交易日',
    update_time datetime not null comment '更新时间',
    unique index trade_dt (trade_dt)
);