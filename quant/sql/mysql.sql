create table if not exists tb_stock_list
(
    id          varchar(36) primary key comment '主键',
    stock_code  varchar(16) not null comment '股票代码',
    stock_name  varchar(16) not null comment '股票简称',
    market      varchar(16) comment '所属市场',
    list_date   date comment '上市时间',
    update_time datetime    not null comment '更新时间',
    index stock (stock_code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='个股列表';

create table if not exists tb_stock_daily_price
(
    id          varchar(36) primary key comment '主键',
    stock_code  varchar(16)      not null comment '股票代码',
    trade_dt    int              not null comment '交易日',
    open        decimal(10, 2)   not null comment '开盘价',
    close       decimal(10, 2)   not null comment '收盘价',
    high        decimal(10, 2)   not null comment '最高价',
    low         decimal(10, 2)   not null comment '最低价',
    pre_close   decimal(10, 2) comment '昨日收盘价',
    volume      bigint           not null comment '成交量',
    amount      decimal(20, 2)   not null comment '成交额',
    amplitude   decimal(10, 2) comment '振幅',
    chg         decimal(10, 2)   not null comment '涨跌额',
    chg_pct     decimal(10, 2)   not null comment '涨跌幅',
    turnover    decimal(10, 2) comment '换手率',
    adjust_type int(2) default 0 not null comment '复权类型 0-不复权,1-前复权，2-后复权',
    update_time datetime         not null comment '更新时间',
    unique index stock_dt (adjust_type, stock_code, trade_dt)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='个股日度收盘价';
create table if not exists tb_trade_calendar
(
    id          int primary key comment '主键',
    trade_dt    int      not null comment '交易日',
    update_time datetime not null comment '更新时间',
    unique index trade_dt (trade_dt)
);


create table if not exists tb_stock_concept_ths
(
    id           varchar(36) primary key comment '主键',
    name         varchar(32)  not null comment '同花顺的概念指数代码',
    concept_code varchar(32)  not null comment '同花顺的概念代码',
    stock_code   varchar(16)  not null comment '股票代码',
    stock_name   varchar(16)  not null comment '股票简称',
    source       varchar(32)  not null comment '来源',
    reason       varchar(256) not null comment '概念原因'
);