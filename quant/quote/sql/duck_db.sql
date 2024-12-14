create table if not exists tb_stock_eod_price
(
    id          varchar(32) primary key,
    stock_code  varchar(10)    not null,
    trade_dt    date           not null,
    open        decimal(10, 2) not null,
    close       decimal(10, 2) not null,
    high        decimal(10, 2) not null,
    low         decimal(10, 2) not null,
    volume      bigint         not null,
    amount      decimal(20, 2) not null,
    amplitude   decimal(10, 2) not null,
    chg         decimal(10, 2) not null,
    chg_pct     decimal(10, 2) not null,
    turnover    decimal(10, 2) not null,
    update_time datetime       not null
);
drop index if exists stock_day;
create unique index stock_day on tb_stock_eod_price (stock_code, trade_dt);
drop index if exists trade_dt;
create table if not exists tb_trade_calendar
(
    id          int primary key,
    trade_dt    int,
    update_time datetime not null
);
drop index if exists trade_dt;
create unique index trade_dt on tb_trade_calendar (trade_dt)