create table if not exists tb_stock_eod_price(
    id varchar(32) primary key,
    stock_code varchar(10) not null,
    trade_dt date not null,
    open decimal(10, 2) not null,
    close decimal(10, 2) not null,
    high decimal(10, 2) not null,
    low decimal(10, 2) not null,
    volume bigint not null,
    amount decimal(20, 2) not null,
    update_time datetime not null,
    unique index 'stock_day' (stock_code, trade_dt)
);


create table if not exists tb_trade_calendar(
    id int primary key auto_increment,
    trade_dt int,
    unique index 'trade_dt' (trade_dt)
);