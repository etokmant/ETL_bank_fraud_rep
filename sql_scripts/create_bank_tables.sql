create schema if not exists bank;

set search_path to bank;

-- Создаем stg-таблицу транзакций
create table if not exists bank.stg_transactions (
    transaction_id varchar(128),
    transaction_date timestamp,
    amount decimal,
    card_num varchar(128),
    oper_type varchar(128),
    oper_result varchar(128),
    terminal varchar(128)
);

-- Создаем stg-таблицу терминалов
create table if not exists bank.stg_terminals (
    terminal_id varchar(128),
    terminal_type varchar(128),
    terminal_city varchar(128),
    terminal_address varchar(256)
);

-- Создаем stg-таблицу черного списка паспортов
create table if not exists bank.stg_passport_blacklist (
    date date,
    passport varchar(128)
);

-- Создаем хранилище транзакций
create table if not exists bank.dwh_fact_transactions (
    trans_id varchar(128) primary key,
    trans_date timestamp,
    card_num varchar(128),
    oper_type varchar(128),
    amt decimal,
    oper_result varchar(128),
    terminal varchar(128)
);

-- Создаем хранилище терминалов
create table if not exists bank.dwh_dim_terminals_hist (
    terminal_id varchar(128) not null,
    terminal_type varchar(128),
    terminal_city varchar(128),
    terminal_address varchar(256),
    effective_from timestamp not null,
    effective_to timestamp not null,
    deleted_flg smallint default 0,
    primary key (terminal_id, effective_from)  -- Составной первичный ключ
);

-- Создаем хранилище черного списка паспортов
create table if not exists bank.dwh_fact_passport_blacklist (
    passport_num varchar(128) primary key,
    entry_dt date
);

-- Создаем витрину мошеннических операций
create table if not exists bank.rep_fraud (
    event_dt timestamp,
    passport varchar(128),
    fio varchar(256),
    phone varchar(128),
    event_type varchar(256),
    report_dt timestamp
);