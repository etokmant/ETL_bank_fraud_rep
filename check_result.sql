-- Проверка результатов etl-процесса

set search_path to bank; -- схема, в которой будут выполняться запросы

select * from information_schema.tables where table_schema = 'bank'; -- таблицы в схеме bank

select * from rep_fraud; -- витрина мошеннических операций

select * from metadata; -- метаданные

select * from dwh_fact_transactions; -- хранилище транзакций

select * from dwh_fact_passport_blacklist; -- хранилище черного списка паспортов

select * from dwh_dim_terminals_hist; -- хранилище терминалов

select * -- удаленные терминалы 
from bank.dwh_dim_terminals_hist 
where deleted_flg = 1; 

select * --  терминалы с измененными данными
from bank.dwh_dim_terminals_hist
where terminal_id in (
    select terminal_id
    from bank.dwh_dim_terminals_hist
    group by terminal_id
    having count(*) > 1
)
order by terminal_id, effective_from;