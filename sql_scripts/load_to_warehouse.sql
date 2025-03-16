-- Загружаем данные в хранилище

-- добавляем записи в хранилище транзакций
insert into bank.dwh_fact_transactions (
	trans_id, 
	trans_date, 
	card_num, 
	oper_type, 
	amt, 
	oper_result, 
	terminal
)
select -- выбираем данные из стейджинговой таблицы
    transaction_id,
    transaction_date,
    card_num,
    oper_type,
    amount,
    oper_result,
    terminal
from bank.stg_transactions 
where transaction_id not in (select trans_id from bank.dwh_fact_transactions);
-- добавляем только те записи, которых еще нет в хранилище 

update bank.dwh_dim_terminals_hist -- обновляем записи в хранилище терминалов
set effective_to = now() - interval '1 second' -- версия записи больше не актуальна
where terminal_id in (
    select terminal_id
    from bank.stg_terminals
    where terminal_id in (
        select terminal_id
        from bank.dwh_dim_terminals_hist
        where effective_to = '2999-12-31 23:59:59'
    )
);

-- добавляем записи в хранилище терминалов
insert into bank.dwh_dim_terminals_hist (
	terminal_id, 
	terminal_type, 
	terminal_city, 
	terminal_address, 
	effective_from, 
	effective_to, 
	deleted_flg
)
select 
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    now(), -- текущее время
    '2999-12-31 23:59:59', -- запись актуальна
    0 -- запись не удалена
from bank.stg_terminals
where terminal_id not in (select terminal_id from bank.dwh_dim_terminals_hist);

-- добавляем записи в хранилище черного списка паспортов
insert into bank.dwh_fact_passport_blacklist (passport_num, entry_dt)
select 
    passport,
    date
from bank.stg_passport_blacklist
where passport not in (select passport_num from bank.dwh_fact_passport_blacklist);