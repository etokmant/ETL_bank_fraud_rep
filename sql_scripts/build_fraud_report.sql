-- Строим витрину мошеннических операций

-- Паспорт в черном списке на момент транзакции
with last_fraud_blacklist as ( 
    select -- выбираем все транзакции, совершенные с использованием паспорта в черном списке
        t.trans_date as event_dt, -- дата и время транзакции  из таблицы dwh_fact_transactions
        c.passport_num as passport, -- номер паспорта клиента из таблицы clients
        concat(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio, -- ФИО клиента
        c.phone, -- телефон клиента
        'Паспорт в черном списке' as event_type,
        now() as report_dt, -- время создания отчета
        row_number() over ( -- порядковый номер строки в рамках заданного окна
            partition by c.passport_num -- группируем по номеру паспорта
            order by t.trans_date desc -- сортируем по дате транзакции в порядке убывания
            -- при этом последняя транзакция получает rn = 1
        ) as rn 
    from bank.dwh_fact_transactions t
    join bank.cards cd on t.card_num = cd.card_num -- соединяем таблицу транзакций с таблицей карт по номеру карты
    join bank.accounts a on cd.account = a.account -- соединяем таблицу карт с таблицей счетов по номеру счета
    join bank.clients c on a.client = c.client_id -- соединяем таблицу счетов с таблицей клиентов по идентификатору клиента
    where c.passport_num in ( -- выбираем паспорта из черного списка, добавленные до момента транзакции
        select passport
        from bank.stg_passport_blacklist
        where date::timestamp <= t.trans_date
    )
)
insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
-- вставляем данные из временной таблицы в витрину, если запись еще не существует
select 
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
from last_fraud_blacklist
where rn = 1 -- выбираем только последнюю транзакцию для каждого паспорта
and not exists ( -- записи еще нет в витрине
    select 1 from bank.rep_fraud rf -- возвращает 1, если найдена хотя бы одна строка в таблице rep_fraud - тогда not exists возвращает false, и текущая запись не добавляется в витрину 
    where rf.passport = last_fraud_blacklist.passport -- номер паспорта совпадает
    and rf.event_type = last_fraud_blacklist.event_type -- тип события совпадает
);

-- Паспорт просрочен на момент транзакции
with last_fraud_expired_passport as (
    select -- выбираем все транзакции, совершенные с использованием просроченного паспорта
        t.trans_date as event_dt,
        c.passport_num as passport,
        concat(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
        c.phone,
        'Просроченный паспорт' as event_type,
        now() as report_dt,
        row_number() over (
            partition by c.passport_num 
            order by t.trans_date desc
        ) as rn
    from bank.dwh_fact_transactions t
    join bank.cards cd on t.card_num = cd.card_num
    join bank.accounts a on cd.account = a.account
    join bank.clients c on a.client = c.client_id
    where c.passport_valid_to is not null -- у паспорта есть срок действия
    and c.passport_valid_to < t.trans_date -- срок действия паспорта истек на момент транзакции
)
insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
from last_fraud_expired_passport
where rn = 1
and not exists (
    select 1 from bank.rep_fraud rf
    where rf.passport = last_fraud_expired_passport.passport
    and rf.event_type = last_fraud_expired_passport.event_type
);

-- Счет просрочен на момент транзакции
with last_fraud_expired_account as (
    select -- выбираем все транзакции, совершенные с использованием счетов, срок действия которых истек на момент транзакции.
        t.trans_date as event_dt,
        c.passport_num as passport,
        concat(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
        c.phone,
        'Просроченный договор' as event_type,
        now() as report_dt,
        row_number() over (
            partition by c.passport_num 
            order by t.trans_date desc
        ) as rn
    from bank.dwh_fact_transactions t
    join bank.cards cd on t.card_num = cd.card_num
    join bank.accounts a on cd.account = a.account
    join bank.clients c on a.client = c.client_id
    where a.valid_to is not null 
    and a.valid_to < t.trans_date -- срок действия счета истек до момента транзакции
)
insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
from last_fraud_expired_account
where rn = 1
and not exists (
    select 1 from bank.rep_fraud rf
    where rf.passport = last_fraud_expired_account.passport
    and rf.event_type = last_fraud_expired_account.event_type
);

-- Операции в разных городах в течение часа
with city_changes as (
    select
        t.card_num,
        date_trunc('hour', t.trans_date) as hour_start,
        count(distinct trm.terminal_city) as city_count
    from bank.dwh_fact_transactions t
    join bank.dwh_dim_terminals_hist trm on t.terminal = trm.terminal_id
    group by t.card_num, date_trunc('hour', t.trans_date)
    having count(distinct trm.terminal_city) > 1
),
last_fraud_city_changes as (
    select
        t.trans_date as event_dt,
        c.passport_num as passport,
        concat(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
        c.phone,
        'Разные города' as event_type,
        now() as report_dt,
        row_number() over (
            partition by c.passport_num 
            order by t.trans_date desc
        ) as rn
    from bank.dwh_fact_transactions t
    join bank.cards cd on t.card_num = cd.card_num
    join bank.accounts a on cd.account = a.account
    join bank.clients c on a.client = c.client_id
    join city_changes cc on t.card_num = cc.card_num
        and date_trunc('hour', t.trans_date) = cc.hour_start
)
insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
from last_fraud_city_changes
where rn = 1
and not exists (
    select 1 from bank.rep_fraud rf
    where rf.passport = last_fraud_city_changes.passport
    and rf.event_type = last_fraud_city_changes.event_type
);

-- Подбор суммы (3 неудачные попытки, затем успешная)
-- t1, t2 - копии таблицы dwh_fact_transactions для сравнения транзакций
with fraud_attempts as (
    select -- цепочки неудачных транзакций с уменьшающейся суммой
        t1.card_num,
        t1.trans_date as start_time,
        t1.amt as start_amount,
        t2.trans_date as next_time,
        t2.amt as next_amount,
        t2.oper_result as next_result,
        row_number() over (partition by t1.card_num, t1.trans_date order by t2.trans_date) as rn,
        -- нумеруем транзакции для каждой карты и каждой начальной транзакции
        count(*) over (partition by t1.card_num, t1.trans_date) as chain_len -- общее количество транзакций в цепочке        
    from bank.dwh_fact_transactions t1
    join bank.dwh_fact_transactions t2 on t1.card_num = t2.card_num
        and t2.trans_date between t1.trans_date and t1.trans_date + interval '20 minutes'
        -- ищем транзакции, которые произошли в течение 20 минут после отклоненной транзакции
        and t2.amt < t1.amt -- сумма следующей транзакции меньше суммы предыдущей
    where t1.oper_result = 'REJECT' -- выбираем только отклоненные транзакции
),
fraud_chains as (
    select -- выбираем цепочки, где было не менее 3 неудачных попыток и нет успешных транзакций до завершения
        card_num,
        start_time,
        chain_len,
        sum(case when next_result = 'SUCCESS' and rn < chain_len then 1 else 0 end) as success_count
        -- если транзакция успешная и произошла до завершения цепочки, считаем ее как 1, иначе как 0
        -- суммируем все значения для каждой цепочки
    from fraud_attempts
    group by card_num, start_time, chain_len -- группируем данные по номеру карты, времени начала цепочки и общему кол-ву транзакций в цепочке
    having chain_len >= 3 -- оставляем только цепочки, где не менее 3 транзакций
        and sum(case when next_result = 'SUCCESS' and rn < chain_len then 1 else 0 end) = 0
        -- и где не было успешных транзакций до завершения цепочки
),
last_successful_operation as ( -- успешная транзакция, завершающая цепочку
    select
        t.card_num,
        t.trans_date as event_dt,
        c.passport_num as passport,
        concat(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
        c.phone,
        'Подбор суммы' as event_type,
        now() as report_dt,
        row_number() over (
            partition by c.passport_num -- группируем данные по номеру паспорта клиента
            order by t.trans_date desc -- сортируем транзакции внутри каждой группы по дате в порядке убывания
        ) as rn
    from bank.dwh_fact_transactions t
    join bank.cards cd on t.card_num = cd.card_num
    join bank.accounts a on cd.account = a.account
    join bank.clients c on a.client = c.client_id
    join fraud_chains fc on t.card_num = fc.card_num -- соединяем транзакции с цепочками мошеннических попыток по номеру карты
        and t.trans_date > fc.start_time -- транзакции, произошедшие после начала цепочки
        and t.trans_date <= fc.start_time + interval '20 minutes' -- транзакции, произошедшие в течение 20 минут после начала цепочки
    where t.oper_result = 'SUCCESS' -- выбираем только успешные транзакции, которые завершают цепочку 
)
insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
from last_successful_operation
where rn = 1
and not exists (
    select 1 from bank.rep_fraud rf
    where rf.passport = last_successful_operation.passport
    and rf.event_type = last_successful_operation.event_type
);