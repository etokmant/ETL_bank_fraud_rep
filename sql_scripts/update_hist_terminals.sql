-- Добавление и удаление терминалов с сохранением истории

-- Закрываем старые записи для измененных терминалов
update bank.dwh_dim_terminals_hist -- обновляем данные в хранилище
set effective_to = date_trunc('second', now() - interval '1 second') -- effective_to устанавливаем на 1 секунду раньше текущего времени - запись больше не актуальна
where terminal_id in ( -- обновляем только те записи, у которых terminal_id находится в результате подзапроса
    select st.terminal_id
    from bank.stg_terminals st
    join bank.dwh_dim_terminals_hist dwh -- соединяем данные из stg-таблцы с данными в хранилище
    on st.terminal_id = dwh.terminal_id
    where (st.terminal_type <> dwh.terminal_type -- проверяем, изменился ли хотя бы один из параметров терминала
       or st.terminal_city <> dwh.terminal_city
       or st.terminal_address <> dwh.terminal_address)
    and dwh.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- проверяем, что запись в хранилище актуальна
);

-- Добавляем новые записи для измененных терминалов
with changed_terminals as ( -- создаем временную таблицу с терминалами, данные которых изменились по сравнению с последней записью в хранилище
    select distinct on (st.terminal_id) -- если для одного терминала есть несколько записей в stg_terminals, выбираем только одну - 1-ю в порядке сортировки (чтобы избежать дублирования)
        st.terminal_id,
        st.terminal_type,
        st.terminal_city,
        st.terminal_address,
        now() as effective_from -- время, с которого новая запись становится актуальной
    from bank.stg_terminals st
    join bank.dwh_dim_terminals_hist dwh
    on st.terminal_id = dwh.terminal_id
    where (st.terminal_type <> dwh.terminal_type -- проверяем, изменился ли один из параметров
       or st.terminal_city <> dwh.terminal_city
       or st.terminal_address <> dwh.terminal_address)
)
insert into bank.dwh_dim_terminals_hist ( -- добавляем новые записи для этих терминалов в хранилище 
    terminal_id, 
    terminal_type, 
    terminal_city, 
    terminal_address,
    effective_from, 
    effective_to, 
    deleted_flg -- флаг, указывающий, удален ли терминал
)
select -- выбираем, какие данные будут вставлены в хранилище 
    ct.terminal_id,
    ct.terminal_type,
    ct.terminal_city,
    ct.terminal_address,
    ct.effective_from,
    to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), -- время, до которого запись остается актуальной (техническая бесконечность)
    0
from changed_terminals ct;

-- Добавляем новые терминалы
with new_terminals as ( -- временная таблица, содержащая только терминалы, которые есть в stg-таблице, но отсутствуют в хранилище
    select -- выбираем данные из stg-таблицы 
        st.terminal_id,
        st.terminal_type,
        st.terminal_city,
        st.terminal_address,
        now() as effective_from
    from bank.stg_terminals st
    where not exists ( -- проверяем, что терминала нет в хранилище
        select 1 -- нам надо просто проверить наличие или отсутствие записи, содержимое не имеет значения   
        from bank.dwh_dim_terminals_hist dwh
        where dwh.terminal_id = st.terminal_id
    ) -- если терминал не найден, он будет выбран
)
insert into bank.dwh_dim_terminals_hist ( -- указываем поля, в которые попадут данные 
    terminal_id, 
    terminal_type, 
    terminal_city, 
    terminal_address,
    effective_from, 
    effective_to, 
    deleted_flg
)
select -- выбираем данные из временной таблицы new_terminals
    nt.terminal_id,
    nt.terminal_type,
    nt.terminal_city,
    nt.terminal_address,
    nt.effective_from,
    to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    0
from new_terminals nt;

-- Помечаем удаленные терминалы
update bank.dwh_dim_terminals_hist
set effective_to = date_trunc('second', now() - interval '1 second'), -- текущее время минус одна секунда - запись больше не актуальна
    deleted_flg = 1 -- флаг, указывающий, что запись удалена 
where terminal_id not in ( -- обновляем только записи, у которых terminal_id отсутствует в stg-таблице
    select terminal_id
    from bank.stg_terminals 
)
and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'); -- проверяем, что запись в хранилище еще актуальна 