WITH table1 as ( 
    select * from {{ ref("fct_transactions") }})
select journal_id from table1 where journal_id = 1