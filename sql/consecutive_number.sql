WITH cte AS (
    SELECT num,
           LEAD(num, 1) OVER (ORDER BY id) AS next1,
           LEAD(num, 2) OVER (ORDER BY id) AS next2
    FROM logs
)
select distinct num AS ConsecutiveNums
FROM cte
where num= next1 and num=next2;