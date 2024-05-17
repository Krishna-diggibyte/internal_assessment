CREATE TABLE weather (
    id INT PRIMARY KEY,
    recordDate DATE,
    temperature INT
);

INSERT INTO weather (id, recordDate, temperature) VALUES
(1, '2015-01-01', 10),
(2, '2015-01-02', 25),
(3, '2015-01-03', 20),
(4, '2015-01-04', 30);

WITH cte AS(
select * ,
lag(temperature,1) OVER(ORDER BY id) AS nextdata
FROM weather
)
select id from cte
where temperature>nextdata