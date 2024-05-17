CREATE TABLE Product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50)
);


INSERT INTO Product (product_id, product_name) VALUES
(100, 'Nokia'),
(200, 'Apple'),
(300, 'Samsung');


CREATE TABLE Sales (
    sale_id INT PRIMARY KEY,
    product_id INT,
    year INT,
    quantity INT,
    price INT
);


INSERT INTO Sales (sale_id, product_id, year, quantity, price) VALUES
(1, 100, 2008, 10, 5000),
(2, 100, 2009, 12, 5000),
(7, 200, 2011, 15, 9000);

select p.product_name, s.year, s.price
from Sales s inner join Product p
on s.product_id = p.product_id;
