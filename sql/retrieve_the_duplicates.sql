select customer_id from day2 Group by (customer_id) having COUNT(customer_id)>1