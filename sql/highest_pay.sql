select d.department_name, max(salary) as 'max salary' from employee_tbl as e join department_tbl as d on d.department_id= e.department_id
group by(d.department_name)