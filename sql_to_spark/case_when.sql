SELECT 
  employee_id,
  salary,
  CASE 
    WHEN salary > 100000 THEN 'High'
    WHEN salary > 50000 THEN 'Medium'
    ELSE 'Low'
  END AS salary_band
FROM employees;