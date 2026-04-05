select user_id 
from loans 
group by user_id 
HAVING 
SUM(CASE WHEN type='Refinance' THEN 1 ELSE 0 END) >=1 
and 
SUM(CASE WHEN type='InSchool' THEN 1 ELSE 0 END) >=1