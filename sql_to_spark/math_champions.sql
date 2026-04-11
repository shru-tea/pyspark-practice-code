'''
Math Champions
Q. You are provided with two tables: Students and Grades. 
Write a query to find students who have higher grade in Math than the 
average grades of all the students together in Math. Display student name and grade 
in Math order by grades.
 
Tables: Students
+--------------+-------------+
| COLUMN_NAME | DATA_TYPE |
+--------------+-------------+
| class_id | int |
| student_id | int |
| student_name | varchar(20) |
+--------------+-------------+

Tables: Grades
+-------------+-------------+
| COLUMN_NAME | DATA_TYPE |
+-------------+-------------+
| student_id | int |
| subject | varchar(20) |
| grade | int |
+-------------+-------------+
'''



SELECT s.student_name, g.grade
FROM Students s JOIN Grades g 
ON s.student_id=g.student_id
WHERE g.subject='Math' AND g.grade > (SELECT AVG(grade) FROM Grades WHERE subject='Math')
ORDER BY g.grade
