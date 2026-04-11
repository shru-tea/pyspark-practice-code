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

SELECT s.student_name, g.grade
FROM Students s JOIN Grades g 
ON s.student_id=g.student_id
WHERE g.subject='Math' AND g.grade > (SELECT AVG(grade) FROM Grades WHERE subject='Math')
ORDER BY g.grade

'''

import pyspark.sql.functions as F

df_join=df_students.join(
          df_grades,
          on="student_id",
          how="inner")

avg_grade=df_grades.filter(F.col("subject")=='Math').agg(F.avg("grade")).collect()[0][0]

math_df=df_join.filter((F.col("subject")=='Math') & (F.col("grade")>avg_grade))

result_df=result_df.select("student_name","grade").orderBy("grade")
result_df.show()