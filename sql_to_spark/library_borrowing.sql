'''
Imagine you're working for a library and you're tasked with generating a report on the borrowing habits of patrons. You have two tables in your database: Books and Borrowers.

 

Write an SQL to display the name of each borrower along with a comma-separated list of the books they have borrowed in alphabetical order, display the output in ascending order of Borrower Name.

'''

SELECT
bor.borrowerName,
STRING_AGG(bo.bookName,', ' ORDER BY bo.bookName) AS borrowedBooks
FROM Borrowers bor 
JOIN Books bo 
ON bor.bookId=bo.bookId
GROUP BY bor.borrowerName
ORDER BY bor.borrowerName ASC