# Mini SQL Engine

A mini sql engine which can run a subset of SQL queries using command line interface.

## Dataset:

1. csv files for tables.
   - If a file is : File1.csv , the table name should be File1
   - There should be no tab separation or space separation, but the values can be in double quotes or without quotes.
2. All the elements in files should be integers.
3. A file named: metadata.txt should be given which will have the following structure for each table:
   ```<begin_table>
    <table_name>
    <attribute 1>
    ..
    ..
    <attribute N>
    <end_table>```
4. Column names should be unique among all the tables. So column names should not​ preceded by table names in SQL queries.

## Type of Queries

The the following set of queries are supported:
1. Project​ Columns(could be any number of columns) from one or more tables :
   - `Select * from table_name`;
   - `Select col1, col2 from table_name`;
2. Aggregate functions​: Simple aggregate functions on a single column. Sum, average, max, min and count. 
    - `Select max(col1) from table_name`;
3. Select/project with ​distinct​ from one table: (distinct of a pair of values indicates the pair should be distinct) 
    - `Select distinct col1, col2 from table_name;`
4. Select with ​WHERE​ from one or more tables :
    - `Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;`
    - In the where queries, there should be a maximum of one AND/ORoperator with no NOT operators.
    - Relational operators that are to be handled in the assignment, the operators include "< , >, <=, >=, =".
5.  Select/Project Columns(could be any number of columns) from table using “​group by​”:  
    - `Select col1, COUNT(col2) from table_name group by col1.`
    - In the group by queries, Sum/Average/Max/Min/Count can be used as aggregate functions.
6. Select/Project Columns(could be any number of columns) from table in ascending/descending order according to a column using “​order by”​:
    - `Select col1,col2 from table_name order by col1 ASC|DESC`.
    - At max only one column can be used to sort the rows.
    - Query can have multiple tables

## Format of Input :

`python3 sql_engine.py "<SQL Query>"`

## Format of Output  :

    <Table1.column1,Table1.column2,...TableN.columnM>
                        Row 1 
                        Row 2 
                        ....... 
                        Row N