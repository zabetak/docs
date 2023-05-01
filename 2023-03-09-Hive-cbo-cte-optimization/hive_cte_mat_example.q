set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.explain.user=false;

CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(6, 2)
);

CREATE TABLE depts
(
    deptno INTEGER,
    name   VARCHAR(10)
);

EXPLAIN
WITH cte AS (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno)
SELECT sup.name, eng.name
FROM cte eng,
     cte sup
WHERE sup.salary > eng.salary
  AND eng.dname = 'Engineering'
  AND sup.dname = 'Support'