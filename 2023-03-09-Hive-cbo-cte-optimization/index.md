# Common Table Expression optimizations using Calcite in Hive

## Introduction

In this doc we explore how we can leverage Calcite for performing 
common table expression (CTE) optimizations in Hive.

### Keywords

Common table expressions are also known under different names and are
related to the following research areas:
* common sub-expressions;
* multi-query optimization;
* operator re-use.

In the sequel we will use the following relational schema to express
the examples.

### Schema
```sql
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
```

### Q1

The following query returns all employees in the `Support` department
who have a higher salary from employees in the `Engineering` 
department.

```sql
SELECT sup.name, eng.name
FROM (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno) eng,
     (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno) sup
WHERE sup.salary > eng.salary
  AND eng.dname = 'Engineering'
  AND sup.dname = 'Support'
```
We could write the query in many different ways but we picked to isolate
the `INNER JOIN` in sub-queries to emphasize on the notion of common
expressions which is the focus of this document.

A very naive plan of Q1 without any kind of optimization is shown below.

```
LogicalProject(NAME=[$3], NAME0=[$0])
  LogicalFilter(condition=[AND(>($4, $1), =(CAST($2):VARCHAR, 'Engineering'), =(CAST($5):VARCHAR, 'Support'))])
    LogicalJoin(condition=[true], joinType=[inner])
      LogicalProject(NAME=[$2], SALARY=[$3], DNAME=[$6])
        LogicalJoin(condition=[=($1, $5)], joinType=[inner])
          LogicalTableScan(table=[[hr, emps]])
          LogicalTableScan(table=[[hr, depts]])
      LogicalProject(NAME=[$2], SALARY=[$3], DNAME=[$6])
        LogicalJoin(condition=[=($1, $5)], joinType=[inner])
          LogicalTableScan(table=[[hr, emps]])
          LogicalTableScan(table=[[hr, depts]])
```

It is pretty clear both from the SQL syntax and the plan that there are
two parts that are identical and these are commonly reffered to as 
common table experssions.
```
      LogicalProject(NAME=[$2], SALARY=[$3], DNAME=[$6])
        LogicalJoin(condition=[=($1, $5)], joinType=[inner])
          LogicalTableScan(table=[[hr, emps]])
          LogicalTableScan(table=[[hr, depts]])
```

If the plan was executed as such then we would have to scan `emps` and
`depts` tables, and join them twice.

An alternative evaluation would be to perform the join only once and
materialize the result somewhere to avoid the cost of recomputing the
whole operation from scratch.

Hive already has already ways to detect and optimize CTEs via the
[SharedWorkOptimizer][15]. The `SharedWorkOptimizer` applies late on
the compilation phase and works only for Tez execution engine.

In this work we aim to introduce CTE optimizations at a higher level,
namely in Calcite and CBO phase, to allow other execution engines to
take advantage of it with the main target being Impala.

## Background

### Shared work optimizer in Hive

The main goal is to eventually replace the [SharedWorkOptimizer][15] in
Hive. The optimizer has three main modes of operation:

#### Subtree merge

Merges two identical subtrees.

#### Remove semi-join

Merges multiple table scans together by potentiall removing semi-join 
optimization on some of them.

#### DPPUnion

TODO CHECK: Fuses two filtered table scans into a single one.

## Represent CTE in Calcite

In this section, we examine how we can represent common table expressions
in Calcite's relational algebra (aka. RelNode).

There is a long discussion under [CALCITE-481][3], on how to represent
sharing in Calcite's relational algebra. This discussion, along with
other improvements around recursive queries CALCITE-2812 [4], led to
the creation of the [Spool][5] operator. The Spool operator also appears
in other DBMS such as [MSSQL][6] and can take various forms.

### [Spool][5] 

Relational expression that iterates over its input and, in addition to
returning its results, will forward them into other consumers.

Calcite offers [TableSpool][7], a specialization of the Spool operator,
that writes results into a table.

By exploiting the TableSpool operator, we could represent the plan of
`Q1` as follows.

```
LogicalProject(name=[$3], name0=[$0])
  LogicalFilter(condition=[AND(>($4, $1), =($2, 'Engineering'), =($5, 'Support'))])
    LogicalJoin(condition=[true], joinType=[inner])
      LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[CTE]])
        LogicalProject(name=[$2], salary=[$3], name0=[$6])
          LogicalJoin(condition=[=($1, $5)], joinType=[inner])
            LogicalTableScan(table=[[hr, emps]])
            LogicalTableScan(table=[[hr, depts]])
      LogicalTableScan(table=[[CTE]])
```
Observe that the CTE representing the `INNER JOIN` between `emps` and
`depts` tables is placed under the `LogicalTableSpool` operator that
is supposed to forward/materialize the results into the `CTE` table.

From a logical standpoint, the plan is valid cause it says that in order
to evaluate the final join (cartesian product) we need to just evaluate
the first branch once and just read it twice.

From a physical perspective though, the order that the the joins inputs
are evaluated is important cause we don't want to scan the `CTE` table
before it gets populated and this information is not explicitly present
in the logical plan. Depending on how we decide to transform this
logical plan to physical plan (for Hive/Impala) this may become a 
limitation.

## Mapping Calcite CTE to physical

This section describes the process to transform a Calcite plan (RelNode)
with CTE information to a physical plan in Hive and Impala.

### Hive

In Hive, the Calcite plan (`RelNode`) is transformed to a physical plan
([Operator][8] graph) in two ways. Either directly by transforming the 
`RelNode` tree to an [optimized operator tree][9] when 
`hive.cbo.returnpath.hiveop` is `true` or indirectly by first 
transforming the `RelNode` to an [optimized SQL query][10] which is
then translated to `Operator` tree.

#### RelNode to Operator

The direct tranformation approach gives us a lot of flexibility on how
we can map the spools and respective scans operators to Hive operators.

One option would be to traverse the RelNode tree and replace the
spools/scans with appropriate graph edges adding additional reduce sink
operators if necessary.

Another option would be to actually materialize the result of table
spool operator in the appropriate (temporary) table and leave the
remaining scan operators as such. In this case, the order of
materializing the spools result into a table and subsequent scans should
be clearly defined.

#### RelNode to SQL

The indirect transformation is a bit trickier cause we need to find a
way to make sharing visible in the SQL level. The most straighforward
way to do this would be to put everything under a spool operator into
a `WITH` query clause.

For instance the query plan with the TableSpool operator outline above
could be mapped to the following SQL query.

```sql
WITH cte AS (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno)
SELECT sup.name, eng.name
FROM cte eng,
     cte sup
WHERE sup.salary > eng.salary
  AND eng.dname = 'Engineering'
  AND sup.dname = 'Support'
```

During SQL to Operator translation, Hive can opt to either [fully expand
CTE as subqueries][11] or [materialize the CTE][12] and replace references
in the original plan with simple table scans. The choice for
materializing the subquery or not depends on the following properties:
* hive.optimize.cte.materialize.threshold
* hive.optimize.cte.materialize.full.aggregate.only

The Hive plan for the SQL query above with CTE materialization enabled
is shown in the [appendix](#hive-cte-materialization-plan). `Stage-1`
contains the materialization of the cte and `Stage-4` the rest of the
query (cartesian product over the cte table).

### Impala

TODO

## Finding interesting CTEs 

In query Q1, it is trivial to spot that the `INNER JOIN` between `emps`
and `depts` appears multiple times inside the query and thus it is a
common table expression. However, it is not always the case, especially
when other optimizations take place and we start pushing/pulling
projections and filters all over the place. 

Finding CTEs and most importantly deciding about the evaluation
strategy is hard and computationally expensive. It has many similarities
with the problem of view based rewritting and [view based
recommendations][13]. 

The same techniques that are used to find interesting views to
materialize from a query workload could be used to find interesting CTEs
in a single query. Any kind of view recommendation engine/ruleset/API
could be used/adapted for this purpose; one such example is the
[lattice functionality][14] implemented in Calcite.

Nevertheless, for simple cases, such as Q1, a simple traversal of the
`RelNode` tree could be sufficient to identify sharing and extract CTEs. 
For example, we could add a simple step at the beginning of the
CBO phase (probably controlled by a flag) that searches for "interesting"
repeated patterns in the query structure. "Interesting" remains to be
defined but that could be expressions with multiple joins,
heavy aggregations, etc.

## Related work

There are many works in the literature that deal with CTE optimizations
focusing on different aspects such as how to represent them,
enumerating/reduce the search space, complexity analysis, etc.

For indicative purposes we highlight the work by[Amr El-Helw et al.][2],
since it can be a useful entrypoint in research literature.

## Appendix

### Calcite queries and plans

The code that was used to generate the Calcite query plans in this
document can be found here:

https://github.com/zabetak/calcite/commit/81387c68fc630b41a869db8f5c2f87cf50da80dc

### Hive CTE materialization plan

```
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-2 depends on stages: Stage-1
  Stage-4 depends on stages: Stage-2, Stage-0
  Stage-0 depends on stages: Stage-1
  Stage-3 depends on stages: Stage-4

STAGE PLANS:
  Stage: Stage-1
    Tez
#### A masked pattern was here ####
      Edges:
        Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 3 (SIMPLE_EDGE)
#### A masked pattern was here ####
      Vertices:
        Map 1 
            Map Operator Tree:
                TableScan
                  alias: e
                  filterExpr: deptno is not null (type: boolean)
                  Statistics: Num rows: 1 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                  Filter Operator
                    predicate: deptno is not null (type: boolean)
                    Statistics: Num rows: 1 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: deptno (type: int), name (type: varchar(10)), salary (type: decimal(6,2))
                      outputColumnNames: _col0, _col1, _col2
                      Statistics: Num rows: 1 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                      Reduce Output Operator
                        key expressions: _col0 (type: int)
                        null sort order: z
                        sort order: +
                        Map-reduce partition columns: _col0 (type: int)
                        Statistics: Num rows: 1 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                        value expressions: _col1 (type: varchar(10)), _col2 (type: decimal(6,2))
            Execution mode: vectorized, llap
            LLAP IO: all inputs
        Map 3 
            Map Operator Tree:
                TableScan
                  alias: d
                  filterExpr: deptno is not null (type: boolean)
                  Statistics: Num rows: 1 Data size: 98 Basic stats: COMPLETE Column stats: NONE
                  Filter Operator
                    predicate: deptno is not null (type: boolean)
                    Statistics: Num rows: 1 Data size: 98 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: deptno (type: int), name (type: varchar(10))
                      outputColumnNames: _col0, _col1
                      Statistics: Num rows: 1 Data size: 98 Basic stats: COMPLETE Column stats: NONE
                      Reduce Output Operator
                        key expressions: _col0 (type: int)
                        null sort order: z
                        sort order: +
                        Map-reduce partition columns: _col0 (type: int)
                        Statistics: Num rows: 1 Data size: 98 Basic stats: COMPLETE Column stats: NONE
                        value expressions: _col1 (type: varchar(10))
            Execution mode: vectorized, llap
            LLAP IO: all inputs
        Reducer 2 
            Execution mode: llap
            Reduce Operator Tree:
              Merge Join Operator
                condition map:
                     Inner Join 0 to 1
                keys:
                  0 _col0 (type: int)
                  1 _col0 (type: int)
                outputColumnNames: _col1, _col2, _col4
                Statistics: Num rows: 1 Data size: 231 Basic stats: COMPLETE Column stats: NONE
                Select Operator
                  expressions: _col1 (type: varchar(10)), _col2 (type: decimal(6,2)), _col4 (type: varchar(10))
                  outputColumnNames: _col0, _col1, _col2
                  Statistics: Num rows: 1 Data size: 231 Basic stats: COMPLETE Column stats: NONE
                  File Output Operator
                    compressed: false
                    Statistics: Num rows: 1 Data size: 231 Basic stats: COMPLETE Column stats: NONE
                    table:
                        input format: org.apache.hadoop.mapred.TextInputFormat
                        output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                        serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                        name: default.cte

  Stage: Stage-2
    Dependency Collection

  Stage: Stage-4
    Tez
#### A masked pattern was here ####
      Edges:
        Reducer 5 <- Map 4 (XPROD_EDGE), Map 6 (XPROD_EDGE)
#### A masked pattern was here ####
      Vertices:
        Map 4 
            Map Operator Tree:
                TableScan
                  alias: eng
                  filterExpr: ((CAST( dname AS STRING) = 'Engineering') and salary is not null) (type: boolean)
                  Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                  Filter Operator
                    predicate: ((CAST( dname AS STRING) = 'Engineering') and salary is not null) (type: boolean)
                    Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: name (type: varchar(10)), salary (type: decimal(6,2))
                      outputColumnNames: _col0, _col1
                      Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                      Reduce Output Operator
                        null sort order: 
                        sort order: 
                        Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                        value expressions: _col0 (type: varchar(10)), _col1 (type: decimal(6,2))
            Execution mode: vectorized, llap
            LLAP IO: all inputs
        Map 6 
            Map Operator Tree:
                TableScan
                  alias: sup
                  filterExpr: ((dname = 'Support') and salary is not null) (type: boolean)
                  Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                  Filter Operator
                    predicate: ((dname = 'Support') and salary is not null) (type: boolean)
                    Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: name (type: varchar(10)), salary (type: decimal(6,2))
                      outputColumnNames: _col0, _col1
                      Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                      Reduce Output Operator
                        null sort order: 
                        sort order: 
                        Statistics: Num rows: 1 Data size: 300 Basic stats: COMPLETE Column stats: NONE
                        value expressions: _col0 (type: varchar(10)), _col1 (type: decimal(6,2))
            Execution mode: vectorized, llap
            LLAP IO: all inputs
        Reducer 5 
            Execution mode: llap
            Reduce Operator Tree:
              Merge Join Operator
                condition map:
                     Inner Join 0 to 1
                keys:
                  0 
                  1 
                outputColumnNames: _col0, _col1, _col2, _col3
                residual filter predicates: {(_col3 > _col1)}
                Statistics: Num rows: 1 Data size: 601 Basic stats: COMPLETE Column stats: NONE
                Select Operator
                  expressions: _col2 (type: varchar(10)), _col0 (type: varchar(10))
                  outputColumnNames: _col0, _col1
                  Statistics: Num rows: 1 Data size: 601 Basic stats: COMPLETE Column stats: NONE
                  File Output Operator
                    compressed: false
                    Statistics: Num rows: 1 Data size: 601 Basic stats: COMPLETE Column stats: NONE
                    table:
                        input format: org.apache.hadoop.mapred.SequenceFileInputFormat
                        output format: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
                        serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe

  Stage: Stage-0
    Move Operator
      files:
          hdfs directory: true
#### A masked pattern was here ####

  Stage: Stage-3
    Fetch Operator
      limit: -1
      Processor Tree:
        ListSink
```

## References

[1]: https://lists.apache.org/thread/xwqzml4xbb7x0d4wpp2v62zp79tf35sk
[2]: http://www.vldb.org/pvldb/vol8/p1704-elhelw.pdf
[3]: https://issues.apache.org/jira/browse/CALCITE-481
[4]: https://issues.apache.org/jira/browse/CALCITE-2812
[5]: https://github.com/apache/calcite/blob/ee9b80b0b68d442991dfaa142722e3488ec73e79/core/src/main/java/org/apache/calcite/rel/core/Spool.java
[6]: https://sqlserverfast.com/epr/table-spool/
[7]: https://github.com/apache/calcite/blob/ee9b80b0b68d442991dfaa142722e3488ec73e79/core/src/main/java/org/apache/calcite/rel/core/TableSpool.java
[8]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/exec/Operator.java
[9]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/parse/CalcitePlanner.java#L575
[10]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/parse/CalcitePlanner.java#L586
[11]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/parse/SemanticAnalyzer.java#L2245
[12]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/parse/SemanticAnalyzer.java#L2249
[13]: https://www.vldb.org/conf/2000/P496.pdf
[14]: https://calcite.apache.org/docs/lattice.html
[15]: https://github.com/apache/hive/blob/dfb1dd9edba4f9a3488fb53ac1d384ca82fa7742/ql/src/java/org/apache/hadoop/hive/ql/optimizer/SharedWorkOptimizer.java
