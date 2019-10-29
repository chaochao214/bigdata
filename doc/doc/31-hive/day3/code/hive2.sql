********************************************************行转列**************************************************

孙悟空	白羊座	A
大海	射手座	A
宋宋	白羊座	B
猪八戒	白羊座	A
凤姐	射手座	A

射手座,A            大海|凤姐
白羊座,A            孙悟空|猪八戒
白羊座,B            宋宋

create table person_info(
name string, 
constellation string, 
blood_type string) 
row format delimited fields terminated by "\t";

select CONCAT_WS(",",constellation,blood_type) c_b,name from person_info;

t1
射手座,A            大海
射手座,A			凤姐
白羊座,A            孙悟空
白羊座,A			猪八戒
白羊座,B            宋宋

select 
 t1.c_b,
 CONCAT_WS("|",COLLECT_SET(t1.name))
from (
  select 
   CONCAT_WS(",",constellation,blood_type) c_b,
   name 
  from person_info) t1
group by 
t1.c_b;

CONCAT(string A/col, string B/col…)：返回输入字符串连接后的结果，支持任意个输入字符串;
CONCAT_WS(separator, str1, str2,...)：它是一个特殊形式的 CONCAT()。第一个参数剩余参数间的分隔符。分隔符可以是与剩余参数一样的字符串。如果分隔符是 NULL，返回值也将为 NULL。这个函数会跳过分隔符参数后的任何 NULL 和空字符串。分隔符将被加到被连接的字符串之间;
COLLECT_SET(col)：函数只接受基本数据类型，它的主要作用是将某字段的值进行去重汇总，产生array类型字段。


********************************************************列转行**************************************************
《疑犯追踪》	悬疑,动作,科幻,剧情
《Lie to me》	悬疑,警匪,动作,心理,剧情
《战狼2》	战争,动作,灾难

create table movie_info(
    movie string, 
    category array<string>) 
row format delimited fields terminated by "\t"
collection items terminated by ",";

select EXPLODE(category) from movie_info;

select movie,EXPLODE(category) from movie_info;

《疑犯追踪》      悬疑
《疑犯追踪》      动作
《疑犯追踪》      科幻
《疑犯追踪》      剧情
《Lie to me》   悬疑
《Lie to me》   警匪
《Lie to me》   动作
《Lie to me》   心理
《Lie to me》   剧情
《战狼2》        战争
《战狼2》        动作
《战狼2》        灾难

select movie,category_name
from movie_info
LATERAL VIEW EXPLODE(category) tmpTable as category_name;

EXPLODE(col)：将hive一列中复杂的array或者map结构拆分成多行。
LATERAL VIEW
用法：LATERAL VIEW udtf(expression) tableAlias AS columnAlias
解释：用于和split, explode等UDTF一起使用，它能够将一列数据拆成多行数据，在此基础上可以对拆分后的数据进行聚合。

********************************************************窗口函数************************************************
OVER()：指定分析函数工作的数据窗口大小，这个数据窗口大小可能会随着行的变而变化
CURRENT ROW：当前行
n PRECEDING：往前n行数据
n FOLLOWING：往后n行数据
UNBOUNDED：起点，UNBOUNDED PRECEDING 表示从前面的起点， UNBOUNDED FOLLOWING表示到后面的终点
LAG(col,n)：往前第n行数据
LEAD(col,n)：往后第n行数据
NTILE(n)：把有序分区中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，NTILE返回此行所属的组的编号。注意：n必须为int类型。

jack,2017-01-01,10
tony,2017-01-02,15
jack,2017-02-03,23
tony,2017-01-04,29
jack,2017-01-05,46
jack,2017-04-06,42
tony,2017-01-07,50
jack,2017-01-08,55
mart,2017-04-08,62
mart,2017-04-09,68
neil,2017-05-10,12
mart,2017-04-11,75
neil,2017-06-12,80
mart,2017-04-13,94

create table business(
name string, 
orderdate string,
cost int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


（1）查询在2017年4月份购买过的顾客及总人数
select name,count(*) 
from business
where substring(orderdate,1,7)="2017-04"
group by name;
+-------+------+--+
| name  | _c1  |
+-------+------+--+
| jack  | 1    |
| mart  | 4    |
+-------+------+--+

select name,count(*) over()
from business
where substring(orderdate,1,7)="2017-04"
group by name;
+-------+------+--+
| name  | _c1  |
+-------+------+--+
| jack  | 2    |
| mart  | 2    |
+-------+------+--+

（2）查询顾客的购买明细及月购买总额
select *,sum(cost) over(distribute by month(orderdate)) from business;
select *,sum(cost) over(partition by month(orderdate)) from business;

（3）要将cost按照日期进行累加
select *,sum(cost) over(sort by orderdate rows between UNBOUNDED PRECEDING and CURRENT ROW) from business;
select *,sum(cost) over(sort by orderdate rows between 1 PRECEDING and 1 FOLLOWING) from business;
select *,sum(cost) over(distribute by name sort by orderdate rows between UNBOUNDED PRECEDING and CURRENT ROW) from business;
select *,sum(cost) over(sort by orderdate rows between CURRENT ROW and UNBOUNDED FOLLOWING) from business;

（4）查询顾客上次的购买时间
select *,
lag(orderdate,1) over(distribute by name sort by orderdate),
lead(orderdate,1) over(distribute by name sort by orderdate) from business;

select *,
lag(orderdate,1,"2016-12-31") over(distribute by name sort by orderdate)
from business;

（5）查询前20%时间的订单信息
select *,ntile(5) over(sort by orderdate) gid from business where gid=1;X

select *,ntile(5) over(sort by orderdate) gid from business having gid=1;X

select *
from(
select *,ntile(5) over(sort by orderdate) gid from business
) t
where
gid=1;

select * from (
    select name,orderdate,cost, ntile(5) over(order by orderdate) sorted
    from business
) t
where sorted = 1;
