drop table stu;
create table stu ( name char(10), age int, primary key ( name ));
insert into stu values ('xiao', 12);
insert into stu values ('aaaa', 10);
insert into stu values ('bbbbb',12);
insert into stu values ('cccccc', 12);
insert into stu values ('dddddd', 13);
select * from stu;
select * from stu where age = 10;
select * from stu where name = 'xiao';
select * from stu where name <= 'x' and age > 5 and age < 60 and age > 10 and age <> 12;

