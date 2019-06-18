create table stu ( name char(10), age int, primary key ( name ));
insert into stu values ('xiao', 12);
insert into stu values ('aaaa', 10);
insert into stu values ('bbbbb',12);
insert into stu values ('cccccc', 12);
insert into stu values ('cccccc', 13);
select * from stu;
select * from stu where age = 12;
