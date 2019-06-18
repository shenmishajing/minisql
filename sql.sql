create table stu ( name char(10) unique , age int, id char(8), primary key ( id ));
create index stu_name on stu ( name );
insert into stu values ('xiao', 12, '12345');
insert into stu values ('aaaa', 10, '23456');
insert into stu values ('bbbbb',12, '34567');
insert into stu values ('cccccc', 12, '45678');
insert into stu values ('dddddd', 13, '45678');
select * from stu;
select * from stu where age > 10;
select * from stu where name = 'xiao';
select * from stu where id = '12345';

