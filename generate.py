import random

start = '''
drop table student;
create table student ( 
sno char(8), 
sname char(16) unique, 
sage int, 
sgender char(1), 
height float,
weight float,
primary key ( sno )
); 
create index student_name on student ( sname );
'''

file = open('test.sql', 'w')
file.write(start)

n = 10 ** 5

for i in range(n):
    sno = ''
    sname = ''
    for j in range(8):
        sno += chr(random.randint(0, 25) + ord('a'))
    for j in range(16):
        sname += chr(random.randint(0, 25) + ord('a'))
    sex = 'b'
    if random.random() >= 0.5:
        sex = 'g'
    height = random.random() * n
    weight = random.random() * n
    age = random.randint(0, n)
    insert = "insert into student values ('{}','{}',{},'{}',{:.2f},{:.2f});\n".format(sno, sname, age, sex, height,
                                                                                      weight)
    file.write(insert)

file.close()
