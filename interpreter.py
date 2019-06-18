import os
import API
import time


def find_last_str(s: str, string: str):
    result = -1
    while True:
        cur_pos = string.find(s, result + 1)
        if cur_pos == -1:
            return result
        else:
            result = cur_pos


def print_welcome():
    print('Welcome to the MySQL monitor.  Commands end with ; or \\g.')
    print('You can use it like MySQL with less command')
    print("Type 'help;' or '\\h' for help. Type '\\c' to clear the current input statement.")


def get_command():
    command = ''
    print('minisql> ', end = '')
    string = input()
    while True:
        if ';' in string:
            command += string[:string.find(';')]
            break
        command += string
        print('      -> ', end = '')
        string = input()
    return command


def execute_commands(api, file_name):
    if os.path.exists(file_name):
        file = open(file_name, 'r')
        command = ''
        for line in file:
            if ';' in line:
                command += line[:line.find(';')]
                try:
                    print('start run command: ' + command)
                    start = time.time()
                    parse_sql(api, command)
                    end = time.time()
                    print(f'finish in {end - start} s')
                except AssertionError as e:
                    print(e)
                command = ''
            else:
                command += line
    else:
        print('文件不存在')


def parse_sql(api, sql):
    sql = sql.replace('\n', ' ').replace('\t', '')
    sql_strs = sql.split()
    command = sql_strs[0].lower()

    if command == 'create':
        command_type = sql_strs[1].lower()

        if command_type == 'table':
            table_name = sql_strs[2]
            start = sql.find('(')
            end = find_last_str(')', sql)
            assert start != -1 and end != -1, '非法SQL，缺少括号'
            artribute_table = sql[start + 1:end].split(',')
            assert len(artribute_table) > 0, '缺少属性'
            api.create_table(table_name, artribute_table)

        elif command_type == 'index':
            assert sql_strs[3] == 'on', 'index非法指令'
            index_name = sql_strs[2]
            table_name = sql_strs[4]
            start = sql.find('(')
            end = sql.find(')')
            artribute_name = sql[start + 1:end]
            artribute_name = artribute_name.replace(' ', '')
            api.create_index(table_name, index_name, artribute_name)

    elif command == 'drop':
        command_type = sql_strs[1].lower()
        if command_type == 'index':
            index_name = sql_strs[2]
            api.drop_index(index_name)
        elif command_type == 'table':
            table_name = sql_strs[2]
            api.drop_table(table_name)

    elif command == 'delete':
        assert sql_strs[1].lower() == 'from', 'SQL非法指令'
        table_name = sql_strs[2]
        if len(sql_strs) > 3:
            if sql_strs[3].lower() == 'where':
                start = sql.lower().find('where')
                conditions = sql[start + 5:]
                conditions = conditions.split('and')
                api.delete_records(table_name, conditions)
        else:  # 若缺省条件
            conditions = []
            api.delete_records(table_name, conditions)

    elif command == 'insert':
        assert sql_strs[1].lower() == 'into', 'SQL非法指令'
        table_name = sql_strs[2]
        start = sql.lower().find('values')
        assert start != -1, 'SQL缺少values关键字'
        tuple_str = sql[start + 6:]
        record = list(eval(tuple_str))
        api.insert_values(table_name, record)

    elif command == 'select':
        assert sql_strs[1] == '*', 'minisql暂不支持非*查找'
        assert sql_strs[2] == 'from', 'SQL语句缺少from'
        table_name = sql_strs[3]
        print('Results')
        print('-------------------')
        if len(sql_strs) > 4:
            if sql_strs[4].lower() == 'where':
                start = sql.lower().find('where')
                conditions = sql[start + 5:]
                conditions = conditions.split('and')
                rec_block = api.select_records(table_name, conditions)
                for block_number, record_number in rec_block:
                    record = api.get_record_by_block(table_name, block_number, record_number)
                    if record[0] == 1:
                        for i in range(1, len(record)):
                            print(record[i], end='\t')
                        print()
        else:
            conditions = []
            rec_block = api.select_records(table_name, conditions)
            for block_number, record_number in rec_block:
                record = api.get_record_by_block(table_name, block_number, record_number)
                for i in range(1, len(record)):
                    print(record[i], end='\t')
                print()
        print('-------------------')

    elif command == 'exec':
        file_name = sql_strs[1].replace(' ', '')
        execute_commands(api, file_name + '.sql')

    elif command == 'show':
        if sql_strs[1] == 'tables':
            print('Tables')
            print('-------------------')
            for name in api.get_tables_names():
                print(name)
            print('-------------------')
        elif sql_strs[1] == 'index':
            print('Index')
            print('-------------------')
            index_list = api.get_index_table()
            for index in index_list:
                print("{}\t{:>10}\t{:>10}\t{:>10}".format(index[0], index[1], index[2], index[3]))

    elif command == 'desc':
        table_name = sql_strs[1]
        atr_list = api.get_atr_table(table_name)
        if atr_list is not None:
            print(table_name)
            print('-------------------')
            for item in atr_list:
                if item['type'] == -1:
                    type_str = 'float'
                elif item['type'] == 0:
                    type_str = 'int'
                else:
                    type_str = 'char({})'.format(item['type'])
                print("{}\t{:>10}".format(item['name'], type_str))
            print('-------------------')
        else:
            print('表不存在')


def main():
    api = API.API(4 * 1024, 4 * 1024)
    print_welcome()
    command = ''
    while command != 'quit':
        command = get_command()
        print(command)
        # print(api.exec_sql(command))
        try:
            parse_sql(api, command)
        except AssertionError as e:
            print(e)


if __name__ == '__main__':
    main()
