import API


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
    print('minisql> ', end='')
    string = input()
    while True:
        if ';' in string:
            command += string[:string.find(';')]
            break
        command += string
        print('      -> ', end='')
        string = input()
    return command

def parse_sql(api, sql):
    sql = sql.replace('\n', ' ').replace('\t', '')
    print(sql)
    sql_strs = sql.split()
    print(sql_strs)
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
            print(table_name)
            for c in artribute_table:
                print(c)
            api.create_table(table_name, artribute_table)

        elif command_type == 'index':
            assert sql_strs[3] == 'on', 'index非法指令'
            index_name = sql_strs[2]
            table_name = sql_strs[4]
            # artribute_name = sql_strs[5].replace("(", '').replace(")", '')
            start = sql.find('(')
            end = sql.find(')')
            artribute_name = sql[start + 1:end]
            artribute_name = artribute_name.replace(' ', '')
            print(artribute_name)
            # self.create_index()
            api.create_index(table_name, index_name, artribute_name)

    elif command == 'drop':
        command_type = sql_strs[1].lower()
        if command_type == 'index':
            index_name = sql_strs[2]
            # self.drop_index()
            # table_name = sql_strs[4]
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
                print(conditions)
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
        record = eval(tuple_str)
        # print(tuple_str)
        print(record)
        api.insert_values(table_name, record)

    elif command == 'select':
        assert sql_strs[1] == '*', 'minisql暂不支持非*查找'
        assert sql_strs[2] == 'from', 'SQL语句缺少from'
        table_name = sql_strs[3]
        # print(sql_strs)
        if len(sql_strs) > 4:
            if sql_strs[4].lower() == 'where':
                start = sql.lower().find('where')
                conditions = sql[start + 5:]
                conditions = conditions.split('and')
                print(conditions)
                rec_block = api.select_records(table_name, conditions)
                print(len(rec_block))
                for block_number, record_number in rec_block:
                    print(api.get_single_record(table_name, block_number, record_number))
        else:
            conditions = []
            rec_block = api.select_records(table_name, conditions)
            print(len(rec_block))
            for block_number, record_number in rec_block:
                print(api.get_single_record(table_name, block_number, record_number))



def main():
    api = API.API(4 * 1024, 4 * 1024)
    print_welcome()
    command = ''
    while command != 'quit':
        command = get_command()
        print(command)
        #print(api.exec_sql(command))
        parse_sql(api, command)


if __name__ == '__main__':
    main()
