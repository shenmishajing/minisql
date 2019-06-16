import re
import record_manager
import index_manager


class API:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.__block_size = block_size
        self.record_manager = record_manager.RecordManager(block_size, memory_size, work_dir)

    def __find_last_str(self, s: str, string: str):
        result = -1
        while True:
            cur_pos = string.find(s, result + 1)
            if cur_pos == -1:
                return result
            else:
                result = cur_pos

    def __construct_table(self, table_name:str, artribute_table:list):
        table = {}
        content = {}
        index = {}
        art = []
        primary_key = 0
        type_size = 4
        for s in artribute_table:
            if 'primary key' in s:
                pri = s.replace(')', '').split('(')[-1].replace(' ', '')
                print(pri)
                for temp in art:
                    if pri == temp['name']:
                        temp['unique'] = 1
                        if temp['type'] != 0 and temp['type'] != -1:
                            type_size = temp['type']
                        break
                    primary_key += 1
            else:
                s_list = s.split() #type:list[str]
                print(s_list)
                artribute = {}
                name = s_list[0]
                type = -1
                unique = 0
                if 'int' == s_list[1]:
                    type = 0
                elif 'float' == s_list[1]:
                    type = -1
                elif 'char' in s_list[1]:
                    temp_list = s_list[1].replace(')', '').split('(')
                    #print(temp_list)
                    type = int(temp_list[1])
                if len(s_list) > 2:
                    if 'unique' in s_list[2]:
                        unique = 1
                artribute['name'] = name
                artribute['type'] = type
                artribute['unique'] = unique
                art.append(artribute)
        print(art)
        content['atr'] = art
        content['primary_key'] = primary_key
        m = self.__block_size // (type_size + 8) #key大小加上pointer的大小
        index[primary_key] = index_manager.IndexManager(m)
        content['index'] = index
        table[table_name] = content
        print(table)
        return table

    def __parse_conditions(self, conditions: list):
        result = {}
        for condition in conditions:
            condition = condition.split()
            art = condition[0]
            op = condition[1]
            data = condition[2]
            if art in result.keys():
                result[art].append((op, data))
            else:
                result[art] = [(op, data)]
        print(result)
        return result

    def exec_sql(self, sql):
        pass

    def parse_sql(self, sql:str):
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
                end = self.__find_last_str(')', sql)
                assert start != -1 and end != -1, '非法SQL，缺少括号'
                artribute_table = sql[start+1:end].split(',')
                assert len(artribute_table) > 0, '缺少属性'
                print(table_name)
                for c in artribute_table:
                    print(c)
                table = self.__construct_table(table_name, artribute_table)
                self.record_manager.create_table(table)
                #self.create_table()
            elif command_type == 'index':
                assert sql_strs[3] == 'on', 'index非法指令'
                index_name = sql_strs[2]
                table_name = sql_strs[4]
                artribute_name = sql_strs[5].replace("(", '').replace(")", '')
                #self.create_index()

        elif command == 'drop':
            command_type = sql_strs[1].lower()
            if command_type == 'index':
                index_name = sql_strs[2]
                #self.drop_index()
            elif command_type == 'table':
                table_name = sql_strs[2]
                #self.drop_table()
        elif command == 'delete':
            assert sql_strs[1].lower() == 'from', 'SQL非法指令'
            table_name = sql_strs[2]
            if len(sql_strs) > 3:
                if sql_strs[3].lower() == 'where':
                    start = sql.lower().find('where')
                    conditions = sql[start + 5:]
                    conditions = conditions.split('and')
                    print(conditions)
                    con = self.__parse_conditions(conditions)
            #self.delete()

        elif command == 'insert':
            assert sql_strs[1].lower() == 'into', 'SQL非法指令'
            table_name = sql_strs[2]
            start = sql.lower().find('values')
            assert start != -1, 'SQL缺少values关键字'
            tuple_str = sql[start+6:]
            record = eval(tuple_str)
            print(tuple_str)
            print(record)





api = API(512,1024)
s = '''
insert into student values ('xiaoming', 18)
'''

api.parse_sql(s)