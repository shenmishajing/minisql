import record_manager


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
        content['prime_key'] = primary_key
        #m = self.__block_size // (type_size + 8) #key大小加上pointer的大小
        #index[primary_key] = index_manager.IndexManager(self.__block_size, type_size)
        #index[primary_key] = 'primary_key_{}'.format(content['art'][primary_key]['name'])
        index_name = '{}_{}'.format(table_name, content['atr'][primary_key]['name'])
        index[primary_key] = index_name
        self.record_manager.catalog_manager.index_map = {index_name: (table_name, primary_key)}
        self.record_manager.index_manager.create_index(index_name, type_size)
        content['index'] = index
        table[table_name] = content
        print(table)
        return table

    def __parse_conditions(self, table_name, conditions: list):
        result = {}
        atributes = self.record_manager.catalog_manager.meta_data[table_name]['atr']
        #test = {'stu': {'atr': [{'name': 'name', 'type': 10, 'unique': 1}, {'name': 'age', 'type': 0, 'unique': 0}], 'prime_key': 0, 'index': {0: 12}}}
        #atributes = test[table_name]['atr']
        for condition in conditions:
            condition = condition.split()
            art = condition[0]
            op = condition[1]
            data = condition[2]
            for temp in atributes:
                if art == temp['name']:
                    if temp['type'] == -1:
                        data = float(data)
                    elif temp['type'] == 0:
                        data = int(data)
                    else:
                        data = data.replace("'", '')
                    break
            if art in result.keys():
                result[art].append((op, data))
            else:
                result[art] = [(op, data)]
        print(result)
        return result

    def create_index(self, table_name, index_name, artribute_name):
        #if artribute_name in self.record_manager.catalog_manager.meta_data[table_name]['index'].keys():
            #return
        art_index = 0
        type_size = 4
        for temp in self.record_manager.catalog_manager.meta_data[table_name]['atr']:
            if temp['name'] == artribute_name:
                if temp['type'] != 0 and temp['type'] != -1:
                    type_size = temp['type']
                break
            art_index += 1
        if art_index == len(self.record_manager.catalog_manager.meta_data[table_name]['atr']):
            print('属性不存在')
            return
        if self.record_manager.catalog_manager.meta_data[table_name]['atr'][art_index] != 1:
            print('非unique属性无法建立索引')
            return

        self.record_manager.index_manager.create_index(index_name, type_size)
        self.record_manager.catalog_manager.create_index(table_name, {art_index: index_name},
                                                         {index_name: (table_name, art_index)})
        #self.record_manager.catalog_manager.meta_data[table_name]['index'][index_name] = index
        for block_number in range(self.record_manager.catalog_manager.meta_data[table_name]['size']):  # 遍历所有的block
            block = self.record_manager.buffer_manager.get_block(table_name, block_number,
                                            self.record_manager.catalog_manager.meta_data[table_name]['record_size'],
                                            self.record_manager.catalog_manager.meta_data[table_name]['fmt'])
            for record_number, record in enumerate(block):
                if record[0]:
                    self.record_manager.index_manager.insert(index_name, record[art_index + 1],
                                                             (block_number, record_number))

    def drop_index(self, index_name):
        self.record_manager.index_manager.drop_index(index_name)
        self.record_manager.catalog_manager.drop_index(index_name)

    def create_table(self, table_name, artribute_table):
        table = self.__construct_table(table_name, artribute_table)
        self.record_manager.create_table(table)

    def drop_table(self, table_name):
        self.record_manager.drop_table(table_name)

    def delete_records(self, table_name, conditions):
        if len(conditions) == 0:
            records = self.record_manager.find_all_records(table_name)
            for rec in records:
                # self.record_manager.delete(table_name, rec[1])
                self.record_manager.delete_by_block(table_name, rec[0], rec[1])
        else:
            con = self.__parse_conditions(table_name, conditions)
            records = self.record_manager.find(table_name, con)
            for rec in records:
                # self.record_manager.delete(table_name, rec[1])
                self.record_manager.delete_by_block(table_name, rec[0], rec[1])

    def insert_values(self, table_name, record):
        self.record_manager.inseret(table_name, record)

    def select_records(self, table_name, conditions):
        if len(conditions) == 0:
            rec_block = self.record_manager.find_all_records(table_name)
        else:
            con = self.__parse_conditions(table_name, conditions)
            rec_block = self.record_manager.find(table_name, con)
        return rec_block

    def get_record_by_block(self, table_name, block_number, record_number):
        record = self.record_manager.get_record_by_block(table_name, block_number, record_number)
        return record

    def get_tables_names(self):
        return self.record_manager.catalog_manager.meta_data.keys()

    def get_atr_table(self, table_name):
        if table_name in self.record_manager.catalog_manager.meta_data.keys():
            return self.record_manager.catalog_manager.meta_data[table_name]['atr']
        else:
            return None

    def parse_sql(self, sql: str):
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
            elif command_type == 'index':
                assert sql_strs[3] == 'on', 'index非法指令'
                index_name = sql_strs[2]
                table_name = sql_strs[4]
                #artribute_name = sql_strs[5].replace("(", '').replace(")", '')
                start = sql.find('(')
                end = sql.find(')')
                artribute_name = sql[start+1:end]
                artribute_name = artribute_name.replace(' ', '')
                print(artribute_name)
                #self.create_index()
                self.create_index(table_name, index_name, artribute_name)
                #这里还存在问题

        elif command == 'drop':
            command_type = sql_strs[1].lower()
            if command_type == 'index':
                index_name = sql_strs[2]
                #self.drop_index()
                #table_name = sql_strs[4]
                self.drop_index(index_name)
            elif command_type == 'table':
                table_name = sql_strs[2]
                self.record_manager.drop_table(table_name)

        elif command == 'delete':
            assert sql_strs[1].lower() == 'from', 'SQL非法指令'
            table_name = sql_strs[2]
            if len(sql_strs) > 3:
                if sql_strs[3].lower() == 'where':
                    start = sql.lower().find('where')
                    conditions = sql[start + 5:]
                    conditions = conditions.split('and')
                    print(conditions)
                    con = self.__parse_conditions(table_name, conditions)
                    records = self.record_manager.find(table_name, con)
                    for rec in records:
                        #self.record_manager.delete(table_name, rec[1])
                        self.record_manager.delete_by_block(table_name, rec[0], rec[1])
            else:#若缺省条件
                records = self.record_manager.find_all_records(table_name)
                for rec in records:
                    #self.record_manager.delete(table_name, rec[1])
                    self.record_manager.delete_by_block(table_name, rec[0], rec[1])

        elif command == 'insert':
            assert sql_strs[1].lower() == 'into', 'SQL非法指令'
            table_name = sql_strs[2]
            start = sql.lower().find('values')
            assert start != -1, 'SQL缺少values关键字'
            tuple_str = sql[start+6:]
            record = eval(tuple_str)
            #print(tuple_str)
            print(record)
            self.record_manager.inseret(table_name, record)

        elif command == 'select':
            assert sql_strs[1] == '*', 'minisql暂不支持非*查找'
            assert sql_strs[2] == 'from', 'SQL语句缺少from'
            table_name = sql_strs[3]
            #print(sql_strs)
            if len(sql_strs) > 4:
                if sql_strs[4].lower() == 'where':
                    start = sql.lower().find('where')
                    conditions = sql[start + 5:]
                    conditions = conditions.split('and')
                    print(conditions)
                    con = self.__parse_conditions(table_name, conditions)
                    rec_block = self.record_manager.find(table_name, con)
                    for rec in rec_block:
                        record = self.record_manager.get_record_by_block(table_name, rec[0], rec[1])
                        print(record[1:])
            else:
                rec_block = self.record_manager.find_all_records(table_name)
                for rec in rec_block:
                    record = self.record_manager.get_record_by_block(table_name, rec[0], rec[1])
                    print(record[1:])








#api = API(512,1024)
#s = '''
#select * from std
#'''

#api.parse_sql(s)