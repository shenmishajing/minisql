import record_manager


class API:
    def __init__(self, block_size, memory_size, work_dir = '.'):
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

    def __construct_table(self, table_name: str, artribute_table: list):
        table = {}
        content = {}
        index = {}
        art = []
        primary_key = 0
        type_size = 4
        for s in artribute_table:
            if 'primary key' in s:
                pri = s.replace(')', '').split('(')[-1].replace(' ', '')
                # print(pri)
                for temp in art:
                    if pri == temp['name']:
                        temp['unique'] = 1
                        if temp['type'] != 0 and temp['type'] != -1:
                            type_size = temp['type']
                        break
                    primary_key += 1
            else:
                s_list = s.split()  # type:list[str]
                # print(s_list)
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
                    # print(temp_list)
                    type = int(temp_list[1])
                if len(s_list) > 2:
                    if 'unique' in s_list[2]:
                        unique = 1
                artribute['name'] = name
                artribute['type'] = type
                artribute['unique'] = unique
                art.append(artribute)
        # print(art)
        content['atr'] = art
        content['prime_key'] = primary_key
        # m = self.__block_size // (type_size + 8) #key大小加上pointer的大小
        # index[primary_key] = index_manager.IndexManager(self.__block_size, type_size)
        # index[primary_key] = 'primary_key_{}'.format(content['art'][primary_key]['name'])
        index_name = '{}_{}'.format(table_name, content['atr'][primary_key]['name'])
        index[primary_key] = index_name
        self.record_manager.catalog_manager.index_map.update({index_name: (table_name, primary_key)})
        self.record_manager.index_manager.create_index(index_name, type_size)
        content['index'] = index
        table[table_name] = content
        # print(table)
        return table

    def __parse_conditions(self, table_name, conditions: list):
        result = {}
        assert table_name in self.record_manager.catalog_manager.meta_data, '表格不存在'
        atributes = self.record_manager.catalog_manager.meta_data[table_name]['atr']
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
        # print(result)
        return result

    def create_index(self, table_name, index_name, artribute_name):
        # if artribute_name in self.record_manager.catalog_manager.meta_data[table_name]['index'].keys():
        # return
        art_index = 0
        type_size = 4
        assert table_name in self.record_manager.catalog_manager.meta_data, '表格不存在'
        for temp in self.record_manager.catalog_manager.meta_data[table_name]['atr']:
            if temp['name'] == artribute_name:
                if temp['type'] != 0 and temp['type'] != -1:
                    type_size = temp['type']
                break
            art_index += 1
        if art_index == len(self.record_manager.catalog_manager.meta_data[table_name]['atr']):
            print('属性不存在')
            return
        if self.record_manager.catalog_manager.meta_data[table_name]['atr'][art_index]['unique'] != 1:
            print('非unique属性无法建立索引')
            return

        if art_index in self.record_manager.catalog_manager.meta_data[table_name]['index'].keys():
            print('该属性已存在index')
            return

        callback = self.record_manager.index_manager.create_index(index_name, type_size)
        if callback:
            self.record_manager.catalog_manager.create_index(table_name, {art_index: index_name},
                                                             {index_name: (table_name, art_index)})
            # self.record_manager.catalog_manager.meta_data[table_name]['index'][index_name] = index
            for block_number in range(self.record_manager.catalog_manager.meta_data[table_name]['size']):  # 遍历所有的block
                block = self.record_manager.buffer_manager.get_block(table_name, block_number,
                                                                     self.record_manager.catalog_manager.meta_data[
                                                                         table_name]['record_size'],
                                                                     self.record_manager.catalog_manager.meta_data[
                                                                         table_name]['fmt'])
                for record_number, record in enumerate(block['block']):
                    if record[0]:
                        self.record_manager.index_manager.insert(index_name, record[art_index + 1],
                                                                 (block_number, record_number))

    def drop_index(self, index_name):
        if index_name in self.record_manager.catalog_manager.index_map.keys():
            table_name = self.record_manager.catalog_manager.index_map[index_name][0]
            atr_index = self.record_manager.catalog_manager.index_map[index_name][1]
            assert atr_index != self.record_manager.catalog_manager.meta_data[table_name][
                'prime_key'], '无法对primary key的index进行删除'
        self.record_manager.index_manager.drop_index(index_name)
        self.record_manager.catalog_manager.drop_index(index_name)

    def create_table(self, table_name, artribute_table):
        assert table_name not in self.record_manager.catalog_manager.meta_data.keys(), 'table已存在'
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

    def get_index_table(self):
        rec = []
        for index_name in self.record_manager.catalog_manager.index_map.keys():
            table_name = self.record_manager.catalog_manager.index_map[index_name][0]
            atr_index = self.record_manager.catalog_manager.index_map[index_name][1]
            atr_table = self.record_manager.catalog_manager.meta_data[table_name]['atr']
            atr_name = atr_table[atr_index]['name']
            atr_type = atr_table[atr_index]['type']
            if atr_type == -1:
                type_str = 'float'
            elif atr_type == 0:
                type_str = 'int'
            else:
                type_str = 'char({})'.format(atr_type)
            rec.append((index_name, table_name, atr_name, type_str))
        return rec
