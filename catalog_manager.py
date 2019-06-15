class CatalogManager:

    def __init__(self, database_name):
        self.database_name = database_name
        self.meta_data = {}

    def create_table(self, table_map):
        for table_name in table_map:
            assert 'atr' in table_map[table_name], table_name + '表格没有属性或者 table_map 中 atr 属性未被设置'
            assert 'prime_key' in table_map[table_name], table_name + '表格没有主键或者 table_map 中 prime_key 属性未被设置'
            assert 'index' in table_map[table_name], table_name + '表格没有索引或者 table_map 中 index 属性未被设置'
            assert table_map[table_name]['prime_key'] in table_map[table_name]['index'], table_name + '表格的主键没有建立索引'
            fmt = 'c'
            record_size = 1
            for i, atr in enumerate(table_map[table_name]['atr']):
                assert 'name' in atr, table_name + '表格第' + str(i) + '个属性没有设置名字或者 name 属性未被设置'
                assert 'type' in atr, table_name + '表格' + atr['name'] + '属性没有类型或者 type 属性未被设置'
                if 'unique' not in atr:
                    atr['unique'] = False
                type = atr['type']
                if type == -1:
                    fmt += 'f'
                    record_size += 4
                elif type == 0:
                    fmt += 'i'
                    record_size += 4
                else:
                    fmt += str(type) + 's'
                    record_size += type
            table_map[table_name]['fmt'] = fmt
            table_map[table_name]['record_size'] = record_size

        self.meta_data.update(table_map)

    def drop_table(self, table_name):
        del self.meta_data[table_name]

    def create_index(self, table_name, index_map):
        self.meta_data[table_name]['index'].update(index_map)

    def drop_index(self, table_name, index_name):
        del self.meta_data[table_name]['index'][index_name]
