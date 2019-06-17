class CatalogManager:

    def __init__(self, work_dir='.'):
        self.work_dir = work_dir
        self.meta_data = {}
        self.index_map = {}

    def save(self):
        catalog_file = open(self.work_dir + '/catalog.txt', 'w')
        catalog_file.write(str(self.meta_data) + '\n')
        catalog_file.write(str(self.index_map) + '\n')

    def open(self):
        catalog_file = open(self.work_dir + '/catalog.txt', 'r')
        self.meta_data = eval(catalog_file.readline())
        self.index_map = eval(catalog_file.readline())

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
            table_map[table_name]['invaild_list'] = []
            table_map[table_name]['size'] = 0

        self.meta_data.update(table_map)

    def drop_table(self, table_name):
        del self.meta_data[table_name]

    def create_index(self, table_name, index_map, index_name):
        self.meta_data[table_name]['index'].update(index_map)
        self.index_map.update(index_name)

    def drop_index(self, index_name):
        table_name, atr_index = self.index_map[index_name]
        del self.index_map[index_name]
        del self.meta_data[table_name]['index'][atr_index]
