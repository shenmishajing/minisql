import os
import catalog_manager
import buffer_manager


class RecordManager:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.block_size = block_size
        self.memory_size = memory_size
        self.work_dir = work_dir
        self.catalog_manager = catalog_manager.CatalogManager()
        self.buffer_manager = buffer_manager.BufferManager(block_size, memory_size, work_dir)

    def create_table(self, table_map):
        for table_name in table_map:
            table_file = open(self.work_dir + table_name, 'wb')
            table_file.close()
            self.catalog_manager.create_table(table_map)

    def drop_table(self, table_name):
        os.remove(self.work_dir + table_name)
        self.catalog_manager.drop_table(table_name)

    def inseret(self, table_name, record):
        record.insert(0, True)
        num_records = self.block_size // self.catalog_manager.meta_data[table_name]['record_size']
        if self.catalog_manager.meta_data[table_name]['invaild_list']:
            block_number, record_number = self.catalog_manager.meta_data[table_name]['invaild_list'][0]
            del self.catalog_manager.meta_data[table_name]['invaild_list'][0]
            block = self.buffer_manager.get_block(table_name, block_number,
                                                  self.catalog_manager.meta_data[table_name]['record_size'],
                                                  self.catalog_manager.meta_data[table_name]['fmt'])
        else:
            block_number = self.catalog_manager.meta_data[table_name]['size']
            self.catalog_manager.meta_data[table_name]['size'] += 1
            block = self.buffer_manager.create_block(table_name, block_number,
                                                     self.catalog_manager.meta_data[table_name]['record_size'],
                                                     self.catalog_manager.meta_data[table_name]['fmt'])
            record_number = 0
            for i in range(1, num_records):
                self.catalog_manager.meta_data[table_name]['invaild_list'].append((block_number, i))

        block['change'] = True
        block['pin'] = True
        block['block'][record_number] = record
        block['pin'] = False

        for atr in self.catalog_manager.meta_data[table_name]['index']:
            self.catalog_manager.meta_data[table_name]['index'][atr].insert(record[atr + 1],
                                                                            (block_number, record_number))

    def delete(self, table_name, record_number):
        block_number, record_number, num_records = self.find_block_number(record_number,
                                                                          self.catalog_manager.meta_data[table_name][
                                                                              'record_size'])
        block = self.buffer_manager.get_block(table_name, block_number,
                                              self.catalog_manager.meta_data[table_name]['record_size'],
                                              self.catalog_manager.meta_data[table_name]['fmt'])
        block['change'] = True
        block['pin'] = True
        block['block'][record_number][0] = False
        block['pin'] = False

        self.catalog_manager.meta_data[table_name]['invaild_list'].append((block_number, record_number))

        for atr in self.catalog_manager.meta_data[table_name]['index']:
            self.catalog_manager.meta_data[table_name]['index'][atr].delete(block['block'][record_number][atr + 1])
