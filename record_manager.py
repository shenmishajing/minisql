import os
import pickle
import catalog_manager
import buffer_manager
import index_manager


class RecordManager:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.block_size = block_size
        self.memory_size = memory_size
        self.work_dir = work_dir
        self.catalog_manager = catalog_manager.CatalogManager(work_dir)
        # self.index_manager = index_manager.IndexManager(block_size)
        if os.path.exists('./IndexManager.obj'):
            with open('./IndexManager.obj', 'rb') as f:
                self.index_manager = pickle.load(f)
        else:
            self.index_manager = index_manager.IndexManager(block_size)
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
        record = (True,) + record
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
            # self.catalog_manager.meta_data[table_name]['index'][atr].insert(record[atr + 1],
            #                                                                 (block_number, record_number))
            index_name = self.catalog_manager.meta_data[table_name]['index'][atr]
            self.index_manager.insert(index_name, record[atr + 1], (block_number, record_number))

    def delete(self, table_name, record_number):
        block_number, record_number, num_records = self.buffer_manager.find_block_number(record_number,
                                                                                         self.catalog_manager.meta_data[
                                                                                             table_name]['record_size'])
        # block = self.buffer_manager.get_block(table_name, block_number,
        #                                       self.catalog_manager.meta_data[table_name]['record_size'],
        #                                       self.catalog_manager.meta_data[table_name]['fmt'])
        # block['change'] = True
        # block['pin'] = True
        # block['block'][record_number][0] = False
        # block['pin'] = False
        #
        # self.catalog_manager.meta_data[table_name]['invaild_list'].append((block_number, record_number))
        #
        # for atr in self.catalog_manager.meta_data[table_name]['index']:
        #     self.catalog_manager.meta_data[table_name]['index'][atr].delete(block['block'][record_number][atr + 1])
        self.delete_with_blocknum(table_name, block_number, record_number)

    def delete_with_blocknum(self, table_name, block_number, record_number):
        block = self.buffer_manager.get_block(table_name, block_number,
                                              self.catalog_manager.meta_data[table_name]['record_size'],
                                              self.catalog_manager.meta_data[table_name]['fmt'])
        block['change'] = True
        block['pin'] = True
        block['block'][record_number][0] = False
        block['pin'] = False

        self.catalog_manager.meta_data[table_name]['invaild_list'].append((block_number, record_number))

        for atr in self.catalog_manager.meta_data[table_name]['index']:
            # self.catalog_manager.meta_data[table_name]['index'][atr].delete(block['block'][record_number][atr + 1])
            index_name = self.catalog_manager.meta_data[table_name]['index'][atr]
            self.index_manager.delete(index_name, block['block'][record_number][atr + 1])

    def calculate_search_range_percentage(self, atr, search_range):
        if atr['type'] == 0:
            if search_range[1] is None and search_range[2] is None:
                search_range_percentage = 1
            elif search_range[1] is not None and search_range[
                2] is None:
                search_range_percentage = (2 ** 31 - search_range[1]) / (2 ** 32)
            elif search_range[1] is None and search_range[
                2] is not None:
                search_range_percentage = 2
            elif search_range[1] is not None and search_range[
                2] is not None:
                search_range_percentage = (search_range[2] - search_range[
                    1]) / (2 ** 32)
        elif atr['type'] == -1:
            if search_range[1] is None and search_range[2] is None:
                search_range_percentage = 1
            elif search_range[1] is not None and search_range[
                2] is None:
                search_range_percentage = (3.4E+38 - search_range[1]) / (6.8E+38)
            elif search_range[1] is None and search_range[
                2] is not None:
                search_range_percentage = 2
            elif search_range[1] is not None and search_range[
                2] is not None:
                search_range_percentage = (search_range[2] - search_range[
                    1]) / (6.8E+38)
        else:
            search_range_percentage = 1

        return search_range_percentage

    def calculate_consistent(self, record, search_range):
        for atr_index in search_range:
            if not ((search_range[atr_index]['range'][1] < record[atr_index + 1] or (
                    search_range[atr_index]['range'][0] and search_range[atr_index]['range'][1] == record[
                atr_index + 1])) and (search_range[atr_index]['range'][2] > record[atr_index + 1] or (
                    search_range[atr_index]['range'][3] and search_range[atr_index]['range'][2] == record[
                atr_index + 1])) and (record[atr_index + 1] not in search_range[atr_index]['unequal'])):
                return False

        return True

    def calculate_search_key(self, table_name, find_commands):
        best_search_key = None
        best_search_range_percentage = 2
        search_range = {}
        for key in find_commands:
            atr_index = None
            for i, atr in enumerate(self.catalog_manager.meta_data[table_name]['atr']):
                if atr['name'] == key:
                    atr_index = i
                    break
            assert atr_index, '搜索的 key 不存在'
            if atr_index not in search_range:
                search_range[atr_index] = {}
                search_range[atr_index]['range'] = [1, None, None, 1]
                search_range[atr_index]['unequal'] = []
            for command in find_commands[key]:
                if command[0] == '=':
                    assert search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[
                        1] or (search_range[atr_index]['range'][0] and search_range[atr_index]['range'][1] == command[
                        1]), '搜素范围冲突'
                    assert search_range[atr_index]['range'][2] is None or search_range[atr_index]['range'][2] > command[
                        1] or (search_range[atr_index]['range'][3] and search_range[atr_index]['range'][2] == command[
                        1]), '搜素范围冲突'
                    assert command[1] not in search_range[atr_index]['unequal'], '搜素范围冲突'
                    search_range[atr_index]['range'][0] = 1
                    search_range[atr_index]['range'][1] = command[1]
                    search_range[atr_index]['range'][2] = command[1]
                    search_range[atr_index]['range'][3] = 1
                    search_range[atr_index]['unequal'] = []
                    if atr_index in self.catalog_manager.meta_data[table_name]['index']:
                        if best_search_range_percentage:
                            best_search_key = atr_index
                            best_search_range_percentage = 0
                elif command[0] == '<>':
                    if (search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[
                        1] or (search_range[atr_index]['range'][0] and search_range[atr_index]['range'][1] == command[
                        1])) and (search_range[atr_index]['range'][2] is None or search_range[atr_index]['range'][2] >
                                  command[1] or (
                                          search_range[atr_index]['range'][3] and search_range[atr_index]['range'][2] ==
                                          command[1])) and (command[1] not in search_range[atr_index]['unequal']):
                        search_range[atr_index]['unequal'].append(command[1])
                elif command[0] == '<':
                    assert search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[
                        1], '搜素范围冲突'
                    if search_range[atr_index]['range'][2] is None or (
                            search_range[atr_index]['range'][2] > command[1] or (
                            search_range[atr_index]['range'][3] and search_range[atr_index]['range'][2] == command[1])):
                        search_range[atr_index]['range'][2] = command[1]
                        search_range[atr_index]['range'][3] = 0
                        for i in range(len(search_range[atr_index]['unequal']) - 1, -1, -1):
                            if not ((search_range[atr_index]['unequal'][i] > search_range[atr_index]['range'][1] or (
                                    search_range[atr_index]['range'][0] and search_range[atr_index]['unequal'][i] ==
                                    search_range[atr_index]['range'][1])) and (
                                            search_range[atr_index]['unequal'][i] < search_range[atr_index]['range'][
                                        2] or (search_range[atr_index]['range'][3] and
                                               search_range[atr_index]['unequal'][i] ==
                                               search_range[atr_index]['range'][1]))):
                                del search_range[atr_index]['unequal'][i]
                        if atr_index in self.catalog_manager.meta_data[table_name]['index']:
                            search_range_percentage = self.calculate_search_range_percentage(
                                self.catalog_manager.meta_data[table_name]['atr'][atr_index],
                                search_range[atr_index]['range'])
                            if search_range_percentage < best_search_range_percentage:
                                best_search_key = atr_index
                                best_search_range_percentage = search_range_percentage
                elif command[0] == '>':
                    assert search_range[atr_index]['range'][2] is None or search_range[atr_index]['range'][2] > command[
                        1], '搜素范围冲突'
                    if search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[
                        1] or (
                            search_range[atr_index]['range'][3] and search_range[atr_index]['range'][2] == command[1]):
                        search_range[atr_index]['range'][1] = command[1]
                        search_range[atr_index]['range'][0] = 0
                        for i in range(len(search_range[atr_index]['unequal']) - 1, -1, -1):
                            if not ((search_range[atr_index]['unequal'][i] > search_range[atr_index]['range'][1] or (
                                    search_range[atr_index]['range'][0] and search_range[atr_index]['unequal'][i] ==
                                    search_range[atr_index]['range'][1])) and (
                                            search_range[atr_index]['unequal'][i] < search_range[atr_index]['range'][
                                        2] or (search_range[atr_index]['range'][3] and
                                               search_range[atr_index]['unequal'][i] ==
                                               search_range[atr_index]['range'][1]))):
                                del search_range[atr_index]['unequal'][i]
                        if atr_index in self.catalog_manager.meta_data[table_name]['index']:
                            search_range_percentage = self.calculate_search_range_percentage(
                                self.catalog_manager.meta_data[table_name]['atr'][atr_index],
                                search_range[atr_index]['range'])
                            if search_range_percentage < best_search_range_percentage:
                                best_search_key = atr_index
                                best_search_range_percentage = search_range_percentage
                elif command[0] == '<=':
                    assert search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[
                        1] or (
                                   search_range[atr_index]['range'][0] and search_range[atr_index]['range'][1] ==
                                   command[1]), '搜素范围冲突'
                    if search_range[atr_index]['range'][2] is None or search_range[atr_index]['range'][2] > command[1]:
                        search_range[atr_index]['range'][2] = command[1]
                        search_range[atr_index]['range'][3] = 1
                        for i in range(len(search_range[atr_index]['unequal']) - 1, -1, -1):
                            if not ((search_range[atr_index]['unequal'][i] > search_range[atr_index]['range'][1] or (
                                    search_range[atr_index]['range'][0] and search_range[atr_index]['unequal'][i] ==
                                    search_range[atr_index]['range'][1])) and (
                                            search_range[atr_index]['unequal'][i] < search_range[atr_index]['range'][
                                        2] or (search_range[atr_index]['range'][3] and
                                               search_range[atr_index]['unequal'][i] ==
                                               search_range[atr_index]['range'][1]))):
                                del search_range[atr_index]['unequal'][i]
                        if atr_index in self.catalog_manager.meta_data[table_name]['index']:
                            search_range_percentage = self.calculate_search_range_percentage(
                                self.catalog_manager.meta_data[table_name]['atr'][atr_index],
                                search_range[atr_index]['range'])
                            if search_range_percentage < best_search_range_percentage:
                                best_search_key = atr_index
                                best_search_range_percentage = search_range_percentage
                elif command[0] == '>=':
                    assert search_range[atr_index]['range'][2] > command[1], '搜素范围冲突'
                    if search_range[atr_index]['range'][1] is None or search_range[atr_index]['range'][1] < command[1]:
                        search_range[atr_index]['range'][1] = command[1]
                        search_range[atr_index]['range'][0] = 1
                        for i in range(len(search_range[atr_index]['unequal']) - 1, -1, -1):
                            if not ((search_range[atr_index]['unequal'][i] > search_range[atr_index]['range'][1] or (
                                    search_range[atr_index]['range'][0] and search_range[atr_index]['unequal'][i] ==
                                    search_range[atr_index]['range'][1])) and (
                                            search_range[atr_index]['unequal'][i] < search_range[atr_index]['range'][
                                        2] or (search_range[atr_index]['range'][3] and
                                               search_range[atr_index]['unequal'][i] ==
                                               search_range[atr_index]['range'][1]))):
                                del search_range[atr_index]['unequal'][i]
                        if atr_index in self.catalog_manager.meta_data[table_name]['index']:
                            search_range_percentage = self.calculate_search_range_percentage(
                                self.catalog_manager.meta_data[table_name]['atr'][atr_index],
                                search_range[atr_index]['range'])
                            if search_range_percentage < best_search_range_percentage:
                                best_search_key = atr_index
                                best_search_range_percentage = search_range_percentage

        return best_search_key, search_range

    def find(self, table_name, find_commands):
        res = []

        best_search_key, search_range = self.calculate_search_key(table_name, find_commands)

        if best_search_key:
            # index_manager = self.catalog_manager.meta_data[table_name]['index'][best_search_key]
            # _, node, pointer_index = index_manager.find()
            index_name = self.catalog_manager.meta_data[table_name]['index'][best_search_key]
            _, node, pointer_index = self.index_manager.find(index_name, search_range[1])
            while node:
                for i in range(pointer_index, len(node.pointers)):
                    block_number, record_number = node.pointers[i]
                    record = self.buffer_manager.get_record_by_block(table_name, block_number,
                                                                     self.catalog_manager.meta_data[table_name][
                                                                         'record_size'],
                                                                     self.catalog_manager.meta_data[table_name]['fmt'])
                    if record[0] and self.calculate_consistent(record, search_range):
                        res.append([block_number, record_number])
                    elif record[0] and not (record[best_search_key + 1] < search_range[best_search_key]['range'][2] or (
                            search_range[best_search_key]['range'][3] and record[best_search_key + 1] ==
                            search_range[best_search_key]['range'][2])):
                        break
                node = node.next
                pointer_index = 0
        else:
            res = self.find(table_name)
            # for block_number in range(self.catalog_manager.meta_data[table_name]['size']):#遍历所有的block
            #     block = self.buffer_manager.get_block(table_name, block_number,
            #                                           self.catalog_manager.meta_data[table_name]['record_size'],
            #                                           self.catalog_manager.meta_data[table_name]['fmt'])
            #     for record_number, record in enumerate(block):
            #         if record[0] and self.calculate_consistent(record, search_range):
            #             res.append([block_number, record_number])

        return res

    def find_all_records(self, table_name):
        res = []
        for block_number in range(self.catalog_manager.meta_data[table_name]['size']):  # 遍历所有的block
            block = self.buffer_manager.get_block(table_name, block_number,
                                                  self.catalog_manager.meta_data[table_name]['record_size'],
                                                  self.catalog_manager.meta_data[table_name]['fmt'])
            for record_number, record in enumerate(block):
                if record[0]:
                    res.append([block_number, record_number])

        return res

    def get_single_record_with_blocknum(self, table_name, block_number, record_number):
        record = self.buffer_manager.get_single_record_with_blocknum(table_name, block_number, record_number,
                                                                     self.catalog_manager.meta_data[table_name][
                                                                         'record_size'],
                                                                     self.catalog_manager.meta_data[table_name]['fmt'])
        return record

    def __del__(self):
        with open('./IndexManager.obj', 'wb') as f:
            pickle.dump(self.index_manager, f)
