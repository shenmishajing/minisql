import struct
from ctypes import create_string_buffer


class BufferManager:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.work_dir = work_dir
        self.block_size = block_size
        self.memory_size = memory_size
        self.current_block_number = 0
        self.buffer = {}

    def __find_block_number(self, record_number, record_size):
        num_records = self.block_size // record_size
        block_number = record_number // num_records
        record_number %= num_records
        return block_number, record_number, num_records

    def get_record(self, table_name, record_number, record_size, fmt):
        block_number, record_number, num_records = self.__find_block_number(record_number, record_size)
        if table_name not in self.buffer:
            self.buffer[table_name] = {}
        if block_number not in self.buffer[table_name]:
            self.buffer[table_name][block_number] = {}
            self.buffer[table_name][block_number]['change'] = False
            self.buffer[table_name][block_number]['pin'] = False
            self.buffer[table_name][block_number]['block'] = []
            if self.current_block_number + num_records > self.memory_size:
                # todo swap strategy
                pass
            table_file = open(self.work_dir + table_name, 'rb')
            table_file.seek(block_number * self.block_size)
            block = table_file.read(self.block_size)
            for i in range(num_records):
                record = list(struct.unpack(fmt, block[i * record_size:(i + 1) * record_size]))
                for j in range(len(record)):
                    if type(record[j]) == bytes:
                        record[j] = record[j].decode()
                self.buffer[table_name][block_number]['block'].append(record)
        assert self.buffer[table_name][block_number]['block'][record_number][0], '访问了被删除的记录'
        return self.buffer[table_name][block_number]['block'][record_number]

    def write_block(self, table_name, block_number, record_size, fmt):
        assert table_name in self.buffer, '要写入的块不在内存中'
        assert block_number in self.buffer[table_name], '要写入的块不在内存中'
        assert self.buffer[table_name][block_number]['pin'], '要写入的块被 pin 了'
        block = create_string_buffer(self.block_size)
        for i, record in enumerate(self.buffer[table_name][block_number]):
            struct.pack_into(fmt, block, i * record_size, *record)
        table_file = open(self.work_dir + table_name, 'wb')
        table_file.seek(block_number * self.block_size)
        table_file.write(bytes(block))
        self.buffer[table_name][block_number]['change'] = False

    def change_block(self, table_name, block_number):
        assert table_name in self.buffer, '要写入的块不在内存中'
        assert block_number in self.buffer[table_name], '要写入的块不在内存中'
        self.buffer[table_name][block_number]['change'] = True
