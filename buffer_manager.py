import struct
from ctypes import create_string_buffer


class BufferManager:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.work_dir = work_dir
        self.block_size = block_size
        self.memory_size = memory_size
        self.current_block_number = 0
        self.current_block_used_time = 0
        self.buffer = {}

    def find_block_number(self, record_number, record_size):
        num_records = self.block_size // record_size
        block_number = record_number // num_records
        record_number %= num_records
        return block_number, record_number, num_records

    def write_block(self, table_name, block_number, fmt):
        assert table_name in self.buffer, '要写入的块不在内存中'
        assert block_number in self.buffer[table_name], '要写入的块不在内存中'
        assert not self.buffer[table_name][block_number]['pin'], '要写入的块被 pin 了'
        block = b''
        for i, record in enumerate(self.buffer[table_name][block_number]['block']):
            if record:
                for item, f in zip(record, fmt):
                    if type(item) == str:
                        block += struct.pack(f, item.encode())
                    else:
                        block += struct.pack(f, item)
        if len(block) < self.block_size:
            block += b'\x00' * (self.block_size - len(block))
        table_file = open(self.work_dir + '/' + table_name, 'wb+')
        table_file.seek(block_number * self.block_size)
        table_file.write(bytes(block))
        self.buffer[table_name][block_number]['change'] = False
        table_file.close()

    def swap_block(self, fmt):
        last_min_time = self.current_block_used_time
        min_time = self.current_block_used_time
        min_table_name = None
        min_block_number = None

        for table_name in self.buffer:
            for block_number in self.buffer[table_name]:
                if not self.buffer[table_name][block_number]['pin'] and self.buffer[table_name][block_number][
                    'time'] < min_time:
                    min_table_name = table_name
                    min_block_number = block_number
                    last_min_time = min_time
                    min_time = self.buffer[table_name][block_number]['time']

        if last_min_time > 10 ** 8:
            for table_name in self.buffer:
                for block_number in self.buffer[table_name]:
                    self.buffer[table_name][block_number]['time'] -= last_min_time
            self.current_block_used_time -= last_min_time

        self.write_block(min_table_name, min_block_number, fmt)

    def get_block(self, table_name, block_number, record_size, fmt):
        num_records = self.block_size // record_size
        if table_name not in self.buffer:
            self.buffer[table_name] = {}
        if block_number not in self.buffer[table_name]:
            if self.current_block_number + 1 > self.memory_size:
                self.swap_block(fmt)
            else:
                self.current_block_number += 1

            self.buffer[table_name][block_number] = {}
            self.buffer[table_name][block_number]['change'] = False
            self.buffer[table_name][block_number]['pin'] = False
            self.buffer[table_name][block_number]['time'] = self.current_block_used_time
            self.buffer[table_name][block_number]['block'] = []

            table_file = open(self.work_dir + '/' + table_name, 'rb')
            table_file.seek(block_number * self.block_size)
            block = table_file.read(self.block_size)

            current_pointer = 0
            for i in range(num_records):
                record = []
                for f in fmt:
                    if f == 'i':
                        item, = struct.unpack(f, block[current_pointer:current_pointer + 4])
                        current_pointer += 4
                    elif f == 'f':
                        item, = struct.unpack(f, block[current_pointer:current_pointer + 4])
                        current_pointer += 4
                    else:
                        item, = struct.unpack(f, block[current_pointer:current_pointer + int(f[:-1])])
                        item = item.decode()[:item.find(b'\x00')]
                        current_pointer += int(f[:-1])
                    record.append(item)
                self.buffer[table_name][block_number]['block'].append(record)

        return self.buffer[table_name][block_number]

    def get_record_by_block(self, table_name, block_number, record_number, record_size, fmt):
        block = self.get_block(table_name, block_number, record_size, fmt)
        return block['block'][record_number]

    def get_record(self, table_name, record_number, record_size, fmt):
        block_number, record_number, _ = self.find_block_number(record_number, record_size)
        return self.get_record_by_block(table_name, block_number, record_number, record_size, fmt)

    def create_block(self, table_name, block_number, record_size, fmt):
        if self.current_block_number + 1 > self.memory_size:
            self.swap_block(record_size, fmt)
        else:
            self.current_block_number += 1
        if table_name not in self.buffer:
            self.buffer[table_name] = {}
        self.buffer[table_name][block_number] = {}
        self.buffer[table_name][block_number]['change'] = False
        self.buffer[table_name][block_number]['pin'] = False
        self.buffer[table_name][block_number]['time'] = self.current_block_used_time
        self.buffer[table_name][block_number]['block'] = []
        num_records = self.block_size // record_size
        for i in range(num_records):
            self.buffer[table_name][block_number]['block'].append([])
        return self.buffer[table_name][block_number]

    def change_block(self, table_name, block_number):
        assert table_name in self.buffer, '要写入的块不在内存中'
        assert block_number in self.buffer[table_name], '要写入的块不在内存中'
        self.buffer[table_name][block_number]['change'] = True
