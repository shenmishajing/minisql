import bplustree


class IndexManager:

    def __init__(self, block_size):
        self.__block_size = block_size
        self.__bplustrees = {}

    def __check_available(self, index_name):
        if index_name in self.__bplustrees.keys():
            return True
        else:
            return False

    def create_index(self, index_name, type_size):
        if self.__check_available(index_name):
            return False
        size = max(self.__block_size // (type_size + 8), 3)
        self.__bplustrees[index_name] = bplustree.BPlusTree(size)
        return True

    def insert(self, index_name, key, pointer):
        if not self.__check_available(index_name):
            return
        self.__bplustrees[index_name].insert(key, pointer)

    def delete(self, index_name, key):
        if not self.__check_available(index_name):
            return
        self.__bplustrees[index_name].delete(key)

    def find(self, index_name, key):
        assert index_name in self.__bplustrees.keys(), '不存在索引'
        return self.__bplustrees[index_name].find(key)

    def get_head(self, index_name):
        assert index_name in self.__bplustrees.keys()
        return self.__bplustrees[index_name].get_head()

    def drop_index(self, index_name):
        if not self.__check_available(index_name):
            return
        self.__bplustrees.pop(index_name)


test = IndexManager(512)
test.create_index('stu_name', 4)
test.insert('stu_name', 'xiao', (0, 0))
print(test.find('stu_name', 'xiao'))