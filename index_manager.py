import bplustree


class IndexManager:

    def __init__(self, size = 3):
        self.__bplustree = bplustree.BPlusTree(size)

    def insert(self, key, pointer):
        self.__bplustree.insert(key, pointer)

    def delete(self, key):
        self.__bplustree.delete(key)

    def find(self, key):
        return self.__bplustree.find(key)