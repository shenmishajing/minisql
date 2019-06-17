import random

#size = 5  # 为节点中存储的记录数


class TreeNode:

    def __init__(self, size):
        self.__size = size
        self.keys = []
        self.next = None
        self.parent = None
        self.pointers = []

    def is_full(self):
        return len(self.keys) == self.__size

    def is_empty(self):
        return len(self.keys) == 0

    def is_leaf(self):
        return len(self.pointers) == len(self.keys)

    def insert_value(self, key, pointer):
        index = 0
        for k in self.keys:
            assert key != k, '存在相同的key，无法插入'
            if key < k:
                break
            index += 1
        self.keys.insert(index, key)
        self.pointers.insert(index, pointer)

    def length(self):
        return len(self.keys)


class BPlusTree:

    def __init__(self, size=3):
        self.__size = size
        self.__root = None
        self.__data_ptr = None

    def get_head(self):
        return self.__data_ptr

    def find_height(self):
        if self.__root is None:
            return 0
        height = 1
        cur_node = self.__root #type:TreeNode
        while not cur_node.is_leaf():
            cur_node = cur_node.pointers[0]
            height += 1
        return height

    def fill(self, l, root: TreeNode, h):
        if root is None:
            return
        l[h].append(root.keys)
        for c in root.pointers:
            self.fill(l, c, h + 1)

    def level_order(self):
        if self.__root is not None:
            h = self.find_height()
            l = []
            for i in range(h):
                l.append([])
            self.fill(l, self.__root, 0)

            for i in range(h - 1):
                for item in l[i]:
                    print(item, end=' ')
                print()

    def print_tree(self):
        if self.__root is None:
            return
        l = [self.__root] #type:list[TreeNode]
        start = 0
        end = 1
        while start != end:
            if not l[start].is_leaf():
                for p in l[start].pointers:
                    l.append(p)
                    end += 1
            if l[start].is_leaf():
                print(l[start].keys, l[start].pointers)
            else:
                print(l[start].keys)
            start += 1


    def insert(self, value, pointer):
        if self.__root is None:  # 若树中还没有节点，则根既为内节点也为叶节点
            self.__root = TreeNode(self.__size)
            leaf_node = self.__root
            self.__data_ptr = leaf_node
        else:
            leaf_node = self.__find_insert_leaf(value)
        if not leaf_node.is_full():
            self.__insert_in_leaf(value, pointer, leaf_node)
        else:
            leaf_node.insert_value(value, pointer)
            upper = (self.__size + 1) // 2
            new_leaf_node = TreeNode(self.__size)
            new_leaf_node.keys = leaf_node.keys[upper:]
            new_leaf_node.pointers = leaf_node.pointers[upper:]
            leaf_node.keys = leaf_node.keys[0:upper]
            leaf_node.pointers = leaf_node.pointers[0:upper]
            new_leaf_node.next = leaf_node.next
            leaf_node.next = new_leaf_node
            key = new_leaf_node.keys[0]
            self.__insert_in_parent(leaf_node, new_leaf_node, key)

    def delete(self, value):
        node = self.find_node(value)
        if node is not None:
            print("in")
            self.__delete_entry(node, value)

    def find_node(self, value):  # 该函数返回最大的<=搜索元素的块，若index与keyindex相同，则利用B+树查找
        if self.__root is None:
            return None
        else:
            node = self.__find_insert_leaf(value)
            found = False
            for key in node.keys:
                if key == value:
                    found = True
                    break
            if found:
                return node
            else:
                return None

    def find(self, value):
        if self.__root is None:
            return None
        else:
            node = self.__find_insert_leaf(value)
            found = False
            index =0
            for key in node.keys:
                if key <= value:
                    found = True
                    break
                index += 1
            if found:
                return (node.pointers[index], node, index)
            else:
                return None

    def __insert_in_leaf(self, value, pointer, node):
        node.insert_value(value, pointer)

    def __find_insert_leaf(self, value):
        if self.__root is not None:
            current_node = self.__root  # type:TreeNode
            while not current_node.is_leaf():
                index = 0
                for i in current_node.keys:
                    if value < i:
                        break
                    index += 1
                current_node = current_node.pointers[index]
            return current_node

    def __insert_in_parent(self, before_node: TreeNode, new_node: TreeNode, value):
        if before_node.parent is None:  # 若beforenode为根节点，则新建一个根节点并插入进去
            new_root = TreeNode(self.__size)
            new_root.pointers.append(before_node)
            new_root.pointers.append(new_node)
            new_root.keys.append(value)
            self.__root = new_root
            before_node.parent = new_root
            new_node.parent = new_root
            return
        parent = before_node.parent  # type:TreeNode
        # 先将新节点插入到parent中
        for i in range(0, len(parent.pointers)):
            if parent.pointers[i] == before_node:
                parent.pointers.insert(i + 1, new_node)
                parent.keys.insert(i, value)
                break
        if len(parent.pointers) <= self.__size + 1:  # 若插入后仍未超过规定的size
            new_node.parent = parent
        else:  # 否则则新建一个parent节点，将parent进行分裂
            upper = (self.__size + 1) // 2
            new_parent = TreeNode(self.__size)
            new_parent.pointers = parent.pointers[upper:]
            parent.pointers = parent.pointers[0:upper]
            key = parent.keys[upper - 1]
            new_parent.keys = parent.keys[upper:]
            parent.keys = parent.keys[:upper - 1]
            for child in new_parent.pointers:
                child.parent = new_parent
            for child in parent.pointers:
                child.parent = parent
            self.__insert_in_parent(parent, new_parent, key)

    def __delete_entry(self, node: TreeNode, value, child: TreeNode = None):
        temp_index = 0
        for temp in node.keys:
            if value == temp:
                node.keys.remove(temp)
                break
            temp_index += 1
        if child is not None:
            node.pointers.remove(child)
        else:
            node.pointers.remove(node.pointers[temp_index])
        if node.parent is None and len(node.pointers) == 1:
            self.__root = node.pointers[0]
            self.__root.parent = None
            del node
        elif node.parent is not None and \
                ((node.is_leaf() == False and len(node.pointers) < (self.__size + 1) // 2) or
                 (node.is_leaf() == True and len(node.keys) < self.__size // 2)):
            previous = True
            parent = node.parent  # type:TreeNode
            child_index = parent.pointers.index(node)
            another_node = None  # type:TreeNode
            key = None
            if child_index == len(parent.pointers) - 1:
                previous = False
                another_node = parent.pointers[child_index - 1]
                key = parent.keys[child_index - 1]
            else:
                another_node = parent.pointers[child_index + 1]
                key = parent.keys[child_index]
            # 若两个节点可以直接合并，则总是保留前一个节点
            if (another_node.is_leaf() == True and len(node.keys) + len(another_node.keys) <= self.__size) \
                    or (another_node.is_leaf() == False and len(node.pointers) + len(another_node.pointers) <= self.__size + 1):
                if previous:
                    temp = node
                    node = another_node
                    another_node = temp
                if not node.is_leaf():
                    another_node.pointers += node.pointers
                    another_node.keys = another_node.keys + \
                                        [key] + node.keys
                    for temp in node.pointers:
                        temp.parent = another_node
                else:
                    another_node.keys += node.keys
                    another_node.pointers += node.pointers
                    another_node.next = node.next
                self.__delete_entry(node.parent, key, node)
                if self.__data_ptr == node:
                    self.__data_ptr = another_node
                del node
            else:  # 从兄弟节点借值
                # 如果node在anothernode的后面,则表明node为最后一个，此时的childindex需要注意
                if not previous:
                    if not node.is_leaf():
                        temp_child = another_node.pointers[-1]
                        temp_value = another_node.keys[-1]
                        another_node.keys.remove(temp_value)
                        another_node.pointers.remove(temp_child)
                        node.keys.insert(0, key)
                        node.pointers.insert(0, temp_child)
                        temp_child.parent = node
                        parent.keys[child_index - 1] = temp_value
                    else:
                        temp_value = another_node.keys[-1]
                        temp_pointer = another_node.pointers[-1]
                        another_node.keys.remove(temp_value)
                        another_node.pointers.remove(temp_pointer)
                        node.keys.insert(0, temp_value)
                        node.pointers.insert(0, temp_pointer)
                        parent.keys[child_index - 1] = temp_value
                else:
                    # 若node在anothernode的前面，那么此时的childindex与value的index是一样的，不需要减1
                    if not node.is_leaf():
                        temp_child = another_node.pointers[0]
                        temp_value = another_node.keys[0]
                        another_node.pointers.remove(temp_child)
                        another_node.keys.remove(temp_value)
                        node.keys.append(key)
                        node.pointers.append(temp_child)
                        temp_child.parent = node
                        parent.keys[child_index] = temp_value
                    else:
                        temp_value = another_node.keys[0]
                        temp_pointer = another_node.pointers[0]
                        another_node.keys.remove(temp_value)
                        another_node.pointers.remove(temp_pointer)
                        node.keys.append(temp_value)
                        node.pointers.append(temp_pointer)
                        temp_value = another_node.keys[0]
                        parent.keys[child_index] = temp_value

'''

bpt = BPlusTree(3) #type:BPlusTree
n = 15
for i in range(15, 0, -1):
    tp = random.randint(1, n**2)
    bpt.insert(i, 0)
    # bpt.level_order()


bpt.print_tree()

print("FIND")
print(bpt.find(11))

for i in range(0, 9):
    bpt.delete(i)
    bpt.print_tree()
    print()


head = bpt.get_head() #type:TreeNode

while head is not None:
    print(head.keys, head.pointers)
    head = head.next

'''