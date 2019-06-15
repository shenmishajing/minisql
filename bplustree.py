import random

size = 5  # 为节点中存储的记录数


class TreeNode:

    def __init__(self, key_index=0):
        self.values = []
        self.key_index = key_index
        self.next = None
        self.parent = None
        self.children = []  # type:list[TreeNode]

    def is_full(self):
        return len(self.values) == size

    def is_empty(self):
        return len(self.values) == 0

    def is_leaf(self):
        return len(self.children) == 0

    def insert_value(self, value):
        self.values.append(value)
        if type(value) == tuple:
            self.values.sort(key=lambda x: x[self.key_index])
        else:
            self.values.sort()

    def __del__(self):
        print("Node Released")
        self.values.clear()
        self.children.clear()


class BPlusTree:

    def __init__(self, key_index=0):
        self.__key_index = key_index
        self.__root = None
        self.__data_ptr = None  # type:TreeNode

    def get_head(self):
        return self.__data_ptr

    def find_height(self):
        if self.__root is None:
            return 0
        height = 1
        cur_node = self.__root
        while not cur_node.is_leaf():
            cur_node = cur_node.children[0]
            height += 1
        return height

    def fill(self, l, root, h):
        if root is None:
            return
        l[h].append(root.values)
        for c in root.children:
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

    def insert(self, value):
        if self.__root is None:  # 若树中还没有节点，则根既为内节点也为叶节点
            self.__root = TreeNode(self.__key_index)
            leaf_node = self.__root
            self.__data_ptr = leaf_node
        else:
            leaf_node = self.__find_insert_leaf(value)
        if not leaf_node.is_full():
            self.__insert_in_leaf(value, leaf_node)
        else:
            leaf_node.insert_value(value)
            upper = (size + 1) // 2
            new_leaf_node = TreeNode(self.__key_index)
            new_leaf_node.values = leaf_node.values[upper:]
            leaf_node.values = leaf_node.values[0:upper]
            new_leaf_node.next = leaf_node.next
            leaf_node.next = new_leaf_node
            key = new_leaf_node.values[0][self.__key_index]
            self.__insert_in_parent(leaf_node, new_leaf_node, key)

    def delete(self, value, index):
        node = self.find(value, index)
        if node is not None:
            print("in")
            self.__delete_entry(node, value, index)

    def find(self, value, index):  # 该函数返回最大的<=搜索元素的块，若index与keyindex相同，则利用B+树查找
        if index != self.__key_index:  # 否则返回第一个找到的node
            node = self.__data_ptr
            found = False
            while node is not None:
                for temp_value in node.values:
                    if temp_value[index] == value:
                        found = True
                        break
                if found:
                    break
            return node
        else:
            if self.__root is None:
                return None
            else:
                node = self.__find_insert_leaf(value)
                found = False
                for v in node.values:
                    if v[self.__key_index] == value:
                        found = True
                        break
                if not found:
                    node = None
                return node

    def __insert_in_leaf(self, value, node):
        node.insert_value(value)

    def __find_insert_leaf(self, value):
        if self.__root is not None:
            current_node = self.__root  # type:TreeNode
            while not current_node.is_leaf():
                index = 0
                for i in current_node.values:
                    if type(value) == tuple:
                        if value[self.__key_index] < i:
                            break
                    else:
                        if value < i:
                            break
                    index += 1
                current_node = current_node.children[index]
            return current_node

    def __insert_in_parent(self, before_node: TreeNode, new_node: TreeNode, value):
        if before_node.parent is None:  # 若beforenode为根节点，则新建一个根节点并插入进去
            new_root = TreeNode(self.__key_index)
            new_root.children.append(before_node)
            new_root.children.append(new_node)
            new_root.values.append(value)
            self.__root = new_root
            before_node.parent = new_root
            new_node.parent = new_root
            return
        parent = before_node.parent  # type:TreeNode
        # 先将新节点插入到parent中
        for i in range(0, len(parent.children)):
            if parent.children[i] == before_node:
                parent.children.insert(i + 1, new_node)
                parent.values.insert(i, value)
                break
        if len(parent.children) <= size + 1:  # 若插入后仍未超过规定的size
            new_node.parent = parent
        else:  # 否则则新建一个parent节点，将parent进行分裂
            upper = (size + 1) // 2
            new_parent = TreeNode(self.__key_index)
            new_parent.children = parent.children[upper:]
            parent.children = parent.children[0:upper]
            key = parent.values[upper - 1]
            new_parent.values = parent.values[upper:]
            parent.values = parent.values[:upper - 1]
            for child in new_parent.children:
                child.parent = new_parent
            for child in parent.children:
                child.parent = parent
            self.__insert_in_parent(parent, new_parent, key)

    def __delete_entry(self, node: TreeNode, value, index, child: TreeNode = None):
        for temp in node.values:
            if type(temp) == tuple:
                if value == temp[index]:
                    node.values.remove(temp)
                    break
            else:
                if value == temp:
                    node.values.remove(temp)
                    break
        if child is not None:
            node.children.remove(child)
        if node.parent is None and len(node.children) == 1:
            self.__root = node.children[0]
            self.__root.parent = None
            del node
        elif node.parent is not None and \
                ((node.is_leaf() == False and len(node.children) < (size + 1) // 2) or
                 (node.is_leaf() == True and len(node.values) < size // 2)):
            previous = True
            parent = node.parent  # type:TreeNode
            child_index = parent.children.index(node)
            another_node = None  # type:TreeNode
            key = None
            if child_index == len(parent.children) - 1:
                previous = False
                another_node = parent.children[child_index - 1]
                key = parent.values[child_index - 1]
            else:
                another_node = parent.children[child_index + 1]
                key = parent.values[child_index]
            # 若两个节点可以直接合并，则总是保留前一个节点
            if (another_node.is_leaf() == True and len(node.values) + len(another_node.values) <= size) \
                    or (another_node.is_leaf() == False and len(node.children) + len(another_node.children) <= size + 1):
                if previous:
                    temp = node
                    node = another_node
                    another_node = temp
                if not node.is_leaf():
                    another_node.children += node.children
                    another_node.values = another_node.values + \
                        [key] + node.values
                    for temp in node.children:
                        temp.parent = another_node
                else:
                    another_node.values += node.values
                    another_node.next = node.next
                self.__delete_entry(node.parent, key, self.__key_index, node)
                if self.__data_ptr == node:
                    self.__data_ptr = another_node
                del node
            else:  # 从兄弟节点借值
                # 如果node在anothernode的后面,则表明node为最后一个，此时的childindex需要注意
                if not previous:
                    if not node.is_leaf():
                        temp_child = another_node.children[-1]
                        temp_value = another_node.values[-1]
                        another_node.values.remove(temp_value)
                        another_node.children.remove(temp_child)
                        node.values.insert(0, key)
                        node.children.insert(0, temp_child)
                        temp_child.parent = node
                        parent.values[child_index - 1] = temp_value
                    else:
                        temp_value = another_node.values[-1]
                        another_node.values.remove(temp_value)
                        node.values.insert(0, temp_value)
                        parent.values[child_index -
                                      1] = temp_value[self.__key_index]
                else:
                    # 若node在anothernode的前面，那么此时的childindex与value的index是一样的，不需要减1
                    if not node.is_leaf():
                        temp_child = another_node.children[0]
                        temp_value = another_node.values[0]
                        another_node.children.remove(temp_child)
                        another_node.values.remove(temp_value)
                        node.values.append(key)
                        node.children.append(temp_child)
                        temp_child.parent = node
                        parent.values[child_index] = temp_value
                    else:
                        temp_value = another_node.values[0]
                        another_node.values.remove(temp_value)
                        node.values.append(temp_value)
                        temp_value = another_node.values[0]
                        parent.values[child_index] = temp_value[self.__key_index]


bpt = BPlusTree(0)
n = 300
for i in range(n):
    tp = (random.randint(1, n**2),)
    bpt.insert(tp)
    # bpt.level_order()


bpt.level_order()

print(bpt.find(0, 0) is None)


for i in range(1, 4):
    bpt.delete(i, 0)

head = bpt.get_head()

while head is not None:
    print(head.values)
    head = head.next
