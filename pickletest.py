import pickle


class A:

    def __init__(self):
        self.values = [1, 2, 3, 4]

class Test:

    def __init__(self, value=0):
        self.value = value
        self.oba = A()

    def get_list(self):
        return self.oba.values

    def get_value(self):
        return self.value




with open('./test.obj', 'rb') as f:
    data = pickle.load(f) #type:Test
    print(data.value)
    print(data.get_list())