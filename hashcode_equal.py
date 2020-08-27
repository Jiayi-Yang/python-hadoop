class User(object):
    def __init__(self, age, first_name):
        self.age = age
        self.first_name = first_name

    def __hash__(self):
        # old method
        # prime = 31
        # return self.age + 31
        # new method
        return hash(self.age)

    def __eq__(self, other):
        if not isinstance(other, User):
            return False
        return self.age == other.age


a = User("Tom1", 123)
dic = {a: 10}
b = User("Tom1", 123)
print(b in dic)

