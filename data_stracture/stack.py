class Stack:
    def __init__(self):
        self.items = []

    def empty(self):
        return self.items == []

    def push(self, ele):
        return self.items.append(ele)

    def pop(self):
        return self.items.pop()

    def peek(self):
        return self.items[-1]

    def size(self):
        return len(self.items)

s = Stack()
print(s.empty())
print(s.size())
s.push(0)
s.push(1)
s.push(2)
print(s.peek())
print(s.size())
print(s.pop())
print(s.peek())
print(s.size())

