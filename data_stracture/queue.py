class Queue:
    def __init__(self):
        self.items = []

    def enqueue(self, ele):
        return self.items.append(ele)

    def dequeue(self):
        return self.items.pop(0)

    def peek(self):
        return self.items[0]

    def empty(self):
        return self.items == []

    def size(self):
        return len(self.items)


q = Queue()
print(q.empty())
q.enqueue(0)
q.enqueue(1)
q.enqueue(2)
print(q.size())
print(q.peek())
q.dequeue()
print(q.peek())
print(q.size())

