class DoublyLinkedListNode:
    def __init__(self,value):
        self.value = value
        self.next = None
        self.pre = None
        
a = DoublyLinkedListNode(1)
b = DoublyLinkedListNode(2)
c = DoublyLinkedListNode(3)
a.next = b
b.pre = a

b.next = c
c.pre = a

print(a.value)
print(b.next.value)
print(c.pre.value)
