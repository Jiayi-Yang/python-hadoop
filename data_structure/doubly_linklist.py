class DoublyLinkListNode:
    def __init__(self,value):
        self.value = value
        self.next = None
        self.pre = None
        
a = DoublyLinkListNode(1)
b = DoublyLinkListNode(2)
c = DoublyLinkListNode(3)
a.next = b
b.pre = a

b.next = c
c.pre = a

print(a.value)
print(b.next.value)
print(c.pre.value)
