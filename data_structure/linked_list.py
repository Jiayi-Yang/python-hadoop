class Node:
    def __init__(self, value):
        self.value = value
        self.next = None


def cycle_check(node):
    slow = node
    fast = node
    while fast is not None and fast.next is not None:
        slow = slow.next
        fast = fast.next.next
        if fast == slow:
            return True
    else:
        return False


# No cycle
x = Node(0)
y = Node(1)
z = Node(2)
x.next = y
y.next = z

# Have cycle
a = Node(0)
b = Node(1)
c = Node(2)
a.next = b
b.next = c
c.next = a

print(cycle_check(x))
print(cycle_check(a))

