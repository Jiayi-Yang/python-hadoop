def largestValues(root):
    if root == None:
        return []

    que = [root]
    re = []

    while len(que) != 0:
        size = len(que)
        tmax = float('-inf')

        for i in range(size):
            t = que.pop(0)
            tmax = max(tmax, t.val)

            if (t.left != None):
                que.append(t.left)
            if (t.right != None):
                que.append(t.right)

        re.append(tmax)
    return re


def bfs(root):
    que = [root]

    while len(que) > 0:
        size = len(que)
        for i in range(size):
            cur = que.pop(0)
            print(cur.val)
            if cur.left is not None:
                que.append(cur.left)
            if cur.right is not None:
                que.append(cur.right)


class TreeNode(object):
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right



# n1 = TreeNode(1)
# n2 = TreeNode(2)
# n3 = TreeNode(3)
# n1.left = n3
# n1.right = n2
# n3.left = TreeNode(5)
# n3.right = TreeNode(3)
# n2.right = TreeNode(9)
#
# print(largestValues(n1))

#         1
#       /   \
#     2       3
#   /   \
# 4       5

# 1   2   3
n1 = TreeNode(1)
n2 = TreeNode(2)
n3 = TreeNode(3)
#   1
#  /  \
#2      3
n1.left = n2
n1.right = n3
n2.left = TreeNode(4)
n2.right = TreeNode(5)
bfs(n1)
