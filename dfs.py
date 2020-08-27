def preorder(root):
    if root:
        return [root.val] + preorder(root.left) + preorder(root.right)
    else:
        return []


def inorder(root):
    if root:
        left = inorder(root.left)
        node = [root.val]
        right = inorder(root.right)
        return left + node + right
    else:
        return []


def postorder(root):
    if root:
        return postorder(root.left) + postorder(root.right) + [root.val]
    else:
        return []


class TreeNode(object):
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

#         1
#       /   \
#     2       3
#   /   \
# 4       5
# inorder(root.left) + [root.val] + inorder(root.right)
n1 = TreeNode(1)
n2 = TreeNode(2)
n3 = TreeNode(3)
n1.left = n2
n1.right = n3
n2.left = TreeNode(4)
n2.right = TreeNode(5)

print(preorder(n1))
print(inorder(n1))
print(postorder(n1))

