def binary_search(arr, target):
    left = 0
    right = len(arr) - 1
    found = False
    while left <= right and not found:
        mid = (left + right)//2
        if arr[mid] == target:
            found = True
        elif target < arr[mid]:
            right = mid - 1
        elif target > mid:
            left = mid + 1
    return found


def rec_binary_search(arr, target):
    found = False
    mid = len(arr) // 2
    if len(arr) == 0:
        return False
    elif arr[mid] == target:
        found = True
    elif target < arr[mid]:
        return rec_binary_search(arr[:mid - 1], target)
    elif target > arr[mid]:
        return rec_binary_search(arr[mid + 1:], target)
    return found


print(rec_binary_search([1, 2, 3, 4, 5], 100))

print(binary_search([1, 2, 3, 4, 5], 100))
print(binary_search([1, 2, 3, 4, 5], 1))
print(binary_search([1, 2, 3, 4, 5], 5))


print(rec_binary_search([1, 2, 3, 4, 5], 100))
print(rec_binary_search([1, 2, 3, 4, 5], 1))
print(rec_binary_search([1, 2, 3, 4, 5], 5))


