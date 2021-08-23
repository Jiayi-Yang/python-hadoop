def bubble_sort(arr):
    for k in range(len(arr)-1, 0, -1):
        print(f"\nThe num of K:{k}")
        for n in range(k):
            print(f"The num of N:{n}")
            if arr[n] > arr[n+1]:
                arr[n], arr[n+1],  = arr[n+1],arr[n]
    return arr


print(bubble_sort([5, 1, 3, 2]))

