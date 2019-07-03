def quicksort(array):
    less = []
    equal = []
    greater = []

    if len(array) > 1:
        pivot = array[0]
        for x in array:
            if x < pivot:
                less.append(x)
            elif x == pivot:
                equal.append(x)
            elif x > pivot:
                greater.append(x)
        # Don't forget to return something!
        return quicksort(less)+equal+quicksort(greater)
    else:
        return array

def quicksort_parallel(array, n_parallel):
    from concurrent.futures import ProcessPoolExecutor
    less = []
    equal = []
    greater = []

    n_parallel -= 1

    if len(array) > 1:
        pivot = array[0]
        for x in array:
            if x < pivot:
                less.append(x)
            elif x == pivot:
                equal.append(x)
            elif x > pivot:
                greater.append(x)
        # Don't forget to return something!
        if n_parallel > 0:
            with ProcessPoolExecutor() as executor:
                results = executor.map(quicksort_parallel, [less, greater], [n_parallel, n_parallel])
                less = list(result)
                return less+equal+greater
        else:
            return quicksort(less)+equal+quicksort(greater)
    else:
        return array

if __name__ == "__main__":
    interval = 20000
    import numpy as np
    import datetime
    import sys
    sys.setrecursionlimit(interval) # 最大再帰回数を2000までに増やす

    array = np.array(list(reversed(range(interval-100))))
    print(array[:10])
    start = datetime.datetime.now()
    result = quicksort(array)
    print("during time: {}".format(datetime.datetime.now() - start))
    print(result[:10])

    array = np.array(list(reversed(range(interval-100))))
    print(array[:10])
    start = datetime.datetime.now()
    result = quicksort_parallel(array, 2)
    print("during time: {}".format(datetime.datetime.now() - start))
    print(result[:10])
