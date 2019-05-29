import concurrent.futures

def merge(array, n_process=0):
    """
    array: list data
    n_process: number of process. 2^k
    """
    l = len(array)
    if l <= 1:
        return array
    # when n_process == 0, paralleling
    if n_process == 1:
        # multiprocessing
        with concurrent.futures.ThreadPoolExecutor() as executor:
            th1 = executor.submit(merge, array[:round(l/2)])
            th2 = executor.submit(merge, array[round(l/2):])
            left = th1.result()
            right = th2.result()
    else:
        left = merge(array[:round(l/2)], n_process/2)
        right = merge(array[round(l/2):], n_process/2)

    array = []
    while len(left) != 0 and len(right) != 0:
        if left[0] < right[0]:
            array.append(left.pop(0))
        else:
            array.append(right.pop(0))
    if len(left) != 0:
        array.extend(left)
    if len(right) != 0:
        array.extend(right)
    return array

if __name__ == "__main__":
    import sys
    import datetime
    n_process = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    # array = list(map(int, sys.argv[2].split(','))) if len(sys.argv) > 2 else list(reversed(range(10000)))
    max_size = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    for i in range(1, max_size):
        array = list(reversed(range(10 ** i)))
        start = datetime.datetime.now()
    # array = [10, 3, 1, 4, 5, 3, 2, 9 ,7]
        sorted_array = merge(array, n_process)
        end = datetime.datetime.now()
        # print("sorted:", sorted_array[:10])
        print(f'size {len(array)}: {end - start}')
