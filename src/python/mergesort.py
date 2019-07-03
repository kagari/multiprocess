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
        # print("n_process: ", int(n_process))
        with concurrent.futures.ProcessPoolExecutor() as executor:
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
    n_process =  int(sys.argv[sys.argv.index("--n_process")+1] if "--n_process" in sys.argv else 0)
    size = int(sys.argv[sys.argv.index("--size")+1] if "--size" in sys.argv else 10000)
    array = list(reversed(range(size)))
    start = datetime.datetime.now()
    sorted_array = merge(array, n_process)
    end = datetime.datetime.now()
    print(f'size {len(array)}: {end - start}')
