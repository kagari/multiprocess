from mergesort import merge
import sys
import datetime
import numpy as np

if __name__ == "__main__":
    DATA_DIR = "./data/"
    measure_start = datetime.datetime.now()
    with open(DATA_DIR + "msort.txt", "w") as f:
        f.write(f'parallel num, data size, sorting time\n')
        for n_process in range(0, 7, 2): # 4threadまで
            for size in range(10000, 100000, 10000):
                times = []
                for _ in range(5): # 5回実行し、平均速度を取る
                    array = list(reversed(range(size)))
                    start = datetime.datetime.now() # 計測開始
                    sorted_array = merge(array, n_process)
                    end = datetime.datetime.now() # 計測終了
                    times.append(end - start)
                f.write(f'{n_process}, {len(array)}, {np.mean(times)}\n')
    
    print(f'{datetime.datetime.now() - measure_start}')
