import concurrent.futures
import time

def func1(x, y):
    print("func1")
    print(x, y)
    for x in range(50000000):
        x * 100
    return True

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        generator = executor.map(func1, [1,2,3,4], [5,6,7,8])
        results = list(generator)
        print(results)
