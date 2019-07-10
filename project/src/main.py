from load_data import read_data
import pandas as pd

def cal_rainfall(data):
    pass

if __name__ == "__main__":
    DATA_PATH = "../data/"
    # non parallel
    datas = read_data(DATA_PATH, parallel=True)
    print(pd.concat([datas['rain'].head(), datas['rain'].tail()]))
