from load_data import read_data
import pandas as pd
import numpy as np
from datetime import datetime as dt

def calc_rainfall(data):
    pass

"""
rx11: 26GHz
rx9 : 18GHz
"""

def _calc_mean_5sec(df):
    print("=======Start mean 5sec Processing======")
    _data_on_5min = []
    for i in range(0, len(df), 5):
         _data_on_5min.append(np.mean(df[i:i+5]))
    rx_data = pd.DataFrame({'value': _data_on_5min})
    print("======= End  mean 5sec Processing======")

    return rx_data

if __name__ == "__main__":
    DATA_PATH = "../data/"
    # parallel
    datas = read_data(DATA_PATH, parallel=True)

    # 10秒毎のデータに変換する
    print("=======Start Rx9  Processing======")
    start = dt.now()
    datas['rx9'][' 1803_RX_LEVEL'] = pd.to_numeric(datas['rx9'][' 1803_RX_LEVEL'], errors='coerce')
    rx9_data = _calc_mean_5sec(datas['rx9'][' 1803_RX_LEVEL'])
    print(f"during time: {dt.now() - start}")

    print("=======Start Rx11 Processing======")
    start = dt.now()
    datas['rx11'][' MX_RX_LEVEL'] = pd.to_numeric(datas['rx11'][' MX_RX_LEVEL'], errors='coerce')
    rx11_data = _calc_mean_5sec(datas['rx11'][' MX_RX_LEVEL'])
    print(f"during time: {dt.now() - start}")

    # 降雨強度データの変換（手順1）
    print(pd.concat([datas['rain'].head(), datas['rain'].tail()]))
    datas['rain'] = datas['rain'] * (8/1000+1/3) * 60
    print(pd.concat([datas['rain'].head(), datas['rain'].tail()]))

    # 生データを物理量に対応させる（手順2）
    print(pd.concat([rx9_data.head(), rx9_data.tail()]))
    rx9_data['value'] = rx9_data['value'].map(lambda x: x/2-121 if x >= 0 else (x+256)/2-121)
    print(pd.concat([rx9_data.head(), rx9_data.tail()]))

    print(rx11_data.head())
    rx11_data['value'] = pd.to_numeric(rx11_data['value'], errors='coerce')
    print(rx11_data.head())
