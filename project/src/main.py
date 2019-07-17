from load_data import read_data
import pandas as pd
import numpy as np
from datetime import datetime as dt

"""
rx11: 26GHz
rx9 : 18GHz
"""


def calc_rainfall(datas):
    # 降雨強度データの変換（手順1）
    rain_data = pd.to_numeric(datas['rain']['Recording started.'], errors='coerce')
    rain_data = rain_data * (8/1000+1/3) * 60
    rain_data.reindex()

    # 生データを物理量に対応させる（手順2）
    datas['rx9'][' 1803_RX_LEVEL'] = pd.to_numeric(datas['rx9'][' 1803_RX_LEVEL'], errors='coerce')
    datas['rx9'][' 1803_RX_LEVEL'] = datas['rx9'][' 1803_RX_LEVEL'].map(lambda x: x/2-121 if x >= 0 else (x+256)/2-121)
    
    datas['rx11'][' MX_RX_LEVEL'] = pd.to_numeric(datas['rx11'][' MX_RX_LEVEL'], errors='coerce')

    # 10秒毎のデータに変換する（ここが重い
    start = dt.now()
    rx9_data = _calc_mean_5sec(datas['rx9'][' 1803_RX_LEVEL'])
    print(f"during time: {dt.now() - start}")

    start = dt.now()
    rx11_data = _calc_mean_5sec(datas['rx11'][' MX_RX_LEVEL'])
    print(f"during time: {dt.now() - start}")

    return rain_data, rx9_data, rx11_data

    
def _calc_mean_5sec(df):
    print("=======Start mean 10sec Processing======")
    _data_on_5min = []
    for i in range(0, len(df), 5):
         _data_on_5min.append(np.mean(df[i:i+5]))
    rx_data = pd.DataFrame({'value': _data_on_5min})
    print("======= End  mean 10sec Processing======")

    return rx_data


if __name__ == "__main__":
    DATA_PATH = "../data/"
    # parallel
    datas = read_data(DATA_PATH, parallel=True)

    rain_data, rx9_data, rx11_data = calc_rainfall(datas)

    print(rain_data.head())
    print(rx9_data.head())
    print(rx11_data.head())
