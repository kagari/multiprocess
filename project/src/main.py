import pandas as pd
import numpy as np
import sys

from load_data import read_data
from datetime import datetime as dt
from concurrent import futures

"""
rx11: 26GHz
rx9 : 18GHz
"""


def calc_rainfall(datas, parallel=False):
    # 降雨強度データの変換（手順1）
    rain_data = pd.to_numeric(datas['rain']['Recording started.'], errors='coerce')
    rain_data = rain_data * (8/1000+1/3) * 60
    rain_data = pd.DataFrame({'value': rain_data.values})

    # 生データを物理量に対応させる（手順2）
    # 10秒毎のデータに変換する（ここが重い
    if parallel:
        # 並列に実行
        with futures.ProcessPoolExecutor(max_workers=4) as executor:
            future0 = executor.submit(_translate, datas['rain']['Recording started.'], 'rain')
            future1 = executor.submit(_translate, datas['rx9'][' 1803_RX_LEVEL'], 'rx9')
            future2 = executor.submit(_translate, datas['rx11'][' MX_RX_LEVEL'], 'rx11')
            rain_data = future0.result()
            rx9_data = future1.result()
            rx11_data = future2.result()
            
    else:
        # 直列に実行
        rain_data = _translate(datas['rain']['Recording started.'], 'rain')
        rx9_data = _translate(datas['rx9'][' 1803_RX_LEVEL'], 'rx9')
        rx11_data = _translate(datas['rx11'][' MX_RX_LEVEL'], 'rx11')

    return rain_data, rx9_data, rx11_data

def _translate(data, data_type):
    """
    data     : datasのリストからrain, rx9などを指定して、対象のカラムを取ってきたもの
    data_type: rain, rx9, rx11が入る
    """
    start = dt.now()
    
    numeric_data = pd.to_numeric(data, errors='coerce')
    if data_type == "rain":
        numeric_data = numeric_data * (8/1000+1/3) * 60
        rain_data = pd.DataFrame({'value': numeric_data})
        return rain_data
        
    elif data_type == "rx9":
        numeric_data = numeric_data.map(lambda x: x/2-121 if x >= 0 else (x+256)/2-121)
        rx9_data = _calc_mean_5sec(numeric_data)
        print(f"during time: {dt.now() - start}")
        return rx9_data
    
    elif data_type == "rx11":
        rx11_data = _calc_mean_5sec(numeric_data)
        print(f"during time: {dt.now() - start}")
        return rx11_data
    
    else:
        print(f"{data_type} is invalid data_type")
        sys.exit()
    
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

    # non parallel
    print("Non parallel")
    start = dt.now()
    rain_data, rx9_data, rx11_data = calc_rainfall(datas, parallel=False)
    print(f"During Time; {dt.now() - start}")
    
    # parallel
    print("On parallel")
    start = dt.now()
    rain_data, rx9_data, rx11_data = calc_rainfall(datas, parallel=True)
    print(f"During Time; {dt.now() - start}")
    
    print(rain_data.head())
    print(rx9_data.head())
    print(rx11_data.head())
