from load_data import read_data
import pandas as pd

def cal_rainfall(data):
    pass

"""
rx11: 26GHz
rx9 : 18GHz
"""

if __name__ == "__main__":
    DATA_PATH = "../data/"
    # parallel
    datas = read_data(DATA_PATH, parallel=True)

    # 10秒毎とのデータに変換する
    print(datas['rain'].head())

    # 降雨強度データの変換（手順1）
    print(pd.concat([datas['rain'].head(), datas['rain'].tail()]))
    datas['rain'] = datas['rain'] * (8/1000+1/3) * 60
    print(pd.concat([datas['rain'].head(), datas['rain'].tail()]))

    # 生データを物理量に対応させる（手順2）
    print(pd.concat([datas['rx9'].head(), datas['rx9'].tail()]))
    datas['rx9'][' 1803_RX_LEVEL'] = pd.to_numeric(datas['rx9'][' 1803_RX_LEVEL'], errors='coerce')
    datas['rx9'][' 1803_RX_LEVEL'] = datas['rx9'][' 1803_RX_LEVEL'].map(lambda x: x/2-121 if x >= 0 else (x+256)/2-121)
    print(pd.concat([datas['rx9'].head(), datas['rx9'].tail()]))

    print(datas['rx11'].head())
    datas['rx11'][' MX_RX_LEVEL'] = pd.to_numeric(datas['rx11'][' MX_RX_LEVEL'], errors='coerce')
    print(datas['rx11'].head())
