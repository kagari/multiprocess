"""
前処理を行う
"""
import concurrent.futures
import pandas as pd
import datetime
import glob
import os
from pprint import pprint

def read_csv_data(path_list, header):
    # データを読み込んでみる
    result = []
    for path in path_list:
        try:
            df = pd.read_csv(path, header=header)
            result.append(df)
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            print(f"{path}, {e}")
                
    return result

def read_data(root_path, parallel=False):
    # データのパス
    DATA_PATH = root_path # データのルートパス
    RAIN_DATA_PATH = DATA_PATH + "RainData"
    Rx_DATA_PATH = DATA_PATH + "RxDATA"
    # ファイルへのパスのリストを取得
    rain_data_path_list = sorted(glob.glob(RAIN_DATA_PATH + "/**/*"))
    rx11_data_path_list = sorted(glob.glob(Rx_DATA_PATH + "/*/*/192.168.100.11_csv.log"))
    rx9_data_path_list = sorted(glob.glob(Rx_DATA_PATH + "/*/*/192.168.100.9_csv.log"))
    # 並列で処理しやすいようにパスを1つのリストにまとめる
    data_paths = [rain_data_path_list, rx11_data_path_list, rx9_data_path_list]

    if parallel:
        # parallelにデータを読む
        start = datetime.datetime.now()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_data_list = [executor.submit(read_csv_data, data_path, header) for data_path, header in zip(data_paths, [0,1,1])]
            results = []
            for future in concurrent.futures.as_completed(future_to_data_list):
                results.append(future.result())
        print(f"Multiprocessing During Time; {datetime.datetime.now() - start}")
        for result in results:
            print(len(result))
    else:
        # non parallel
        start = datetime.datetime.now()
        read_csv_data(rain_data_path_list, 0)
        read_csv_data(rx11_data_path_list, 1)
        read_csv_data(rx9_data_path_list,  1)
        print(f"During Time; {datetime.datetime.now() - start}")



if __name__ == "__main__":
    DATA_PATH = "../data/"
    # parallel
    read_data(DATA_PATH)
    # parallel
    read_data(DATA_PATH, parallel=True)
