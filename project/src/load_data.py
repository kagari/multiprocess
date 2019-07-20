"""
前処理を行う
"""
import pandas as pd
import datetime
import glob
import os
from pprint import pprint
from concurrent import futures


def _read_csv_data(path_list, header):
    # データを読み込んでみる
    result = []
    for path in path_list:
        try:
            df = pd.read_csv(path, header=header)
            result.append(df)
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            # print(f"{path}, {e}")
            pass
                
    return result


def read_data(root_path, parallel=False):
    """
    input
    - root_path: データ置き場のルートのパス
    
    output
    - result: データのリスト
    """
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

    results = {}
    if parallel == "Thread":
        # parallelにデータを読む
        start = datetime.datetime.now()
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            th0 = executor.submit(_read_csv_data, rain_data_path_list, 0, 'rain')
            th1 = executor.submit(_read_csv_data, rx9_data_path_list, 1, 'rx9')
            th2 = executor.submit(_read_csv_data, rx11_data_path_list, 1, 'rx11')
            results['rain'] = pd.concat(th0.result())
            results['rx9'] = pd.concat(th1.result())
            results['rx11'] = pd.concat(th2.result())
        print(f"MultiThread During Time: {datetime.datetime.now() - start}")
        
    elif parallel == "Process":
        # parallelにデータを読む
        start = datetime.datetime.now()
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            p0 = executor.submit(_read_csv_data, rain_data_path_list, 0, 'rain')
            p1 = executor.submit(_read_csv_data, rx9_data_path_list, 1, 'rx9')
            p2 = executor.submit(_read_csv_data, rx11_data_path_list, 1, 'rx11')
            results['rain'] = pd.concat(p0.result())
            results['rx9'] = pd.concat(p1.result())
            results['rx11'] = pd.concat(p2.result())
        print(f"MultiProcess During Time: {datetime.datetime.now() - start}")
        
    else:
        # non parallel
        start = datetime.datetime.now()
        results["rain"] = pd.concat(_read_csv_data(rain_data_path_list, 0))
        results["rx9"]  = pd.concat(_read_csv_data(rx9_data_path_list,  1))
        results["rx11"] = pd.concat(_read_csv_data(rx11_data_path_list, 1))
        print(f"During Time; {datetime.datetime.now() - start}")
    return results


if __name__ == "__main__":
    # 速度比較用
    DATA_PATH = "../data/"
    
    # non parallel
    print("Non parallel")
    start = datetime.datetime.now()
    read_data(DATA_PATH)
    print(f"During Time; {datetime.datetime.now() - start}")
    
    # parallel
    print("On MultiParallel")
    start = datetime.datetime.now()
    read_data(DATA_PATH, parallel=True)
    print(f"During Time; {datetime.datetime.now() - start}")

    print("On MultiThread")
    start = datetime.datetime.now()
    read_data(DATA_PATH, parallel=True)
    print(f"During Time; {datetime.datetime.now() - start}")
