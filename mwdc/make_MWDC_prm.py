# R. Kojima 2025

import numpy as np
from pyspark.sql.functions import explode

def drift_calib_data(timing_data: np.ndarray, filename: str, h_range: tuple[int, int], nbin:  int):
    """
    Docstring for drift_calib_data
    
    :param timing_data: Description
    :type timing_data: np.ndarray
    :param filename: Description
    :type filename: str
    :param h_range: Description
    :type h_range: tuple
    :param nbin: Description
    :type nbin: int
    """
    # Spark DataFrame から timing データを取得し、リストを展開
#    df_timing = sdf.select(explode(sdf[column_name]).alias("timing")).toPandas()
    df_timing = timing_data
    print(filename)
    # NumPy を使ってヒストグラムを作成
    # timing_data is already a histogram, use it directly
    histy_y = df_timing
    histy_edge = np.linspace(h_range[0], h_range[1], nbin + 1)
    histy_x = (histy_edge[:-1] + histy_edge[1:]) / 2.0
    hx_contents = histy_y.astype(float)

    # 正規化（全体を合計して1にする）
    totx = np.sum(hx_contents)
    if totx > 0:
        hx_contents /= totx
    else:
        print("エラー: ヒストグラムの合計がゼロです。")
        return

    # 積算分布関数（CDF）の計算
    tx = np.cumsum(hx_contents)

    # CDF の 0.1% (tolerance) 以上の範囲を特定
    tolerance = 1e-3
    begin_x = None
    end_x = None

    for i in range(len(tx)):
        if begin_x is None and tx[i] >= tolerance:
            begin_x = i
        if tx[i] >= 1.0 - tolerance:
            end_x = i
            break

    # データのサイズと出力ファイル名
    if begin_x is not None and end_x is not None:
        size_x = end_x - begin_x + 1
        #filenamex = f"./prm/mwdc/{filename}.dat"

        # ファイルに書き出し
        with open(filename, "w") as outfilex:
            outfilex.write("histy_x,tx\n")
            for i in range(begin_x, end_x + 1):
                outfilex.write(f"{histy_x[i]},{tx[i]}\n")

        print(f"ファイル '{filename}' に保存しました。")
    else:
        print("エラー: begin_x または end_x が未定義です。")

    return histy_x,hx_contents,filename