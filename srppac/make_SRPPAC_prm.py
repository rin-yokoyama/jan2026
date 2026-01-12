# R. Kojima
import numpy as np

def dq_calib_data(charge_data,filename,h_range, nbin):
    df_charge = charge_data
    print(filename)
    print("ここまで")
    # NumPy を使ってヒストグラムを作成
    histy_y, histy_edge = np.histogram(df_charge, range=h_range, bins=nbin)
    histy_x = (histy_edge[:-1] + histy_edge[1:]) / 2.0
    #hx_contents = histy_y.astype(float)

    hx_contents = charge_data

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
        filenamex = filename

        # ファイルに書き出し
        with open(filenamex, "w") as outfilex:
            outfilex.write(f"histy_x,tx\n")
            for i in range(begin_x, end_x + 1):
                outfilex.write(f"{histy_x[i]},{tx[i]}\n")

        print(f"ファイル '{filenamex}' に保存しました。")
    else:
        print("エラー: begin_x または end_x が未定義です。")

    return histy_x,hx_contents,filenamex