from bcb import sgs
import pandas as pd
import httpx


def extrair_e_consolidar_macro(dict_series, start="2018-01-01"):
    list_dfs = []

    for nome, codigo in dict_series.items():
        df = sgs.get({nome: codigo}, start=start)
        sgs._CLIENT = httpx.Client(timeout=60.0)
        # remover duplicados
        df = df[~df.index.duplicated(keep="first")]
        # 1 linha por mes (ultimo dia do mes)
        df = df.resample("ME").last()
        # converter index para periodo mensal (YYYY-MM)
        df.index = df.index.to_period("M")
        list_dfs.append(df)

    df_consolidado = pd.concat(list_dfs, axis=1)

    df_consolidado.to_csv("data/raw/dados_macro.csv")

    return df_consolidado


if __name__ == "__main__":
    dict_series = {
        "selic": 432,
        "ipca": 433,
        "inadimplencia_pf": 21084,
        "juros_cartao": 20786,
    }
    df_consolidado = extrair_e_consolidar_macro(dict_series)
    print("processo concluído")
