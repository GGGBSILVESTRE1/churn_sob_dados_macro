import basedosdados as bd
import os
from dotenv import load_dotenv

load_dotenv()
GOOGLE_CLOUD_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID")


def extrair_dados_ans():
    query = """

    SELECT 
        ano, mes, codigo_operadora, sigla_uf, 
        sexo, faixa_etaria, tipo_vigencia_plano, 
        contratacao_beneficiario,    segmentacao_beneficiario, 
        tipo_vinculo, 
        SUM(quantidade_beneficiario_ativo) as total_ativos, 
        SUM(quantidade_beneficiario_aderido) as total_adesoes, 
        SUM(quantidade_beneficiario_cancelado) as total_cancelados
    FROM `basedosdados.br_ans_beneficiario.informacao_consolidada`
    WHERE sigla_uf = 'SP' AND ano >= 2021
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    """

    df = bd.read_sql(query, billing_project_id=GOOGLE_CLOUD_PROJECT_ID)
    df.to_parquet("data/raw/dados_ans.parquet", index=False)


if __name__ == "__main__":
    extrair_dados_ans()
    print("processo concluído")
