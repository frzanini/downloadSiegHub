import os
import xmltodict
import pandas as pd
from utils.functions import GerenciadorArquivos

def get_info_nfe(nome_arquivo, valores):
    print(f"Arquivo {nome_arquivo} acessado com sucesso!")
    with open(f"nfs/{nome_arquivo}", "rb") as arquivo_xml: 
        dic_arquivo = xmltodict.parse(arquivo_xml)

        try:
            if "NFe" in dic_arquivo:
                infos_nf = dic_arquivo["NFe"]["infNFe"]
            else:
                infos_nf = dic_arquivo["nfeProc"]["NFe"]["infNFe"]
            numero_nota = infos_nf["@Id"]
            empresa_emissora = infos_nf["emit"]["xNome"]
            nome_cliente = infos_nf["dest"]["xNome"]
            endereco = infos_nf["dest"]["enderDest"]
            if "vol" in infos_nf["transp"]:
                peso_bruto = infos_nf["transp"]["vol"]["pesoB"]
            else:
                peso = "Não informado"
            valores.append([numero_nota, empresa_emissora, nome_cliente, endereco, peso_bruto])
        except Exception as e:
            print(f"Erro encontrado em: {e}, no arquivo {nome_arquivo}")


output_dir = os.path.join(os.path.join(os.getcwd(), "temp"))
lista_arquivos = GerenciadorArquivos.listar_arquivos_by_path(output_dir)
#os.listdir(output_dir)