import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import glob
from lxml import etree

import xmltodict
import pandas as pd
import GerenciadorArquivos as ga

def extrair_dados_dfe(arquivo_xml, namespaces):
    """
    Extrai dados relevantes de um arquivo XML de DF-e.

    Args:
        arquivo_xml (str): Caminho para o arquivo XML.
        namespaces (dict): Namespaces utilizados nos arquivos XML.

    Returns:
        dict: Um dicionário com os dados extraídos, ou None em caso de erro.
    """

    try:
        tree = ET.parse(arquivo_xml)
        root = tree.getroot()

        # Extrai a chave de acesso (funciona para NF-e, CT-e e MDF-e)
        chave_acesso = root.find('.//ns:infNFe/ns:Id', namespaces).text.replace('NFe', '')

        # Extrai dados do emitente
        cnpj_emitente = root.find('.//ns:emit/ns:CNPJ', namespaces).text

        # Extrai dados do destinatário (pode ser CPF ou CNPJ)
        destinatario = root.find('.//ns:dest', namespaces)
        if destinatario.find('.//ns:CNPJ', namespaces) is not None:
            cpf_cnpj_destinatario = destinatario.find('.//ns:CNPJ', namespaces).text
        else:
            cpf_cnpj_destinatario = destinatario.find('.//ns:CPF', namespaces).text

        # Extrai dados do ICMS (pode variar entre NF-e, CT-e e MDF-e)
        valor_total_dfe = root.find('.//ns:total/ns:ICMSTot/ns:vNF', namespaces).text
        valor_total_base_icms = root.find('.//ns:total/ns:ICMSTot/ns:vBC', namespaces).text
        valor_total_icms = root.find('.//ns:total/ns:ICMSTot/ns:vICMS', namespaces).text
        valor_total_pis = root.find('.//ns:total/ns:ICMSTot/ns:vPIS', namespaces).text
        valor_total_cofins = root.find('.//ns:total/ns:ICMSTot/ns:vCOFINS', namespaces).text
        valor_total_icms_st = root.find('.//ns:total/ns:ICMSTot/ns:vST', namespaces).text
        valor_total_base_icms_st = root.find('.//ns:total/ns:ICMSTot/ns:vBCST', namespaces).text

        # Extrai a cidade de destino (pode variar entre NF-e, CT-e e MDF-e)
        cidade_destino = root.find('.//ns:dest/ns:enderDest/ns:xMun', namespaces).text

        dados_extraidos = {
            'chave_acesso': chave_acesso,
            'cnpj_emitente': cnpj_emitente,
            'cpf_cnpj_destinatario': cpf_cnpj_destinatario,
            'cidade_destino': cidade_destino,
            'valor_total_dfe': valor_total_dfe,
            'valor_total_base_icms': valor_total_base_icms,
            'valor_total_icms': valor_total_icms,
            'valor_total_pis': valor_total_pis,
            'valor_total_cofins': valor_total_cofins,
            'valor_total_icms_st': valor_total_icms_st,
            'valor_total_base_icms_st': valor_total_base_icms_st
        }

        return dados_extraidos

    except ET.ParseError as e:
        print(f"Erro de parsing XML no arquivo '{arquivo_xml}': {e}")
        return None
    except AttributeError as e:
        print(f"Atributo não encontrado no arquivo '{arquivo_xml}': {e}")
        return None
    except Exception as e:
        print(f"Erro ao processar o arquivo '{arquivo_xml}': {e}")
        return None


def converter_xml_para_parquet(pasta_xml, arquivo_parquet) : #, schema_xsd):
    """
    Converte múltiplos arquivos XML de DF-e em um único arquivo Parquet.

    Args:
        pasta_xml (str): Caminho para a pasta contendo os arquivos XML.
        arquivo_parquet (str): Caminho para o arquivo Parquet de saída.
        schema_xsd (str): Caminho para o arquivo XSD de validação.
    """

    todos_dados = []
    for arquivo_xml in glob.glob(os.path.join(pasta_xml, '*.xml')):

        #if not validar_schema_xml(arquivo_xml, schema_xsd):
        #    continue  # Ignora arquivos inválidos

        namespaces = {
            'ns': 'http://www.portalfiscal.inf.br/nfe',  # Namespace NF-e
            'nfe': 'http://www.portalfiscal.inf.br/nfe',
            'cte': 'http://www.portalfiscal.inf.br/cte',  # Namespace CT-e
            'mdfe': 'http://www.portalfiscal.inf.br/mdfe'  # Namespace MDF-e
        }

        dados_extraidos = extrair_dados_dfe(arquivo_xml, namespaces)

        if dados_extraidos:
            todos_dados.append(dados_extraidos)

    if todos_dados:
        df = pd.DataFrame(todos_dados)
        tabela = pa.Table.from_pandas(df)
        pq.write_table(tabela, arquivo_parquet)
        print(f"Arquivo Parquet '{arquivo_parquet}' criado com sucesso!")
    else:
        print("Nenhum dado extraído dos arquivos XML.")



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
lista_arquivos = ga.listar_arquivos_by_path(output_dir)
#os.listdir(output_dir)