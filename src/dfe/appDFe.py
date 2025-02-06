import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import glob
from lxml import etree
import re

def validar_schema_xml(arquivo_xml, schema_xsd):
    """
    Valida um arquivo XML contra um schema XSD.

    Args:
        arquivo_xml (str): Caminho para o arquivo XML.
        schema_xsd (str): Caminho para o arquivo XSD.

    Returns:
        bool: True se o arquivo XML for válido, False caso contrário.
    """
    try:
        xml_parser = etree.XMLParser()
        xml_tree = etree.parse(arquivo_xml, xml_parser)

        xsd_tree = etree.parse(schema_xsd)
        xml_schema = etree.XMLSchema(xsd_tree)

        if xml_schema.validate(xml_tree):
            return True
        else:
            print(f"Arquivo XML '{arquivo_xml}' não está em conformidade com o schema XSD '{schema_xsd}'. Erros: {xml_schema.error_log}")
            return False

    except Exception as e:
        print(f"Erro ao validar schema para o arquivo '{arquivo_xml}': {e}")
        return False



def extrair_dados_dfe(arquivo_xml):
    """
    Extrai dados relevantes de um arquivo XML de DF-e, 
    validando o tipo de chave de acesso.

    Args:
        arquivo_xml (str): Caminho para o arquivo XML.

    Returns:
        dict: Um dicionário com os dados extraídos, ou None em caso de erro.
    """

    try:
        tree = ET.parse(arquivo_xml)
        root = tree.getroot()

        # Define namespaces
        namespaces = {
            'ns': 'http://www.portalfiscal.inf.br/nfe',  # Namespace NF-e
            'nfe': 'http://www.portalfiscal.inf.br/nfe',
            'cte': 'http://www.portalfiscal.inf.br/cte',  # Namespace CT-e
            'mdfe': 'http://www.portalfiscal.inf.br/mdfe'  # Namespace MDF-e
        }

        # Extrai a chave de acesso usando XPath (considerando diferentes namespaces)
        chave_acesso_elemento = root.find('.//ns:infNFe/ns:Id', namespaces) or \
                                 root.find('.//cte:infCte/cte:Id', namespaces) or \
                                 root.find('.//mdfe:infMDFe/mdfe:Id', namespaces)

        if chave_acesso_elemento is None:
            print(f"Chave de acesso não encontrada no arquivo '{arquivo_xml}'.")
            return None

        chave_acesso = chave_acesso_elemento.text.replace('NFe', '').replace('CTe', '').replace('MDFe', '')

        # Valida a chave de acesso
        if not validar_chave_acesso(chave_acesso):
            print(f"Chave de acesso inválida no arquivo '{arquivo_xml}'.")
            return None

        # Extrai dados do emitente (considerando diferentes namespaces)
        emitente = root.find('.//ns:emit', namespaces) or root.find('.//cte:emit', namespaces) or root.find('.//mdfe:emit', namespaces)
        cnpj_emitente = emitente.find('.//ns:CNPJ', namespaces).text if emitente.find('.//ns:CNPJ', namespaces) is not None else None

        # Extrai dados do destinatário (considerando diferentes namespaces e CPF/CNPJ)
        destinatario = root.find('.//ns:dest', namespaces) or root.find('.//cte:dest', namespaces) or root.find('.//mdfe:dest', namespaces)
        if destinatario is not None:
            if destinatario.find('.//ns:CNPJ', namespaces) is not None:
                cpf_cnpj_destinatario = destinatario.find('.//ns:CNPJ', namespaces).text
            elif destinatario.find('.//ns:CPF', namespaces) is not None:
                cpf_cnpj_destinatario = destinatario.find('.//ns:CPF', namespaces).text
            else:
                cpf_cnpj_destinatario = None
        else:
            cpf_cnpj_destinatario = None

        # Extrai dados do total (considerando diferentes namespaces)
        total = root.find('.//ns:total/ns:ICMSTot', namespaces) or root.find('.//cte:total/cte:ICMSTot', namespaces)
        if total is not None:
            valor_total_dfe = total.find('.//ns:vNF', namespaces).text if total.find('.//ns:vNF', namespaces) is not None else None
            valor_total_base_icms = total.find('.//ns:vBC', namespaces).text if total.find('.//ns:vBC', namespaces) is not None else None
            valor_total_icms = total.find('.//ns:vICMS', namespaces).text if total.find('.//ns:vICMS', namespaces) is not None else None
            valor_total_pis = total.find('.//ns:vPIS', namespaces).text if total.find('.//ns:vPIS', namespaces) is not None else None
            valor_total_cofins = total.find('.//ns:vCOFINS', namespaces).text if total.find('.//ns:vCOFINS', namespaces) is not None else None
            valor_total_icms_st = total.find('.//ns:vST', namespaces).text if total.find('.//ns:vST', namespaces) is not None else None
            valor_total_base_icms_st = total.find('.//ns:vBCST', namespaces).text if total.find('.//ns:vBCST', namespaces) is not None else None
        else:
            valor_total_dfe = None
            valor_total_base_icms = None
            valor_total_icms = None
            valor_total_pis = None
            valor_total_cofins = None
            valor_total_icms_st = None
            valor_total_base_icms_st = None

        # Extrai a cidade de destino (considerando diferentes namespaces)
        cidade_destino = root.find('.//ns:dest/ns:enderDest/ns:xMun', namespaces).text if root.find('.//ns:dest/ns:enderDest/ns:xMun', namespaces) is not None else None

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

    except Exception as e:
        print(f"Erro ao processar o arquivo '{arquivo_xml}': {e}")
        return None

def validar_chave_acesso(chave_acesso):
    """
    Valida a chave de acesso de acordo com as regras de cada DF-e.

    Args:
        chave_acesso (str): Chave de acesso a ser validada.

    Returns:
        bool: True se a chave de acesso for válida, False caso contrário.
    """

    # Expressões regulares para validar cada tipo de chave de acesso
    padrao_nfe = r"^[0-9]{44}$"
    padrao_cte = r"^[0-9]{44}$"
    padrao_mdfe = r"^[0-9]{44}$"

    # Verifica o tipo de chave de acesso e aplica a validação correspondente
    if len(chave_acesso) == 44:
        if re.match(padrao_nfe, chave_acesso):
            return True  # NF-e válida
        elif re.match(padrao_cte, chave_acesso):
            return True  # CT-e válida
        elif re.match(padrao_mdfe, chave_acesso):
            return True  # MDF-e válida

    return False  # Chave de acesso inválida
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

    lista_arquivos = listar_arquivos_xml(output_dir)
    todos_dados = []
    for arquivo_xml in lista_arquivos: # glob.glob(os.path.join(pasta_xml, '*.xml')):

        #if not validar_schema_xml(arquivo_xml, schema_xsd):
        #    continue  # Ignora arquivos inválidos

        namespaces = {
            'ns': 'http://www.portalfiscal.inf.br/nfe',  # Namespace NF-e
            'nfe': 'http://www.portalfiscal.inf.br/nfe',
            'cte': 'http://www.portalfiscal.inf.br/cte',  # Namespace CT-e
            'mdfe': 'http://www.portalfiscal.inf.br/mdfe'  # Namespace MDF-e
        }

        dados_extraidos = extrair_dados_dfe(arquivo_xml) #, namespaces)

        if dados_extraidos:
            todos_dados.append(dados_extraidos)

    if todos_dados:
        df = pd.DataFrame(todos_dados)
        tabela = pa.Table.from_pandas(df)
        pq.write_table(tabela, arquivo_parquet)
        print(f"Arquivo Parquet '{arquivo_parquet}' criado com sucesso!")
    else:
        print("Nenhum dado extraído dos arquivos XML.")




def listar_arquivos_xml(pasta):
    """
    Lista os arquivos XML em uma pasta e suas subpastas.

    Args:
        pasta (str): Caminho para a pasta.

    Returns:
        list: Lista de caminhos completos para os arquivos XML na pasta e subpastas.
    """
    arquivos_xml = []
    for root, dirs, files in os.walk(pasta):
        for file in files:
            if file.endswith(".xml"):
                arquivos_xml.append(os.path.join(root, file))
    return arquivos_xml




output_dir = os.path.join(os.path.join(os.getcwd(), "temp"))
# Exemplo de uso
pasta_xml = output_dir #'caminho/para/pasta/xml'  # Substitua pelo caminho da sua pasta
arquivo_parquet = 'dados_dfe.parquet'
schema_xsd = 'caminho/para/schema/nfe_v4.00.xsd'  # Substitua pelo caminho do seu schema XSD

output_dir = os.path.join(os.path.join(os.getcwd(), "temp"))
lista_arquivos = listar_arquivos_xml(output_dir)


converter_xml_para_parquet(output_dir, arquivo_parquet) #, schema_xsd)