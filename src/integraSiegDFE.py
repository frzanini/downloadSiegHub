import requests
import os
import base64
import json
from dotenv import load_dotenv
import datetime
from enum import Enum
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Union, Optional
from nfelib.nfe.bindings.v4_0.proc_nfe_v4_00 import NfeProc


def preencher_payload(
    xml_type: int = 1,
    take: int = 50,
    skip: int = 0,
    data_emissao_inicio: datetime = None,
    data_emissao_fim: datetime = None,
    download_event: bool = True):
    """
    Preenche um payload JSON com os valores fornecidos.

    Args:
        xml_type: Tipo de XML.
        take: Número de registros a serem retornados. Max 50
        skip: Número de registros a serem puladas.
        data_emissao_inicio: Data de emissão inicial.
        data_emissao_fim: Data de emissão final.
        download_event: Indica se o evento deve ser baixado.

    Returns:
        Um dicionário representando o payload JSON.
    """

    if data_emissao_inicio == None:
        data_emissao_inicio = datetime.datetime.now()

    if data_emissao_fim == None:
        data_emissao_fim = datetime.datetime.now()

    payload = {
        "XmlType": xml_type,
        "Take": take,
        "Skip": skip,
        "DataEmissaoInicio": data_emissao_inicio.strftime("%Y-%m-%d")+"T00:00:00.000Z",
        "DataEmissaoFim": data_emissao_fim.strftime("%Y-%m-%d")+"T23:59:59.999Z",
        "Downloadevent": download_event
    }

    return json.dumps(payload)

def get_base64_from_api(payload: Union[dict, list]) -> Optional[str]:
    """
    Faz uma chamada à API para obter um conteúdo codificado em Base64.

    :param payload: Um dicionário ou lista contendo os dados a serem enviados no corpo da requisição.
    :return: Uma string Base64 em caso de sucesso, ou None em caso de falha.
    :raises ValueError: Se o payload for inválido ou se a variável de ambiente API_KEY não for encontrada.
    """
    # Validar se o payload está vazio ou não é do tipo esperado
    if not payload:
        #not isinstance(payload, (dict, list)) or 
        print(payload)
        print(type(payload))
        raise ValueError("O payload deve ser um dicionário ou lista não vazios.")

    # Parâmetros do JSON
    payload = {
        "XmlType": 1,
        "Take": 50,
        "Skip": 0,
        "DataEmissaoInicio": "2024-12-11T00:00:00.000Z",
        "DataEmissaoFim": "2024-12-11T23:59:59.999Z",
        "Downloadevent": True
    }

    # Carregar as variáveis de ambiente
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    load_dotenv(dotenv_path=env_path)

    # Obter a API_KEY do arquivo .env
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY não encontrada no arquivo .env.")

    # Construir a URL da API
    url = f"https://api.sieg.com/BaixarXmls?api_key={api_key}"

    try:
        # Fazer a requisição POST
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Levantar exceção em caso de erro HTTP

        # Retornar o conteúdo em Base64
        return response.text

    except requests.RequestException as e:
        # Logar ou tratar erro conforme necessário
        print(f"Erro ao se comunicar com a API: {e}")
        print("url:",url)
        print("payload:",payload)
        return None


def process_and_save_base64_json(json_string: str, output_dir: str):
    """
    Processa um JSON contendo itens codificados em Base64, decodifica e salva cada item como um arquivo XML.

    :param json_string: String JSON com vários itens codificados em Base64.
    :param output_dir: Diretório de saída onde os arquivos XML serão salvos.
    :raises ValueError: Se a string JSON for inválida ou se algum item não puder ser decodificado.
    """

    print("1")
    try:
        # Carregar a string JSON em um dicionário/lista
        data = json.loads(json_string)
    except json.JSONDecodeError:
        raise ValueError("A string fornecida não é um JSON válido.")
    
    print("2")
    # Validar se o JSON é uma lista
    if isinstance(data, dict):
        print("O JSON é um dicionário. Chaves disponíveis:")
        print(data.keys())
    elif isinstance(data, list):
        print("O JSON é uma lista. Tamanho:", len(data))
        #print("Primeiro item na lista:", base64_data_json[0] if base64_data_json else "Lista vazia")
    else:
        print("Tipo de dado inesperado:", type(data))

    print("3")
    # Criar o diretório de saída, se não existir
    os.makedirs(output_dir, exist_ok=True)

    for item in data:
        try:
            # Decodificar o item Base64
            decoded_content = base64.b64decode(item)
            
            nfe_proc = NfeProc.from_xml(decoded_content)
            nfe_proc.to_xml()

            

            # Criar o caminho do arquivo XML
            file_path = os.path.join(output_dir, f"item_{nfe_proc.NFe.infNFe.ChNFe + 1}.xml")

            # Salvar o conteúdo decodificado no arquivo XML
            with open(file_path, "wb") as xml_file:
                xml_file.write(decoded_content)

            print(f"Arquivo salvo: {file_path}")

        except (base64.binascii.Error, TypeError):
            print(f"Erro ao decodificar o item na posição {index}.")
            continue


def download_xml_by_sieg(p_data_ini: datetime, p_data_fim: datetime):
    """
    Faz o download de arquivos XML em Base64 a partir da API SIEG, salvando-os em diretórios organizados por data.

    :param p_data_ini: Data inicial do intervalo de busca.
    :param p_data_fim: Data final do intervalo de busca.
    """
    for days_offset in range((p_data_fim - p_data_ini).days + 1):
        # Calcular a data atual no loop
        data_atual = p_data_ini + timedelta(days=days_offset)
        print("processando dia ",data_atual," ... ")

        # Diretório de saída baseado na data
        output_dir = os.path.join(
            os.path.join(os.getcwd(), "temp"),
            str(data_atual.year),
            f"{data_atual.month:02}",
            f"{data_atual.day:02}"
        )

        os.makedirs(output_dir, exist_ok=True)  # Garantir que o diretório existe

        for xml_type in XmlType:
            # Preencher o payload para a requisição
            payload = preencher_payload(xml_type.value, 50, 0, data_atual, data_atual)
            print("payload: ",payload)

            try:
                
                # Obter a resposta Base64 da API
                base64_str = get_base64_from_api(payload)
                #print("base64_str:", base64_str)
                
                # Processar e salvar os arquivos decodificados
                if base64_str:
                    process_and_save_base64_json(base64_str, output_dir)
                else:
                    print(f"Nenhum dado encontrado para {data_atual} e tipo {xml_type.value}.")

            except Exception as e:
                print(f"Erro ao processar {data_atual} para o tipo {xml_type.value}: {e}")


class XmlType(Enum):
    Nfe = 1
    Cte = 2
    Nfse = 3
    Nfce = 4 
    Cfe = 5

download_xml_by_sieg(datetime.now() - timedelta(days=3), datetime.now() - timedelta(days=3))

#print(preencher_payload(XmlType.Nfe.value,50,0))