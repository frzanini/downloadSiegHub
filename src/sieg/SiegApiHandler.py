import os
import time
import json
import base64
import requests
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Union, Optional
from dotenv import load_dotenv
from utils.functions import GerenciadorArquivos
from dfe.DocumentoFiscalParser import DocumentoFiscalParser

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sieg_api.log"),
        logging.StreamHandler()
    ]
)

class XmlType(Enum):
    NFE = 1
    CTE = 2
    NFSE = 3
    NFCE = 4
    CFE = 5

class SiegApiHandler:
    """
    Classe para gerenciar interações com a API da Sieg e processar os arquivos retornados.
    """

    def __init__(self):
        # Carregar variáveis de ambiente
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path=env_path)

        self.api_key = os.getenv("API_KEY")
        self.base_url = os.getenv("URL_BAIXAR_XMLS")

        if not self.api_key or not self.base_url:
            logging.error("API_KEY ou URL_BAIXAR_XMLS não encontrados no arquivo .env.")
            raise ValueError("API_KEY ou URL_BAIXAR_XMLS não encontrados no arquivo .env.")
        
        logging.info("API Handler inicializado com sucesso.")

    def _build_payload(self, xml_type: int, take: int = 50, skip: int = 0, 
                       data_emissao_inicio: Optional[datetime] = None, 
                       data_emissao_fim: Optional[datetime] = None) -> dict:
        """
        Constrói o payload para a chamada à API.
        """
        data_emissao_inicio = data_emissao_inicio or datetime.now()
        data_emissao_fim = data_emissao_fim or datetime.now()

        payload = {
            "XmlType": xml_type,
            "Take": take,
            "Skip": skip,
            "DataEmissaoInicio": data_emissao_inicio.strftime("%Y-%m-%dT00:00:00.000Z"),
            "DataEmissaoFim": data_emissao_fim.strftime("%Y-%m-%dT23:59:59.999Z"),
            "Downloadevent": True
        }
        logging.debug(f"Payload construído: {payload}")
        return payload

    def get_base64_data(self, payload: dict) -> Optional[str]:
        """
        Faz uma chamada à API para obter os dados codificados em Base64.
        """
        url = f"{self.base_url}?api_key={self.api_key}"
        logging.info(f"Enviando requisição para URL: {url}")
        logging.debug(f"Payload: {payload}")

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logging.info(f"Resposta recebida com sucesso: {response.status_code}")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Erro ao se comunicar com a API: {e}")
            return None

    def process_and_save_base64(self, json_data: Union[str, dict, list], output_dir: str):
        """
        Processa o JSON contendo itens Base64 e salva os arquivos decodificados.
        """
        try:
            data = json.loads(json_data) if isinstance(json_data, str) else json_data
            logging.info(f"Dados carregados para processamento. Total de itens: {len(data)}")
        except json.JSONDecodeError:
            logging.error("Dados fornecidos não são um JSON válido.")
            raise ValueError("Dados fornecidos não são um JSON válido.")

        os.makedirs(output_dir, exist_ok=True)
        contador = 1

        for item in data:
            try:
                decoded_content = base64.b64decode(item)
                texto_decodificado = decoded_content.decode('utf-8')

                parserDFe = DocumentoFiscalParser()
                resultado = parserDFe.parse_documento_fiscal_string(texto_decodificado)
                print(resultado)
                continue

                file_name = GerenciadorArquivos.gerar_nome_arquivo_temp(str(contador),"xml")
                file_path = os.path.join(output_dir, file_name)
                with open(file_path, "wb") as xml_file:
                    xml_file.write(decoded_content)
                logging.info(f"Arquivo salvo com sucesso: {file_path}")
                contador += 1
            except (base64.binascii.Error, TypeError) as e:
                # Registrar o tipo de erro e a mensagem associada
                logging.warning(f"Erro ao decodificar o item na posição {contador}: {type(e).__name__} - {e}")


    def download_xmls(self, start_date: datetime, end_date: datetime):
        """
        Faz o download dos arquivos XML para o intervalo de datas fornecido.
        """
        for day_offset in range((end_date - start_date).days + 1):
            current_date = start_date + timedelta(days=day_offset)
            logging.info(f"Processando dia {current_date}...")

            output_dir = os.path.join(
                os.getcwd(), "temp", str(current_date.year), 
                f"{current_date.month:02}", f"{current_date.day:02}"
            )

            os.makedirs(output_dir, exist_ok=True)

            for xml_type in XmlType:
                payload = self._build_payload(xml_type.value, data_emissao_inicio=current_date, data_emissao_fim=current_date)
                logging.debug(f"Payload para {xml_type.name}: {payload}")

                time.sleep(3)
                base64_data = self.get_base64_data(payload)
                if base64_data:
                    self.process_and_save_base64(base64_data, output_dir)
                else:
                    logging.info(f"Nenhum dado encontrado para {current_date} e tipo {xml_type.name}.")


