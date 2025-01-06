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
            "DataEmissaoInicio": data_emissao_inicio.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "DataEmissaoFim": data_emissao_fim.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
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
        logging.info(f"Payload: {payload}")

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
        #output_dir2 = output_dir+"eventos"
        #os.makedirs(output_dir+"\\eventos", exist_ok=True)

        contador = 1

        for item in data:
            try:

                local_dir = output_dir
                #decoded_content = base64.b64decode(item)
                texto_decodificado = base64.b64decode(item).decode('utf-8')

                parserDFe = DocumentoFiscalParser()
                resultado = parserDFe.parse_documento_fiscal_string(texto_decodificado)

                if "cnpj_emitente" in resultado :
                    local_dir = f"{output_dir}\\{resultado["cnpj_emitente"]}"
                    os.makedirs(local_dir, exist_ok=True)
                    os.makedirs(local_dir+"\\eventos", exist_ok=True)

                #print(resultado)
                #continue
                file_name = ""
                if not isinstance(resultado, dict) or "erro" in resultado:
                    file_name = GerenciadorArquivos.gerar_nome_arquivo_temp(str(contador),"xml")
                    logging.error(f"Arquivo sem parse: {file_name}")
                    #resultado['isevent'] = '1'
                elif "isevent" in resultado:
                    if resultado['isevent'] == '1':
                        file_name = f"{resultado["chave_acesso"]}_{resultado["tipo_documento"]}_{resultado["tipo_evento"]}_{resultado["sequencia_evento"]}.xml"
                        file_path = os.path.join(local_dir+"\\eventos", file_name)
                    else:
                        file_name = file_name = f"{resultado["chave_acesso"]}_{resultado["tipo_documento"]}.xml"
                        file_path = os.path.jodin(local_dir, file_name)
                else:
                    file_name = file_name = f"{resultado["chave_acesso"]}_{resultado["tipo_documento"]}.xml"
                    file_path = os.path.join(local_dir, file_name)

                with open(file_path, "wb") as xml_file:
                    xml_file.write(texto_decodificado.encode("utf-8"))
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

            main_dir = os.path.join(
                os.getcwd(), "temp", str(current_date.year), 
                f"{current_date.month:02}", f"{current_date.day:02}"
            )

            os.makedirs(main_dir, exist_ok=True)

            for xml_type in XmlType:

                output_dir = os.path.join(main_dir, xml_type.name)
                os.makedirs(output_dir, exist_ok=True)

                # Início do horário em 00:00
                hora_atual = datetime.strptime("00:00", "%H:%M")
                intervalo = timedelta(hours=1) ## NA VERDADE SÃO 2, MINUTOS=59 SEGUNDOS=59

                # Laço para gerar 12 intervalos
                for i in range(12):
                    #hora_inicio = hora_atual.strptime("00:00", "%H:%M")  # Formata a hora de início
                    #hora_fim = (hora_atual + intervalo - timedelta(minutes=1)).strptime("00:00", "%H:%M")  # Hora final ajustada para 23:59 no último minuto
                    #logging.info(f"Intervalo {i+1}: {hora_inicio} - {hora_fim}")

                    # Concatena as horas à data_base usando replace
                    data_hora_inicio = current_date.replace(hour=hora_atual.hour, minute=hora_atual.minute, second=0)
                    data_hora_fim = current_date.replace(hour=(hora_atual + intervalo).hour, minute=59, second=59)
    
                    payload = self._build_payload(xml_type.value, data_emissao_inicio=data_hora_inicio, data_emissao_fim=data_hora_fim)
                    logging.info(f"Payload para {xml_type.name}: {payload}")

                    time.sleep(3)
                    base64_data = self.get_base64_data(payload)
                    if base64_data:
                        self.process_and_save_base64(base64_data, output_dir)
                    else:
                        logging.info(f"Nenhum dado encontrado para {data_hora_inicio} e {data_hora_fim} e tipo {xml_type.name}.")

                    # Incrementa o horário em 2 horas
                    hora_atual += intervalo


