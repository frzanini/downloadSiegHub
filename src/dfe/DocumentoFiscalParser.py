import xml.etree.ElementTree as ET
import logging
from datetime import datetime
from typing import Dict, Optional, Any
import os
from pathlib import Path
from utils.functions import StringUtils

class DocumentoFiscalParser:
    def __init__(self):
        self.namespaces = {
            'nfe': 'http://www.portalfiscal.inf.br/nfe',    # NF-e namespace
            'nfse': 'http://www.abrasf.org.br/nfse.xsd',    # NFS-e namespace
            'cte': 'http://www.portalfiscal.inf.br/cte',    # CT-e namespace
            'mdfe': 'http://www.portalfiscal.inf.br/mdfe',  # MDF-e namespace
        }
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging with appropriate format and level."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def ler_arquivo_xml(self, caminho_arquivo: str) -> Optional[str]:
            """
            Lê um arquivo XML e retorna seu conteúdo como string.
            
            Args:
                caminho_arquivo: Caminho para o arquivo XML
                
            Returns:
                Optional[str]: Conteúdo do arquivo XML ou None em caso de erro
                
            Raises:
                FileNotFoundError: Se o arquivo não for encontrado
                PermissionError: Se não houver permissão para ler o arquivo
                UnicodeDecodeError: Se houver erro na decodificação do arquivo
            """
            try:
                # Normaliza o caminho do arquivo
                caminho = Path(caminho_arquivo).resolve()
                
                # Verifica se o arquivo existe
                if not caminho.exists():
                    raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
                
                # Verifica se é um arquivo
                if not caminho.is_file():
                    raise ValueError(f"O caminho especificado não é um arquivo: {caminho}")
                
                # Verifica se tem extensão .xml
                if caminho.suffix.lower() != '.xml':
                    raise ValueError(f"O arquivo não tem extensão .xml: {caminho}")
                
                # Verifica o tamanho do arquivo (evita arquivos muito grandes)
                tamanho_max = 10 * 1024 * 1024  # 10MB
                if caminho.stat().st_size > tamanho_max:
                    raise ValueError(f"Arquivo muito grande. Tamanho máximo permitido: {tamanho_max/1024/1024}MB")
                
                # Tenta ler o arquivo com diferentes encodings
                encodings = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1']
                
                for encoding in encodings:
                    try:
                        with open(caminho, 'r', encoding=encoding) as file:
                            conteudo = file.read()
                            logging.info(f"Arquivo XML lido com sucesso usando encoding {encoding}")
                            return conteudo
                    except UnicodeDecodeError:
                        continue
                
                raise UnicodeDecodeError(
                    "Não foi possível decodificar o arquivo com nenhum dos encodings suportados"
                )
                
            except FileNotFoundError as e:
                logging.error(f"Arquivo não encontrado: {e}")
                raise
            except PermissionError as e:
                logging.error(f"Erro de permissão ao ler o arquivo: {e}")
                raise
            except UnicodeDecodeError as e:
                logging.error(f"Erro ao decodificar o arquivo: {e}")
                raise
            except Exception as e:
                logging.exception(f"Erro inesperado ao ler o arquivo XML: {e}")
                raise

    def parse_documento_fiscal_arquivo(self, caminho_arquivo: str) -> Dict[str, Any]:
        """
        Parse um documento fiscal a partir de um arquivo XML.
        
        Args:
            caminho_arquivo: Caminho para o arquivo XML
            
        Returns:
            Dict[str, Any]: Dicionário com os dados do documento fiscal ou erro
        """
        logging.info(f"Iniciando o parse do arquivo: {caminho_arquivo}")
        try:
            xml_string = self.ler_arquivo_xml(caminho_arquivo)
            if xml_string:
                return self.parse_documento_fiscal_string(xml_string)
            else:
                return {'erro': 'Não foi possível ler o conteúdo do arquivo XML'}
                
        except FileNotFoundError:
            return {'erro': f'Arquivo não encontrado: {caminho_arquivo}'}
        except PermissionError:
            return {'erro': f'Sem permissão para ler o arquivo: {caminho_arquivo}'}
        except UnicodeDecodeError:
            return {'erro': 'Erro ao decodificar o arquivo. Encoding não suportado.'}
        except Exception as e:
            logging.exception(f"Erro ao processar o arquivo: {caminho_arquivo}")
            return {'erro': f'Erro ao processar o arquivo: {str(e)}'}

    def parse_documento_fiscal_string(self, xml_string: str) -> Dict[str, Any]:
        """Parse a fiscal document from an XML string."""
        logging.info("Iniciando o parse de um documento fiscal a partir de uma string.")
        try:
            root = self._parse_xml(xml_string)
            tipo_documento = self._identificar_tipo_documento(root)
            
            processors = {
                'NF-e': self._processar_nfe,
                'CT-e': self._processar_nfe,  # Similar structure to NF-e
                'MDF-e': self._processar_nfe, # Similar structure to NF-e
                'NFS-e': self._processar_nfse,
                'Evento': self._processar_evento,
                'procEvento': self._processar_proc_evento
            }
            
            processor = processors.get(tipo_documento)
            if processor:
                logging.info(f"Documento identificado como {tipo_documento}.")
                return processor(root, tipo_documento)
            else:
                logging.warning("Tipo de documento fiscal não identificado.")
                return {'erro': 'Tipo de documento fiscal não identificado'}

        except ET.ParseError as e:
            logging.error(f"Erro ao interpretar o XML: {str(e)}")
            return {'erro': 'Conteúdo fornecido não é um XML válido'}
        except Exception as e:
            logging.exception("Erro inesperado durante o parse do documento fiscal.")
            return {'erro': f'Erro inesperado: {str(e)}'}

    def _parse_xml(self, xml_string: str) -> ET.Element:
        """Parse XML string into ElementTree."""
        logging.debug("Interpretando o XML fornecido.")

        xml_str = StringUtils.remover_caracteres(xml_string,'<','>')

        return ET.fromstring(xml_string)

    def _identificar_tipo_documento(self, root: ET.Element) -> Optional[str]:
        """Identify the type of fiscal document."""
        logging.debug("Identificando o tipo de documento fiscal.")
        tag = root.tag.lower()
        
        tipo_mapping = {
            'proceventonfe': 'procEvento',
            'proceventocte': 'procEvento',
            'proceventomdfe': 'procEvento',
            'eventoproc': 'Evento',
            'evento': 'Evento',
            'cteproc': 'CT-e',
            'cte': 'CT-e',
            'mdfeproc': 'MDF-e',
            'mdfe': 'MDF-e',
            'compnfse': 'NFS-e',
            'nfse': 'NFS-e',
            'nfeproc': 'NF-e',
            'nfe': 'NF-e'
        }
        
        for suffix, doc_type in tipo_mapping.items():
            if tag.endswith(suffix):
                return doc_type
                
        logging.warning(f"Tipo de documento não identificado para a tag: {tag}")
        return None

    def _processar_nfe(self, root: ET.Element, tipo_documento: str) -> Dict[str, Any]:
        """Process NF-e, CT-e, or MDF-e documents."""
        logging.debug(f"Processando {tipo_documento}.")
        try:
            # Define namespace based on document type
            ns = 'nfe' if tipo_documento == 'NF-e' else ('cte' if tipo_documento == 'CT-e' else 'mdfe')
            
            # Extract emitter CNPJ
            cnpj_emitente = root.find(f'.//{ns}:emit/{ns}:CNPJ', self.namespaces)
            if cnpj_emitente is None:
                raise ValueError(f"CNPJ do emitente não encontrado no {tipo_documento}")

            # Extract recipient identification
            destinatario = self._extrair_destinatario(root, ns)
            if destinatario is None:
                raise ValueError(f"Destinatário não encontrado no {tipo_documento}")

            # Extract access key and emission date
            info_tag = f'inf{tipo_documento.replace("-", "")}'
            chave_acesso = root.find(f'.//{ns}:{info_tag}', self.namespaces).get('Id').replace(tipo_documento.replace("-", ""), '')
            data_emissao = root.find(f'.//{ns}:ide/{ns}:dhEmi', self.namespaces).text

            return self._formatar_saida(tipo_documento, cnpj_emitente.text, destinatario, chave_acesso, data_emissao)

        except AttributeError as e:
            logging.error(f"Erro ao processar {tipo_documento}: Estrutura inválida - {str(e)}")
            return {'erro': f'Estrutura do {tipo_documento} inválida'}
        except ValueError as e:
            logging.error(f"Erro ao processar {tipo_documento}: {str(e)}")
            return {'erro': str(e)}
        except Exception as e:
            logging.exception(f"Erro inesperado ao processar {tipo_documento}")
            return {'erro': f'Erro inesperado ao processar {tipo_documento}: {str(e)}'}

    def _processar_nfse(self, root: ET.Element, tipo_documento: str) -> Dict[str, Any]:
        """Process NFS-e documents."""
        logging.debug("Processando NFS-e.")
        try:
            # Extract emitter CNPJ
            cnpj_emitente = root.find('.//nfse:Prestador/nfse:Cnpj', self.namespaces)
            if cnpj_emitente is None:
                raise ValueError("CNPJ do prestador não encontrado na NFS-e")

            # Extract recipient identification
            tomador_cnpj = root.find('.//nfse:Tomador/nfse:IdentificacaoTomador/nfse:Cnpj', self.namespaces)
            tomador_cpf = root.find('.//nfse:Tomador/nfse:IdentificacaoTomador/nfse:Cpf', self.namespaces)
            
            destinatario = tomador_cnpj.text if tomador_cnpj is not None else (
                tomador_cpf.text if tomador_cpf is not None else None
            )

            if destinatario is None:
                raise ValueError("Tomador não possui CNPJ ou CPF válido")

            # Extract service number and emission date
            numero_nfse = root.find('.//nfse:IdentificacaoNfse/nfse:Numero', self.namespaces)
            if numero_nfse is None:
                raise ValueError("Número da NFS-e não encontrado")

            data_emissao = root.find('.//nfse:DataEmissao', self.namespaces)
            if data_emissao is None:
                raise ValueError("Data de emissão não encontrada na NFS-e")

            return self._formatar_saida('NFS-e', cnpj_emitente.text, destinatario, 
                                      numero_nfse.text, data_emissao.text)

        except AttributeError as e:
            logging.error(f"Erro ao processar NFS-e: Estrutura inválida - {str(e)}")
            return {'erro': 'Estrutura da NFS-e inválida'}
        except ValueError as e:
            logging.error(f"Erro ao processar NFS-e: {str(e)}")
            return {'erro': str(e)}
        except Exception as e:
            logging.exception("Erro inesperado ao processar NFS-e")
            return {'erro': f'Erro inesperado ao processar NFS-e: {str(e)}'}

    def _processar_evento(self, root: ET.Element, tipo_documento: str) -> Dict[str, Any]:
        """Process event documents."""
        logging.debug("Processando evento.")
        try:
            ns = 'nfe'  # Default namespace
            
            # Extract required information
            chave_acesso = root.find(f'.//{ns}:chNFe', self.namespaces)
            tipo_evento = root.find(f'.//{ns}:tpEvento', self.namespaces)
            descricao_evento = root.find(f'.//{ns}:xEvento', self.namespaces)
            data_evento = root.find(f'.//{ns}:dhEvento', self.namespaces)

            if None in [chave_acesso, tipo_evento, descricao_evento, data_evento]:
                raise ValueError("Informações obrigatórias do evento não encontradas")

            return {
                'tipo_documento': 'Evento',
                'chave_acesso': chave_acesso.text,
                'tipo_evento': tipo_evento.text,
                'descricao_evento': descricao_evento.text,
                'data_evento': datetime.fromisoformat(data_evento.text).strftime('%Y-%m-%d %H:%M:%S')
            }

        except AttributeError as e:
            logging.error(f"Erro ao processar evento: Estrutura inválida - {str(e)}")
            return {'erro': 'Estrutura do evento inválida'}
        except ValueError as e:
            logging.error(f"Erro ao processar evento: {str(e)}")
            return {'erro': str(e)}
        except Exception as e:
            logging.exception("Erro inesperado ao processar evento")
            return {'erro': f'Erro inesperado ao processar evento: {str(e)}'}

    def _processar_proc_evento(self, root: ET.Element, tipo_documento: str) -> Dict[str, Any]:
        """Process procEvento documents for any DFe type."""
        logging.debug("Processando procEvento.")
        try:
            # Determine the namespace based on the root tag
            ns_map = {
                'proceventonfe': 'nfe',
                'proceventocte': 'cte',
                'proceventomdfe': 'mdfe'
            }

            ns_lookup = root.tag.lower().split('}')[-1]

            ns = ns_map.get(ns_lookup, None)

            # If namespace not found, try to find the most specific namespace in the root element
            if ns is None:
                for ns_prefix, ns_uri in root.nsmap.items():
                    if ns_uri in self.namespaces.values():
                        ns = ns_prefix
                        break

            # Extract event information
            evento = root.find(f'.//{ns}:evento', self.namespaces)
            if evento is None:
                evento = root.find(f'.//{ns}:eventocte', self.namespaces)
                if evento is None:
                    evento = root.find(f'.//{ns}:eventomdf', self.namespaces)
                    if evento is None:
                        raise ValueError("Evento não encontrado no procEvento")

            infEvento = evento.find(f'.//{ns}:infEvento', self.namespaces)
            if infEvento is None:
                raise ValueError("infEvento não encontrado no procEvento")

            # Extract required fields
            campos = {
                'chave_acesso': (f'.//{ns}:chNFe', 'text'),
                'tipo_evento': (f'.//{ns}:tpEvento', 'text'),
                'numero_sequencial': (f'.//{ns}:nSeqEvento', 'text'),
                'cnpj_autor': (f'.//{ns}:CNPJ', 'text'),
                'data_evento': (f'.//{ns}:dhEvento', 'text'),
                'descricao_evento': (f'.//{ns}:xEvento', 'text'),
                'protocolo': (f'.//{ns}:nProt', 'text')
            }

            resultado = {'tipo_documento': 'procEvento'}
            
            for campo, (xpath, attr_type) in campos.items():
                elemento = infEvento.find(xpath, self.namespaces)
                if elemento is not None:
                    valor = getattr(elemento, attr_type)
                    if campo == 'data_evento':
                        valor = datetime.fromisoformat(valor).strftime('%Y-%m-%d %H:%M:%S')
                    resultado[campo] = valor

            # Add processing status
            retEvento = root.find(f'.//{ns}:retEvento', self.namespaces)
            if retEvento is not None:
                resultado['status_processamento'] = retEvento.find(f'.//{ns}:cStat', self.namespaces).text
                resultado['motivo'] = retEvento.find(f'.//{ns}:xMotivo', self.namespaces).text

            return resultado

        except ValueError as e:
            logging.error(f"Erro ao processar procEvento: {str(e)}")
            return {'erro': str(e)}
        except Exception as e:
            logging.exception("Erro inesperado ao processar procEvento")
            return {'erro': f'Erro inesperado ao processar procEvento: {str(e)}'}

    def _extrair_destinatario(self, root: ET.Element, ns: str) -> Optional[str]:
        """Extract recipient identification (CNPJ or CPF) from document."""
        dest_path = f'.//{ns}:dest'
        destinatario = root.find(f'{dest_path}/{ns}:CNPJ', self.namespaces)
        if destinatario is None:
            destinatario = root.find(f'{dest_path}/{ns}:CPF', self.namespaces)
        return destinatario.text if destinatario is not None else None

    
    def _formatar_saida(self, tipo_documento, cnpj_emitente, destinatario, chave_acesso, data_emissao):
        logging.debug("Formatando a saída do documento fiscal.")
        data_emissao_formatada = datetime.fromisoformat(data_emissao).strftime('%Y-%m-%d %H:%M:%S')
        return {
            'tipo_documento': tipo_documento,
            'cnpj_emitente': cnpj_emitente,
            'destinatario': destinatario,
            'chave_acesso': chave_acesso,
            'data_emissao': data_emissao_formatada
        }
    # [Previous methods remain unchanged: parse_documento_fiscal_arquivo, _parse_xml, 
    # _processar_nfse, _processar_evento, _formatar_saida]