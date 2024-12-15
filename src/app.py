from sieg.SiegApiHandler import *
from utils.functions import *


output_dir = os.path.join(
            os.path.join(os.getcwd(), "temp")
        )
#resultado = GerenciadorArquivos.listar_arquivos_by_path(output_dir)

#print(resultado)


"""for chave, valor in resultado.items():
    print(f"Chave: {chave}, Valor: {valor}")
    for key, value in valor.items():
        print(f"key: {key}")
        print(f"value: {value}")"""

#download_xml_by_sieg(datetime.now() - timedelta(days=3), datetime.now() - timedelta(days=3))

api_handler = SiegApiHandler()
data_inicio = datetime(2024, 12, 9)
data_fim = datetime(2024, 12, 10)

api_handler.download_xmls(data_inicio, data_fim)


# Exemplo de uso
if __name__ == "__main__":
    logging.info("Iniciando execução do script.")
    api_handler = SiegApiHandler()
    data_inicio = datetime(2024, 12, 9)
    data_fim = datetime(2024, 12, 10)

    #api_handler.download_xmls(data_inicio, data_fim)
    logging.info("Execução concluída.")