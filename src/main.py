import requests
import os
import base64
import json
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

# Obter a API_KEY do arquivo .env
api_key = os.getenv("API_KEY")

# URL da API
url = f"https://api.sieg.com/BaixarXmls?api_key={api_key}"

# Parâmetros do JSON
payload = {
    "XmlType": 1,
    "Take": 50,
    "Skip": 0,
    "DataEmissaoInicio": "2024-12-11T00:00:00.000Z",
    "DataEmissaoFim": "2024-12-11T23:59:59.999Z",
    "Downloadevent": True
}
#"CnpjEmit": "47488431000102",
# Cabeçalhos
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# Fazer a requisição POST
response = requests.post(url, json=payload) #, headers=headers)

# Verificar o resultado
if response.status_code == 200:
    print("Requisição bem-sucedida!")

    # Obter o conteúdo Base64 retornado
    base64_data =  response.json()  # Supondo que o JSON tenha o Base64 como string principal
    #print("base64_data| type: ", type(base64_data))
    # Decodificar o Base64 para bytes

    base64_data_json = json.loads(base64_data)

    if isinstance(base64_data_json, dict):
        print("O JSON é um dicionário. Chaves disponíveis:")
        print(base64_data_json.keys())
    elif isinstance(base64_data_json, list):
        print("O JSON é uma lista. Tamanho:", len(base64_data_json))
        #print("Primeiro item na lista:", base64_data_json[0] if base64_data_json else "Lista vazia")
    else:
        print("Tipo de dado inesperado:", type(base64_data_json))


    for item in base64_data_json:
        #print("Primeiro item na lista:", base64_data_json[0] if base64_data_json else "Lista vazia")
        #print("Primeiro item na lista:", base64.b64decode(base64_data_json[0]) if base64_data_json else "Lista vazia")
        break


    

    decoded_data = base64.b64decode(base64_data)

    # Caminho para salvar o arquivo na pasta \temp
    temp_dir = os.path.join(os.getcwd(), "temp")
    os.makedirs(temp_dir, exist_ok=True)  # Criar a pasta se ela não existir
    xml_path = os.path.join(temp_dir, "resultado.xml")
    txt_path = os.path.join(temp_dir, "resultado.txt")
    json_path = os.path.join(temp_dir, "resultado.json")

    # Salvar o conteúdo XML no arquivo
    with open(xml_path, "wb") as xml_file:
        xml_file.write(decoded_data)
    
    print("O XML foi salvo no arquivo 'resultado.xml'.")

    with open(txt_path, "w") as txt_file:
        txt_file.write(base64_data)
    
    with open(json_path, "w") as file:
        json.dump(base64_data, file)

    print("O XML foi salvo no arquivo 'resultado.txt'.")

else:
    print(f"Erro: {response.status_code}")
    print("Mensagem de erro:", response.text)
