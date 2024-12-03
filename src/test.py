import base64
import os
from pathlib import Path

# Obter o diretório atual como um objeto Path
current_dir = Path().resolve()

# Construir o caminho absoluto para a PastaTemp
temp_dir = os.path.join(os.getcwd(), "temp") #(current_dir.parent / 'Temp').resolve()

print(f"temp_dir: {temp_dir}")

# Caminho do arquivo Base64
base64_file_path = temp_dir #'C:\Users\frzanini\Documents\workspace\comapa\downloadSiegHub\temp\resultado.txt'

# Caminho da pasta para salvar os arquivos XML extraídos
output_dir = f"{temp_dir}\resultado.txt"


print(f"output_dir: {output_dir}")

# Verifique se o diretório de saída existe, caso contrário, crie-o
#os.makedirs(output_dir, exist_ok=True)

# Ler o arquivo Base64
with open(output_dir,"r") as base64_file:
    base64_content = base64_file.read()

# Decodificar o Base64 para binário
decoded_data = base64.b64decode(base64_content)

# Agora, você precisa separar os arquivos XML. Supondo que o conteúdo seja múltiplos arquivos XML concatenados
# e cada XML começa com a tag '<?xml' (ou similar), você pode usar isso como base para separar.
# Vamos procurar por essa tag para dividir os arquivos.

xmls = []
start_idx = 0

# Dividir os arquivos baseando na tag '<?xml'
while True:
    start_idx = decoded_data.find(b'<nfeProc', start_idx)
    if start_idx == -1:
        break
    
    end_idx = decoded_data.find(b'</nfeProc>', start_idx)  # Ajuste de acordo com a tag final do XML
    if end_idx == -1:
        break
    
    xml_data = decoded_data[start_idx:end_idx + len('</nfeProc>')]  # Extrair o XML completo
    xmls.append(xml_data)
    start_idx = end_idx

# Salvar cada XML em um arquivo separado
for i, xml_data in enumerate(xmls):
    xml_file_path = os.path.join(output_dir, f'arquivo_{i+1}.xml')
    with open(xml_file_path, 'wb') as xml_file:
        xml_file.write(xml_data)
    print(f"Arquivo XML {i+1} salvo em: {xml_file_path}")
