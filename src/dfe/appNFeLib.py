import os
from nfelib.nfe.bindings.v4_0.proc_nfe_v4_00 import NfeProc


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

##nfe_proc = NfeProc.from_path("nfelib/nfe/samples/v4_0/leiauteNFe/NFe35200159594315000157550010000000012062777161.xml")

# Ler o XML de uma NF-e:
for arquivo in lista_arquivos:
    
    nfe_proc = NfeProc.from_path(arquivo)
    nfe_proc.to_xml()
    print(nfe_proc.NFe.infNFe.Id) #, nfe_proc.infNFe.emit.xNome, nfe_proc.infNFe.dest.xNome, nfe_proc.infNFe.dest.enderDest.vTotal)


