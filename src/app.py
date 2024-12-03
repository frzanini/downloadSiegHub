from nfelib.nfe.bindings.v4_0.proc_nfe_v4_00 import NfeProc
import os


 # Caminho para salvar o arquivo na pasta \temp
temp_dir = os.path.join(os.getcwd(), "temp")
os.makedirs(temp_dir, exist_ok=True)  # Criar a pasta se ela não existir



nfe_proc = NfeProc.from_path(f"{temp_dir}/resultado.xml")
# (pode ser usado também o metodo from_xml(xml) )

