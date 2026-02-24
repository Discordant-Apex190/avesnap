import requests
import os
from tqdm import tqdm
from zipfile import ZipFile

ONE_KB = 1024 
CHUNK_SIZE = ONE_KB * 2000
save_path = "data/backbone.zip"
save_path_for_tsv = "data/VernacularName.tsv"
backbone_data_url = "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip"

os.makedirs(os.path.dirname(save_path), exist_ok=True)

full_path = os.path.abspath(save_path)
dir_save_path = os.path.dirname(save_path)

try:
    with requests.get(backbone_data_url, stream = True) as r:
        total_size = int(r.headers.get('content-length', 0))
        with tqdm(total=total_size, unit='iB', unit_scale=True) as t:
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
                        t.update(len(chunk))
except Exception as e:
    print(f"Error has occured: {e}")

except KeyboardInterrupt as exit:
    print(f"Exiting program {exit}")
    try:
        if os.path.exists(full_path):
            os.remove(full_path)
            os.rmdir(dir_save_path)
            print(f"Directory '{save_path}' and all its contents removed.")
        else:
            print(f"Directory '{save_path}' does not exist.")
    except Exception as e:
        print(f"Error has occured: {e}")


with ZipFile(full_path, 'r') as zObject:
    zObject.extract(
        "VernacularName.tsv", path=save_path_for_tsv)
zObject.close()

print("Removing the zip file now. The tsv is extracted!")
os.remove(full_path)
