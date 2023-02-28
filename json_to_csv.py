import argparse
import json
import csv
import concurrent.futures
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Transforme un fichier JSON en CSV')
parser.add_argument('-i', '--input', type=str, required=True, help='Le fichier d\'entrée JSON')
parser.add_argument('-o', '--output', type=str, default='output.csv', help='Le fichier de sortie CSV')
parser.add_argument('-l', '--limit', type=int, help='Le nombre maximum d\'items à extraire du JSON')
parser.add_argument('-w', '--workers', type=int, default=4, help='Le nombre de threads à utiliser')
args = parser.parse_args()

with open(args.input, 'r') as f:
    data = json.load(f)

if args.limit:
    data = data[:args.limit]

keys = set()
print("\n- Extracting keys ..... : ", end='')
for item_keys in data:
    for key in item_keys:
        keys.add(key)
print("Done.\n")

def process_item(item):
    row = {}
    for key in keys:
        row[key] = item.get(key, '')
    return row

with open(args.output, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for item in data:
            future = executor.submit(process_item, item)
            futures.append(future)
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            writer.writerow(future.result())
