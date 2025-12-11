import sys
import json
from collections import defaultdict
from urllib.request import urlopen

feature_map = {
    'agent for': ('biolink:has_chemical_role', 'biolink:chemical_role_of'),
    'aid for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by'),
    'control for': ('biolink:treats', 'biolink:treated_by'),
    'diagnostic for': ('biolink:diagnoses', 'biolink:is_diagnosed_by'),
    'indication for': ('biolink:treats', 'biolink:treated_by'),
    'reversal for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by'),
    'support for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by')
}


def get_molepro_indications(molepro_indications):
    #tsv file with molepro indications
    indications = {}
    with open(molepro_indications, 'r') as f:
        f.readline()  # skip header
        for line in f:
            row = line.strip().split('\t')
            indication = row[0]
            xref = row[1]
            primary_name = row[2]
            feature_action = row[3]
            indications[indication.lower()] = {
                'indication': indication,
                'xref': xref,
                'primary_name': primary_name,
                'feature_action': feature_action,
                'predicate': feature_map.get(feature_action, ('biolink:treats', 'biolink:treated_by'))[0]
            }
    print(f"Loaded {len(indications)} indications from MolePro")
    return indications


def generate_indications_config(filename: str):
    molepro_indications = get_molepro_indications(filename)
    config_indications = json.load(open('src/translator_ingest/ingests/drug_rep_hub/indications_config.json'))
    indications = {}
    drug_rep_hub_file = 'data/drug_rep_hub/v1/source_data/repo-drug-annotation.txt'
    with open(drug_rep_hub_file, 'r') as f:
        for line in f:
            if line.startswith('!'):
                continue
            if line.startswith('pert_iname'):
                continue
            row = line.strip().split('\t')
            if len(row) < 6:
                # print(f"Skipping malformed line: {line.strip()}")
                continue
            pert_iname = row[0] 
            target = row[5]
            if '|' in row[5]:
                indication_list = [ind.strip() for ind in target.strip('"').split('|')]
            elif ',' in row[5]:
                indication_list = [ind.strip() for ind in target.strip('"').split(',')]
            else:
                indication_list = [row[5].strip('"').strip()]
            for indication in indication_list:
                if not indication:
                    continue
                if indication in config_indications:
                    indications[indication] = config_indications[indication]
                elif indication.lower() in molepro_indications:
                    indications[indication] = {
                        'xref': molepro_indications[indication.lower()]['xref'],
                        'primary_name': molepro_indications[indication.lower()]['primary_name'],
                        'feature_action': molepro_indications[indication.lower()]['feature_action'],
                        'predicate': molepro_indications[indication.lower()]['predicate']
                    }
                else:
                    if ',' not in indication:
                        print(f"ERROR: Indication {indication} not found in existing config or MolePro")
                        print(f'  "{indication}": {{')
                        print('    "feature_action": "indication for",')
                        print('    "predicate": "biolink:treats",')
                        print('    "primary_name": "UNKNOWN",')
                        print('    "xref": "UNKNOWN"')
                        print('  },')
    print(f"Identified {len(indications)} indications")
    json.dump(indications, open('src/translator_ingest/ingests/drug_rep_hub/indications_config_new.json', 'w'), indent=2, sort_keys=True)


def get_genes():
    #download gene symbols from HGNC
    URL = "https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json"
    with urlopen(URL) as response:
        data = json.loads(response.read())
        genes = {}
        aliases = defaultdict(set)
        for entry in data['response']['docs']:
            symbol = entry['symbol']
            hgnc_id = entry['hgnc_id']
            genes[symbol] = hgnc_id
            aliases_list = entry.get('alias_symbol', [])
            for alias in aliases_list:
                aliases[alias].add(hgnc_id)
    return genes, aliases
        

def get_molepro_targets(molepro_targets):
    #tsv file with molepro targets
    targets = {}
    with open(molepro_targets, 'r') as f:
        f.readline()  # skip header
        for line in f:
            row = line.strip().split('\t')
            gene_symbol = row[0]
            hgnc_id = row[1]
            targets[gene_symbol] = hgnc_id
    print(f"Loaded {len(targets)} targets from MolePro")
    return targets


def generate_target_config():
    gene_ids, aliases = get_genes()
    if sys.argv.length > 1:
        molepro_targets = sys.argv[1]
        molepro_targets = get_molepro_targets(molepro_targets)
    dru_rep_hub_file = 'data/drug_rep_hub/v1/source_data/repo-drug-annotation.txt'
    targets = {}
    with open(dru_rep_hub_file, 'r') as f:
        for line in f:
            if line.startswith('!'):
                continue
            if line.startswith('pert_iname'):
                continue
            row = line.strip().split('\t')
            if len(row) < 4:
                # print(f"Skipping malformed line: {line.strip()}")
                continue
            pert_iname = row[0] 
            target = row[3]
            target_gene_symbols = [gene.strip() for gene in target.split('|')]
            target_ids = []
            for symbol in target_gene_symbols:
                if not symbol:
                    continue
                if symbol in gene_ids:
                    targets[symbol] = gene_ids[symbol]
                elif symbol in aliases:
                    if len(aliases[symbol]) == 1:
                        targets[symbol] = list(aliases[symbol])[0]
                    else:
                        print(f"Ambiguous gene symbol {symbol} found in HGNC with IDs {aliases[symbol]}")
                        targets[symbol] = list(aliases[symbol])[0]  # pick one arbitrarily
                elif symbol in molepro_targets:
                    targets[symbol] = molepro_targets[symbol]
                    print(f"Using MolePro target for symbol {symbol}: {molepro_targets[symbol]}")
                else:
                    print(f"Gene symbol {symbol} not found in HGNC")
    with open('src/translator_ingest/ingests/drug_rep_hub/target_config.json', 'w') as out_file:
        json.dump(targets, out_file, indent=2, sort_keys=True)


if __name__ == "__main__":
    generate_indications_config(sys.argv[1])