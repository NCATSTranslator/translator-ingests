"""
Preprocessing script for DrugCentral ingest.

This script:
1. Downloads the PostgreSQL dump file (if needed)
2. Connects to an existing DrugCentral PostgreSQL database
3. Queries the database to extract relevant data
4. Outputs TSV files for Koza to transform

Note: This script is not currently used by the main ingest pipeline.
The main drugcentral.py ingest connects directly to the database.
"""

import os
import psycopg2
import psycopg2.extras
import csv
from pathlib import Path
from loguru import logger
import requests

# Constants
DRUGCENTRAL_DUMP_URL = 'https://unmtid-shinyapps.net/download/drugcentral.dump.11012023.sql.gz'
DRUGCENTRAL_PROVENANCE_ID = 'infores:drugcentral'

# Predicate mappings
OMOP_RELATION_MAP = {
    'off-label use': 'biolink:applied_to_treat',
    'reduce risk': 'biolink:preventative_for_condition', 
    'contraindication': 'biolink:contraindicated_in',
    'symptomatic treatment': 'biolink:treats',
    'indication': 'biolink:treats',
    'diagnosis': 'biolink:diagnoses'
}

# Knowledge source mappings for bioactivity
ACT_TYPE_TO_KNOWLEDGE_SOURCE_MAP = {
    'IUPHAR': 'infores:gtopdb',
    'KEGG DRUG': 'infores:kegg',
    'PDSP': 'infores:pdsp',
    'CHEMBL': 'infores:chembl',
    'DRUGBANK': 'infores:drugbank'
}

# Filter out semantic types that don't represent diseases/conditions
EXCLUDED_STYS = [
    'T002', 'T007', 'T034', 'T040', 'T042', 'T058', 'T059', 'T060', 'T061',
    'T109', 'T121', 'T130', 'T131', 'T167'
]


class DrugCentralPreprocessor:
    """Preprocessor for DrugCentral data."""
    
    def __init__(self, output_dir: str):
        """Initialize preprocessor with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_connection = None
        
    def download_dump(self) -> Path:
        """Download DrugCentral PostgreSQL dump file."""
        dump_path = self.output_dir / 'drugcentral.dump.sql.gz'
        
        if dump_path.exists():
            logger.info(f"Dump file already exists at {dump_path}")
            return dump_path
            
        logger.info(f"Downloading DrugCentral dump from {DRUGCENTRAL_DUMP_URL}")
        response = requests.get(DRUGCENTRAL_DUMP_URL, stream=True)
        response.raise_for_status()
        
        with open(dump_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Downloaded dump to {dump_path}")
        return dump_path
        
    def connect_to_db(self) -> psycopg2.extensions.connection:
        """Connect to DrugCentral database (assumes it's already loaded)."""
        # Try environment variables first
        try:
            db_host = os.environ.get('DRUGCENTRAL_DB_HOST', 'unmtid-dbs.net')
            db_user = os.environ.get('DRUGCENTRAL_DB_USER', 'drugman')
            db_password = os.environ.get('DRUGCENTRAL_DB_PASSWORD', 'dosage')
            db_name = os.environ.get('DRUGCENTRAL_DB_NAME', 'drugcentral')
            db_port = os.environ.get('DRUGCENTRAL_DB_PORT', '5433')
            
            logger.info(f"Connecting to DrugCentral database at {db_host}:{db_port}")
            
            connection = psycopg2.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name,
                port=db_port
            )
            
            logger.info("Successfully connected to DrugCentral database")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def extract_chemical_phenotype_data(self, cursor) -> None:
        """Extract chemical-phenotype relationships."""
        logger.info("Extracting chemical-phenotype relationships")
        
        excluded_stys_sql = ', '.join(f"'{sty}'" for sty in EXCLUDED_STYS)
        query = f'''
            SELECT struct_id, relationship_name, umls_cui, cui_semantic_type, snomed_conceptid, snomed_full_name
            FROM public.omop_relationship
            WHERE umls_cui IS NOT NULL
            AND (cui_semantic_type IS NULL OR cui_semantic_type NOT IN ({excluded_stys_sql}))
        '''
        
        cursor.execute(query)
        
        output_path = self.output_dir / 'chemical_phenotype.tsv'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            # Write header
            writer.writerow([
                'struct_id', 'relationship_name', 'umls_cui', 
                'cui_semantic_type', 'snomed_conceptid', 'snomed_full_name'
            ])
            
            # Write data
            for row in cursor:
                writer.writerow([
                    row['struct_id'],
                    row['relationship_name'],
                    row['umls_cui'],
                    row['cui_semantic_type'] or '',
                    row['snomed_conceptid'] or '',
                    row['snomed_full_name'] or ''
                ])
                
        logger.info(f"Wrote chemical-phenotype data to {output_path}")
        
    def extract_adverse_events(self, cursor) -> None:
        """Extract adverse event data from FAERS."""
        logger.info("Extracting adverse events from FAERS")
        
        query = '''
            SELECT struct_id, meddra_code, llr 
            FROM public.faers 
            WHERE llr > llr_threshold 
            AND drug_ae > 25
        '''
        
        cursor.execute(query)
        
        output_path = self.output_dir / 'adverse_events.tsv'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            # Write header
            writer.writerow(['struct_id', 'meddra_code', 'llr'])
            
            # Write data
            for row in cursor:
                writer.writerow([row['struct_id'], row['meddra_code'], row['llr']])
                
        logger.info(f"Wrote adverse events data to {output_path}")
        
    def extract_bioactivity_data(self, cursor) -> None:
        """Extract bioactivity data."""
        logger.info("Extracting bioactivity data")
        
        query = '''
            SELECT a.struct_id, a.act_value, a.act_unit, 
                   a.act_type, a.act_source, a.act_source_url, 
                   a.action_type, dc.component_id, c.accession
            FROM public.act_table_full a
            JOIN public.td2tc dc ON a.target_id = dc.target_id
            JOIN public.target_component c ON dc.component_id = c.id
        '''
        
        cursor.execute(query)
        
        output_path = self.output_dir / 'bioactivity.tsv'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            # Write header
            writer.writerow([
                'struct_id', 'act_value', 'act_unit', 'act_type',
                'act_source', 'act_source_url', 'action_type',
                'component_id', 'accession'
            ])
            
            # Write data
            for row in cursor:
                writer.writerow([
                    row['struct_id'],
                    row['act_value'] or '',
                    row['act_unit'] or '',
                    row['act_type'] or '',
                    row['act_source'] or '',
                    row['act_source_url'] or '',
                    row['action_type'] or '',
                    row['component_id'],
                    row['accession']
                ])
                
        logger.info(f"Wrote bioactivity data to {output_path}")
        
    def extract_structure_data(self, cursor) -> None:
        """Extract chemical structure information."""
        logger.info("Extracting chemical structure data")
        
        query = '''
            SELECT id, inchikey, smiles, cd_formula, cd_molweight
            FROM public.structures
        '''
        
        cursor.execute(query)
        
        output_path = self.output_dir / 'structures.tsv'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            # Write header
            writer.writerow(['struct_id', 'inchikey', 'smiles', 'formula', 'molecular_weight'])
            
            # Write data
            for row in cursor:
                writer.writerow([
                    row['id'],
                    row['inchikey'] or '',
                    row['smiles'] or '',
                    row['cd_formula'] or '',
                    row['cd_molweight'] or ''
                ])
                
        logger.info(f"Wrote structure data to {output_path}")
        
    def preprocess(self):
        """Run the complete preprocessing pipeline."""
        try:
            # Download dump if needed
            self.download_dump()
            
            # Connect to database
            connection = self.connect_to_db()
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Extract data to TSV files
            self.extract_chemical_phenotype_data(cursor)
            self.extract_adverse_events(cursor)
            self.extract_bioactivity_data(cursor)
            self.extract_structure_data(cursor)
            
            # Close database connection
            cursor.close()
            connection.close()
            
            logger.info("Preprocessing completed successfully")
            
        except Exception as e:
            logger.error(f"Preprocessing failed: {e}")
            raise


def main():
    """Run preprocessing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Preprocess DrugCentral data for Koza ingest')
    parser.add_argument('--output-dir', required=True, help='Output directory for TSV files')
    args = parser.parse_args()
    
    # Run preprocessing
    preprocessor = DrugCentralPreprocessor(args.output_dir)
    preprocessor.preprocess()


if __name__ == '__main__':
    main()