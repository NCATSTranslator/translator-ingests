import duckdb

db = duckdb.connect(":memory:", read_only=False)
db.execute("""
copy (
with
  hpoa as (select * from read_csv('data/hpoa/phenotype.hpoa')),
  g2p as (select * from read_csv('data/hpoa/genes_to_phenotype.txt'))
select g2p.*, array_to_string(list(hpoa.reference),';') as publications
from g2p
     left outer join hpoa on hpoa.hpo_id = g2p.hpo_id
                 and g2p.disease_id = hpoa.database_id
		             and hpoa.frequency = g2p.frequency
group by all
) to 'data/hpoa/genes_to_phenotype_with_publications.tsv' (delimiter '\t', header true)
""")
