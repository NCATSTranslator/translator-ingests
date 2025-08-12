import duckdb

db = duckdb.connect(":memory:", read_only=False)
db.execute("""
copy (
with
  hpoa as (select * from read_csv('data/phenotype.hpoa')),
  g2p as (select * from read_csv('data/genes_to_phenotype.txt')),
  g2d as (select 
    replace(ncbi_gene_id, 'NCBIGene:', '') as ncbi_gene_id_clean,
    disease_id, 
    association_type 
    from read_csv('data/genes_to_disease.txt')),
  g2d_grouped as (select 
    ncbi_gene_id_clean,
    disease_id,
    array_to_string(list(distinct association_type), ';') as association_types
    from g2d 
    group by ncbi_gene_id_clean, disease_id)
select g2p.*, 
       array_to_string(list(hpoa.reference),';') as publications,
       coalesce(g2d_grouped.association_types, '') as gene_to_disease_association_types
from g2p
     left outer join hpoa on hpoa.hpo_id = g2p.hpo_id
                 and g2p.disease_id = hpoa.database_id
		             and hpoa.frequency = g2p.frequency
     left outer join g2d_grouped on g2p.ncbi_gene_id = g2d_grouped.ncbi_gene_id_clean
                 and g2p.disease_id = g2d_grouped.disease_id
group by all
) to 'data/genes_to_phenotype_preprocessed.tsv' (delimiter '\t', header true)
""")
