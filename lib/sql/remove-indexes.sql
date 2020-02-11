-- nodes indexes
drop index if exists
	idx_literal_lang,
	idx_node_content,
	idx_node_dcontent,
	idx_node_icontent,
	idx_node_tcontent;

-- nodes constraints
alter table nodes drop constraint nodes_ltype_fkey;

-- triples indexes
drop index if exists
	-- idx_triples_spo,
	idx_triples_cspo,
	idx_triples_p,
	idx_triples_ops,
	idx_triples_pso;

-- triples constraints
alter table triples drop constraint triples_context_fkey;
alter table triples drop constraint triples_creator_fkey;
alter table triples drop constraint triples_object_fkey;
alter table triples drop constraint triples_predicate_fkey;
alter table triples drop constraint triples_subject_fkey;
alter table triples drop constraint triples_check;
