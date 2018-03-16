
create unique index idx_node_essence_ns on nodes(ntype, svalue)
	where ltype is null
		and lang is null;

create unique index idx_node_essence_gs on nodes(lang, svalue)
	where ntype = 'string'
		and ltype is null
		and lang is not null;

create unique index idx_node_essence_nls on nodes(ntype, ltype, svalue)
	where ltype is not null
		and lang is null;


alter table triples add constraint triples_unique unique(subject, predicate, object);

create index idx_triples_ops on triples(object, predicate, subject)
	where deleted = false;

create index idx_triples_pso on triples(predicate, subject, object)
	where deleted = false;
