const fs = require('fs');

require('colors');
const big_integer = require('big-integer');
const pg = require('pg');
// const graphy = require('graphy');
const ttl_read = require('@graphy/content.ttl.read');
const worker = require('worker');

const P_XSD = 'http://www.w3.org/2001/XMLSchema#';
const P_XSD_BOOLEAN = P_XSD+'boolean';
const P_XSD_INTEGER = P_XSD+'integer';
const P_XSD_DOUBLE = P_XSD+'double';
const P_XSD_DECIMAL = P_XSD+'decimal';
const P_XSD_DATE = P_XSD+'date';
const P_XSD_DATETIME = P_XSD+'datetime';
const P_GEOSPARQL_WKT_LITERAL = 'http://www.opengis.net/ont/geosparql#wktLiteral';

const HPF_PARAM = {};
const HPF_STRING = {};
const HPF_PLAIN = {};

const N_TRIPLES_BATCH = process.env.TRIPLES_BATCH? +process.env.TRIPLES_BATCH: 1 << 10;
const N_MAX_DEADLOCK_RETRIES = require('os').cpus().length*3;
const H_RESULT_EMPTY = {rows:[], rowCount:0};

function* snowflake(i_datacenter) {
	let t_prev = 0;

	let ni_dc = 10;
	let ni_seq = 12;

	let i_shift_dc = ni_seq;
	let i_shift_ts = ni_seq + ni_dc;
	let xm_seq = -1 ^ (-1 << ni_seq);

	let i_seq = 0;
	let t_epoch = 1288834974657;

	for(;;) {
		let t_now = Math.max(Date.now(), t_prev+1);

		// protect against system clock change
		if(t_now < t_prev) {
			throw new Error(`system clock appears to have changed while script was running; ${t_now} < ${t_prev}`);
		}

		// same milliseconds as previous id
		if(t_prev === t_now) {
			i_seq = (i_seq + 1) % xm_seq;

			// no sequence number; stall cpu till next time
			if(!i_seq) {
				while(t_now <= t_prev) t_now = Date.now();
			}
		}
		// new milliseconds; reset sequence
		else {
			i_seq = 0;
		}

		// save as previous
		t_prev = t_now;

		// generate id: ((t_now - t_epoch) << i_shift_ts) | (i_datacenter << i_shift_dc);
		yield big_integer(t_now - t_epoch)
			.shiftLeft(i_shift_ts)
			.or(
				big_integer(i_datacenter).shiftLeft(i_shift_dc)
			).or(i_seq).toString();
	}
}

class field_id {
	constructor(fg_snowflake, b_aux=false) {
		Object.assign(this, {
			snowflake: fg_snowflake,
			aux: b_aux,
		});
	}

	next() {
		return this.snowflake.next().value;
	}
} Object.assign(field_id.prototype, {
	type: HPF_STRING,
});

class field_latent {
	constructor(s_field, fk_tuple) {
		Object.assign(this, {
			field: s_field,
			ready: fk_tuple,
			indices: [],
		});
	}
}

class extra_where {
	constructor(w_value) {
		Object.assign(this, {
			value: w_value,
		});
	}

	text(s_key) {
		let w_value = this.value;
		return `${s_key.replace(/_/g, '.')} ${null === w_value? 'is null': `= '${w_value}'`}`;
	}
}

class extra_object {
	text(s_key) {
		return `${s_key.replace(/_/g, '.')} = spo.${s_key}`;
	}
}

class tuples {
	constructor(h_fields, h_extras={}) {
		if(!('where' in h_extras)) h_extras.where = [];
		if(!('object' in h_extras)) h_extras.object = [];

		let h_latents = {};
		for(let s_key in h_fields) {
			let z_field = h_fields[s_key];
			if(z_field instanceof field_latent) {
				h_latents[s_key] = [];
			}
			else if(z_field instanceof extra_where) {
				h_extras.where.push(z_field.text(s_key));
				delete h_fields[s_key];
			}
			else if(z_field instanceof extra_object) {
				h_extras.object.push(z_field.text(s_key));
				delete h_fields[s_key];
			}
		}

		Object.assign(this, {
			index: 0,
			fields: h_fields,
			auxes: [],
			values: [],
			tuples: [],
			extras: h_extras,
		});
	}

	push(h_values) {
		let {
			fields: h_fields,
			values: a_values,
			tuples: a_tuples,
			auxes: a_auxes,
			extras: h_extras,
		} = this;

		// construct tuple from texts
		let a_texts = [];

		// each field
		for(let s_key in h_fields) {
			let z_field = h_fields[s_key];

			// present in values
			if('undefined' !== typeof h_values[s_key]) {
				// value
				let w_value = h_values[s_key];

				// which field
				switch(z_field) {
					case HPF_STRING: a_texts.push("'"+w_value+"'"); break;
					case HPF_PLAIN: a_texts.push(w_value); break;
					case HPF_PARAM: {
						a_texts.push('$'+(++this.index));
						a_values.push(w_value);
						break;
					}
					default: {
						// latent field
						if(z_field instanceof field_latent) {
							a_texts.push('$'+(++this.index));
							let nl_values = a_values.push(w_value);
							z_field.indices.push(nl_values-1);
						}
						// static
						else if('string' === typeof z_field) {
							// they should match
							if(z_field === w_value) {
								a_texts.push("'"+w_value+"'");
							}
							else {
								throw new Error(`field ${s_key} is static, but descriptor value "${w_value}" != "${z_field}"`);
							}
						}
						// usually null (now just as string)
						else if(null === z_field) {
							a_texts.push("'"+w_value+"'");
						}
						else {
							throw new Error('exotic field type: '+z_field);
						}
					}
				}
			}
			// not present
			else {
				// static value
				if('string' === typeof z_field) {
					a_texts.push(`'${z_field}'`);
				}
				// id field
				else if(z_field instanceof field_id) {
					let z_value = z_field.next();

					if(z_field.aux) {
						a_auxes.push(z_value);
					}
					else {
						switch(z_field.type) {
							case HPF_PLAIN: a_texts.push(z_value); break;
							case HPF_STRING: a_texts.push("'"+z_value+"'"); break;
							default: {
								throw new Error('exotic field id type');
							}
						}
					}
				}
				// null value
				else if(null === z_field) {
					a_texts.push('null');
				}
				// empty
				else {
					throw new Error('expected value but encountered undefined at field: '+z_field);
				}
			}
		}

		// first row and types given
		if(!a_tuples.length && h_extras.types) {
			let h_types = h_extras.types;
			let i_column = 0;

			// each field
			for(let s_field in h_fields) {
				let z_field = h_fields[s_field];
				let s_type = h_types[z_field instanceof field_latent? z_field.field: s_field];

				// prepend types to tuple
				a_texts[i_column] = `${a_texts[i_column]}::${s_type}`;

				// advance column
				i_column += 1;
			}
		}

		// finalize texts into tuple
		a_tuples.push('('+a_texts.join(',')+')');
	}

	prepare(k_loader) {
		let {
			fields: h_fields,
			values: a_values,
		} = this;

		// field names
		let a_fields = [];
		for(let s_key in h_fields) {
			let z_field = h_fields[s_key];

			// latent field
			if(z_field instanceof field_latent) {
				// ref latent indices
				let a_indices = z_field.indices;

				// each result row
				a_indices.forEach((i_value) => {
					// apply transform and replace value
					a_values[i_value] = z_field.ready(k_loader, a_values[i_value]);
				});

				// name of deferred field
				a_fields.push(z_field.field);
			}
			// id field w/ aux
			else if(z_field instanceof field_id && z_field.aux) {
				continue;
			}
			// just name of field
			else {
				a_fields.push(s_key);
			}
		}

		return {
			fields: a_fields.join(','),
			tuples: this.tuples.join(','),
			values: this.values,
			auxes: this.auxes,
			extras: this.extras,
		};
	}

	reset() {
		this.index = 0;
		this.tuples.length = 0;
		this.values.length = 0;
		this.auxes.length = 0;
	}
}

class tuples_terms extends tuples {
	constructor(h_fields, h_extras) {
		h_fields = Object.assign({
			svalue: HPF_PARAM,
			ntype: HPF_STRING,
		}, h_fields);

		super(h_fields, h_extras);
	}
}

class tuples_terms_typecast extends tuples_terms {
	constructor(h_fields, h_extras) {
		super(h_fields, Object.assign({
			types: {
				id: 'bigint',
				svalue: 'text',
				ntype: 'nodetype',
				ltype: 'bigint',
				lang: 'character varying(5)',
				bvalue: 'boolean',
				ivalue: 'bigint',
				dvalue: 'double precision',
				tvalue: 'timestamp without time zone',
				tzoffset: 'integer',
			},
		}, h_extras));
	}
}

class tuples_triples_ssn extends tuples {
	constructor(h_fields, h_extras) {
		Object.assign(h_fields, {
			s_svalue: HPF_PARAM,
			s_ntype: HPF_STRING,
			s_ltype: new extra_where(null),
			s_lang: new extra_where(null),
			o_svalue: HPF_PARAM,
		});

		super(h_fields, Object.assign({
			types: {
				id: 'bigint',
				s_svalue: 'text',
				s_ntype: 'nodetype',
				s_ltype: 'bigint',
				s_lang: 'character varying(5)',
				p_id: 'bigint',
				p_svalue: 'text',
				p_ntype: 'nodetype',
				o_svalue: 'text',
				o_ntype: 'nodetype',
				o_ltype: 'bigint',
				o_lang: 'character varying(5)',
			},
		}, h_extras));
	}
}

class tuples_triples_ssn_ps extends tuples_triples_ssn {
	constructor(h_fields, h_extras) {
		Object.assign(h_fields, {
			p_svalue: HPF_PARAM,
			p_ntype: new extra_where('uri'),
			p_ltype: new extra_where(null),
			p_lang: new extra_where(null),
		});

		super(h_fields, h_extras);
	}
}


class injector {
	constructor(k_loader, f_injector) {
		Object.assign(this, {
			loader: k_loader,
			injector: f_injector,
		});
	}

	async eval(kq, b_exact) {
		if(kq.tuples.length) {
			let k_loader = this.loader;
			let h_prepared = kq.prepare(k_loader);
			let n_expect = kq.tuples.length;
			let h_result = await k_loader.query(...this.injector(h_prepared));

			// expect each insertion to be new
			if(b_exact) {
				// nothing was inserted
				if(!h_result.rowCount) {
					debugger;
					console.error(`was expecting ${n_expect} insertions but nothing was successfully inserted!`.red);
				}
				// not fulfilled
				else if(h_result.rowCount !== n_expect) {
					debugger;
					console.warn(`was expecting ${n_expect} insertions but only succeeded on ${h_result.rowCount} rows`.yellow);
				}
			}

			// reset queue
			kq.reset();

			// return results
			return h_result;
		}
		else {
			return H_RESULT_EMPTY;
		}
	}
}


class sync_table_terms_nodes {
	constructor(k_loader) {
		Object.assign(this, {
			loader: k_loader,
			mapped: {},
			pending: new Set(),
		});
	}

	has(p_uri) {
		return (p_uri in this.mapped);
	}

	get(p_uri) {
		return this.mapped[p_uri];
	}

	add(p_uri) {
		this.pending.add(p_uri);
	}

	async sync(s_where, a_upserts=[]) {
		let {
			mapped: h_mapped,
			pending: as_pending,
			loader: k_loader,
		} = this;

		// download rows
		(await k_loader.query(`
			select id, svalue
			from nodes
			where ${s_where}
		`)).rows.forEach((h_row) => {
			// save mappings
			h_mapped[h_row.svalue] = h_row.id;
		});

		// do upserts
		if(a_upserts.length) {
			a_upserts.forEach((p_uri) => {
				if(!(p_uri in h_mapped)) {
					as_pending.add(p_uri);
				}
			});

			// upsert primitive datatypes
			return await this.upsert();
		}
		else {
			return H_RESULT_EMPTY;
		}
	}

	async upsert() {
		let {
			mapped: h_mapped,
			pending: as_pending,
			loader: {
				injector_terms_nodes: ki_terms_nodes,
			},
		} = this;

		// no pending datatypes
		if(!as_pending.size) return H_RESULT_EMPTY;

		// prepare statements
		let kq_terms = new tuples_terms_typecast({
			id: new field_id(this.loader.snowflake),
			ntype: 'uri',
		});

		// each pending datatype
		for(let p_uri of as_pending) {
			kq_terms.push({
				svalue: p_uri,
			});
		}

		// upsert datatypes
		let h_result = await ki_terms_nodes.eval(kq_terms);

		// save mappings
		h_result.rows.forEach((h_row) => {
			h_mapped[h_row.svalue] = h_row.id;
		});

		// remove from pending set
		as_pending.clear();

		return h_result;
	}
}


const XM_TH_TRIPLE_S = 1 << 0;
const XM_TH_TRIPLE_P = 1 << 1;
const XM_TH_TRIPLE_O = 1 << 2;
const XM_TH_PREDICATE_ID = 1 << 3;
const XM_TH_PREDICATE_VALUE = 1 << 4;
const XM_TH_OBJECT_DATATYPE = 1 << 5;

const XM_TH_READY = XM_TH_TRIPLE_S | XM_TH_TRIPLE_P | XM_TH_TRIPLE_O;

class triple_handler {
	constructor(k_loader, y_quad) {
		Object.assign(this, {
			loader: k_loader,
			state: 0,
			triple: y_quad,
			descriptor: {},
		});
	}

	subject(h_term) {
		this.state |= XM_TH_TRIPLE_S;
		Object.assign(this.descriptor, {
			s_svalue: h_term.svalue,
			s_ntype: h_term.ntype,
		});
	}

	predicate_id(si) {
		this.state |= XM_TH_TRIPLE_P | XM_TH_PREDICATE_ID;
		this.descriptor.p_id = si;
	}

	predicate_value(p_uri) {
		this.state |= XM_TH_TRIPLE_P | XM_TH_PREDICATE_VALUE;
		this.descriptor.p_svalue = p_uri;
	}

	object_sn(h_term) {
		this.state |= XM_TH_TRIPLE_O;
		Object.assign(this.descriptor, {
			o_svalue: h_term.svalue,
			o_ntype: h_term.ntype,
		});
	}

	object_datatyped(h_term) {
		this.state |= XM_TH_TRIPLE_O | XM_TH_OBJECT_DATATYPE;
		Object.assign(this.descriptor, {
			o_svalue: h_term.svalue,
			o_ntype: h_term.ntype,
			o_datatype: h_term.datatype,
		});
	}

	object(h_term) {
		this.state |= XM_TH_TRIPLE_O;
		let h_descriptor = Object.assign(this.descriptor, {
			o_svalue: h_term.svalue,
			o_ntype: h_term.ntype,
		});

		if('lang' in h_term) {
			h_descriptor.o_lang = h_term.lang;
		}
		else if('ltype' in h_term) {
			h_descriptor.o_ltype = h_term.ltype;
		}
	}

	check() {
		let {
			state: xm_state,
			descriptor: h_descriptor,
			loader: k_loader,
		} = this;

		// all terms are present; sort triple
		if(XM_TH_READY === (XM_TH_READY & xm_state)) {
			// predicate id
			if(XM_TH_PREDICATE_ID & xm_state) {
				// object has datatype
				if(XM_TH_OBJECT_DATATYPE & xm_state) {
					k_loader.triples_ssn_pi_osd.push(h_descriptor);
				}
				// object is well defined
				else {
					k_loader.triples_ssn_pi_osnlg.push(h_descriptor);
				}
			}
			// predicate value
			else {
				// object has datatype
				if(XM_TH_OBJECT_DATATYPE & xm_state) {
					k_loader.triples_ssn_ps_osd.push(h_descriptor);
				}
				// object is well defined
				else {
					k_loader.triples_ssn_ps_osnlg.push(h_descriptor);
				}
			}
		}
	}
}

class blank_node {
	constructor(kt) {
		// make new set
		// let k_set = graphy.set();

		// save fields
		Object.assign(this, {
			label: kt.concise(),
			// set: k_set,
			handlers: [],
		});
	}

	// add triple to blank node's set
	add(y_quad, k_handler, b_subject=false) {
		// add handler to list
		this.handlers.push([k_handler, b_subject]);

		// add triple to blank node's set
		return this.set.add(y_quad);
	}

	// blank node's contents are finalized
	close(k_loader) {
		// hash blank node's contents
		let p_label = this.set.hash_blank_node(this.label, {});

		// make term
		let h_term = {
			svalue: p_label,
			ntype: 'bnode',
		};

		// add term
		k_loader.terms_sn.push(h_term);

		// handlers
		this.handlers.forEach(([k_handler, b_subject]) => {
			// as subject
			if(b_subject) {
				k_handler.subject(h_term);
			}
			// as object
			else {
				k_handler.object_sn(h_term);
			}

			// check handler
			k_handler.check();
		});
	}
}

class kiwi_loader {
	constructor(i_datacenter) {
		let y_client = new pg.Client({
			connectionString: process.env.DATABASE_URL,
		});
		let fg_snowflake = snowflake(i_datacenter);

		Object.assign(this, {
			client: y_client,
			snowflake: fg_snowflake,

			terms_sn: new tuples_terms({
				id: new field_id(fg_snowflake),
				ltype: null,
			}),

			terms_snlg: new tuples_terms({
				id: new field_id(fg_snowflake),
				ntype: 'string',
				ltype: null,
				lang: null,
			}),

			terms_sd: new tuples_terms({
				id: new field_id(fg_snowflake),
				ntype: 'string',
				datatype: new field_latent('ltype', (k, p) => k.datatypes.get(p)),
			}),

			terms_snidbgtl: new tuples_terms({
				id: new field_id(fg_snowflake),
				ltype: null,
				ivalue: null,
				dvalue: null,
				bvalue: null,
				tvalue: null,
			}),

			triples_ssn_pi_osd: new tuples_triples_ssn({
				id: new field_id(fg_snowflake),
				p_id: HPF_PLAIN,
				o_ntype: new extra_where('string'),
				o_datatype: new field_latent('o_ltype', (k, p) => k.datatypes.get(p)),
				o_lang: new extra_where(null),
			}),

			triples_ssn_pi_osnlg: new tuples_triples_ssn({
				id: new field_id(fg_snowflake),
				p_id: HPF_PLAIN,
				o_ntype: HPF_STRING,
				o_ltype: null,
				o_lang: null,
			}, {
				object: [
					'o.ntype = spo.o_ntype',
					'(o.ltype = spo.o_ltype or (o.ltype is null and spo.o_ltype is null))',
					'(o.lang = spo.o_lang or (o.lang is null and spo.o_lang is null))',
				],
			}),

			triples_ssn_ps_osd: new tuples_triples_ssn_ps({
				id: new field_id(fg_snowflake),
				o_ntype: new extra_where('string'),
				o_datatype: new field_latent('o_ltype', (k, p) => k.datatypes.get(p)),
				o_lang: new extra_where(null),
			}),

			triples_ssn_ps_osnlg: new tuples_triples_ssn_ps({
				id: new field_id(fg_snowflake),
				o_ntype: HPF_STRING,
				o_ltype: null,
				o_lang: null,
			}, {
				object: [
					'o.ntype = spo.o_ntype',
					'(o.ltype = spo.o_ltype or (o.ltype is null and spo.o_ltype is null))',
					'(o.lang = spo.o_lang or (o.lang is null and spo.o_lang is null))',
				],
			}),

			injector_terms: new injector(this, (h) => [
				`
					insert into nodes (${h.fields})
					values ${h.tuples}
					on conflict do nothing;
				`, h.values,
			]),

			injector_terms_nodes: new injector(this, (h) => [
				`
					with inputs(${h.fields}) as (
						values ${h.tuples}
					), inserts as (
						insert into nodes(${h.fields})
						select * from inputs
						on conflict(ntype, svalue)
							where ltype is null and lang is null
							do nothing
						returning id, svalue
					)
					select 'i' as source, id, svalue
						from inserts
						union all
					select 's' as source, n.id, n.svalue
						from inputs
						join nodes n using(svalue, ntype);
				`, h.values,
			]),

			injector_triples_pi: new injector(this, (h) => [
				`
					with spo(${h.fields}) as (values ${h.tuples})
					insert into triples (id, subject, predicate, object)
					select spo.id as id, s.id as subject, spo.p_id as predicate, o.id as object
						from spo
						join nodes s
							on s.ntype = spo.s_ntype and s.svalue = spo.s_svalue
						join nodes o
							on o.svalue = spo.o_svalue
							${h.extras.object.map(s => ' and '+s).join('')}
						where ${h.extras.where.join(' and ')}
					on conflict do nothing;
				`, h.values,
			]),

			injector_triples_ps: new injector(this, (h) => [
				`
					with spo(${h.fields}) as (values ${h.tuples})
					insert into triples (id, subject, predicate, object)
					select spo.id as id, s.id as subject, p.id as predicate, o.id as object
						from spo
						join nodes s
							on s.ntype = spo.s_ntype and s.svalue = spo.s_svalue
						join nodes p
							on p.svalue = spo.p_svalue
						join nodes o
							on o.svalue = spo.o_svalue
							${h.extras.object.map(s => ' and '+s).join('')}
						where ${h.extras.where.join(' and ')}
					on conflict do nothing;
				`, h.values,
			]),

			predicates: new sync_table_terms_nodes(this),
			datatypes: new sync_table_terms_nodes(this),

			blanknodes: {},
		});
	}

	async query(s_sql, a_values, n_retries=0) {
		try {
			return await this.client.query(s_sql, a_values);
		}
		catch(e_query) {
			// deadlock
			if('deadlock detected' === e_query.message) {
				// too many retries
				if(n_retries > N_MAX_DEADLOCK_RETRIES) {
					console.error('retried too many times to avoid deadlock');
					return null;
				}

				// try again
				return await this.query(s_sql, a_values, n_retries+1);
			}
			// some other error
			else {
				console.dir(e_query);
				console.error(`${e_query}`.red);
			}
		}
	}

	async update_tables() {
		let {
			injector_terms: ki_terms,
			injector_triples_pi: ki_triples_pi,
			injector_triples_ps: ki_triples_ps,

			terms_sn: kq_terms_sn,
			terms_sd: kq_terms_sd,
			terms_snlg: kq_terms_snlg,
			terms_snidbgtl: kq_terms_snidbgtl,

			// triples_ssn_pi_osl: kq_triples_ssn_pi_osl,
			triples_ssn_pi_osd: kq_triples_ssn_pi_osd,
			triples_ssn_pi_osnlg: kq_triples_ssn_pi_osnlg,
			// triples_ssn_ps_osl: kq_triples_ssn_ps_osl,
			triples_ssn_ps_osd: kq_triples_ssn_ps_osd,
			triples_ssn_ps_osnlg: kq_triples_ssn_ps_osnlg,
		} = this;


		// await this.datatypes.upsert();
		// await this.predicates.upsert();

		// await ki_terms.eval(kq_terms_sn);
		// await ki_terms.eval(kq_terms_sd);
		// await ki_terms.eval(kq_terms_snlg);
		// await ki_terms.eval(kq_terms_snidbgtl);

		// // await ki_triples_pi.eval(kq_triples_ssn_pi_osl);
		// await ki_triples_pi.eval(kq_triples_ssn_pi_osd);
		// await ki_triples_pi.eval(kq_triples_ssn_pi_osnlg);
		// debugger;
		// // await ki_triples_ps.eval(kq_triples_ssn_ps_osl);
		// await ki_triples_ps.eval(kq_triples_ssn_ps_osd);
		// await ki_triples_ps.eval(kq_triples_ssn_ps_osnlg);


		// terms
		let c_terms_inserted = [
			// upsert datatypes and predicates first
			await this.datatypes.upsert(),
			await this.predicates.upsert(),

			// upsert terms
			await ki_terms.eval(kq_terms_sn),
			await ki_terms.eval(kq_terms_sd),
			await ki_terms.eval(kq_terms_snlg),
			await ki_terms.eval(kq_terms_snidbgtl),
		].reduce((c_terms, h_result) => {
			return h_result.rowCount + c_terms;
		}, 0);

		// upsert triples
		let c_triples_inserted = [
			// await ki_triples_pi.eval(kq_triples_ssn_pi_osl),
			await ki_triples_pi.eval(kq_triples_ssn_pi_osd, true),
			await ki_triples_pi.eval(kq_triples_ssn_pi_osnlg, true),
			// await ki_triples_ps.eval(kq_triples_ssn_ps_osl),
			await ki_triples_ps.eval(kq_triples_ssn_ps_osd, true),
			await ki_triples_ps.eval(kq_triples_ssn_ps_osnlg, true),
		].reduce((c_triples, h_result) => {
			return h_result.rowCount + c_triples;
		}, 0);

		return {
			terms: c_terms_inserted,
			triples: c_triples_inserted,
		};
	}


	push_blank_node(kt, y_quad, k_handler, b_subject) {
		let {
			// terms_sn: kq_terms_sn,
			blanknodes: h_blanknodes,
		} = this;

		// blank node label
		let s_label = kt.value;

		// blank node exists
		if(s_label in h_blanknodes) {
			// add quad to set
			h_blanknodes[s_label].add(y_quad, k_handler, b_subject);
		}
		// blank node not yet exists
		else {
			// create new blank node
			let kt_blank_node = new blank_node(kt);

			// add triple to blank node
			kt_blank_node.add(y_quad, k_handler, b_subject);

			// and save to hash
			h_blanknodes[s_label] = kt_blank_node;
		}
	}

	push_literal(kt, k_handler) {
		let {
			terms_snlg: kq_terms_snlg,
			terms_snidbgtl: kq_terms_snidbgtl,
			datatypes: k_datatypes,
		} = this;

		// assert literal
		if(!kt.isLiteral) throw new Error('exotic type: '+kt.termType);

		let h_term;
		let s_value = kt.value;

		// integer
		if(kt.isInteger) {
			h_term = {
				ntype: 'int',
				svalue: s_value,
				ivalue: kt.number,
				ltype: k_datatypes.get(P_XSD_INTEGER),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// double
		else if(kt.isDouble) {
			h_term = {
				ntype: 'double',
				svalue: s_value,
				dvalue: kt.number,
				ltype: k_datatypes.get(P_XSD_DOUBLE),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// decimal
		else if(kt.isDecimal) {
			h_term = {
				ntype: 'double',
				svalue: s_value,
				dvalue: kt.number,
				ltype: k_datatypes.get(P_XSD_DECIMAL),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// boolean
		else if(kt.isBoolean) {
			h_term = {
				ntype: 'boolean',
				svalue: s_value,
				bvalue: kt.boolean,
				ltype: k_datatypes.get(P_XSD_BOOLEAN),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// languaged
		else if('language' in kt) {
			h_term = {
				ntype: 'string',
				svalue: s_value,
				lang: kt.language,
			};

			// language / ltype term
			kq_terms_snlg.push(h_term);
		}
		// date
		else if(P_XSD_DATE === kt.datatype.value) {
			h_term = {
				ntype: 'date',
				svalue: s_value,
				tvalue: s_value,
				ltype: k_datatypes.get(P_XSD_DATE),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// date time
		else if(P_XSD_DATETIME === kt.datatype.value) {
			h_term = {
				ntype: 'date',
				svalue: s_value,
				tvalue: s_value,
				ltype: k_datatypes.get(P_XSD_DATETIME),
			};

			// all columns term
			kq_terms_snidbgtl.push(h_term);
		}
		// datatyped
		else if(kt.hasOwnProperty('datatype')) {
			let p_datatype = kt.datatype.value;

			// datatype already exists
			if(k_datatypes.has(p_datatype)) {
				h_term = {
					ntype: 'string',
					svalue: s_value,
					ltype: k_datatypes.get(p_datatype),
				};

				// language / ltype term
				kq_terms_snlg.push(h_term);
			}
			// need to fetch it
			else {
				// add datatype to pending
				k_datatypes.add(p_datatype);

				// make term
				h_term = {
					ntype: 'string',
					svalue: s_value,
					datatype: p_datatype,
				};

				// add term to appropriate nodes queue
				this.terms_sd.push(h_term);

				// datatyped
				return k_handler.object_datatyped(h_term);
			}
		}
		// plain
		else {
			h_term = {
				ntype: 'string',
				svalue: s_value,
			};

			// plain term
			this.terms_sn.push(h_term);
		}

		// save to triple handler
		k_handler.object(h_term);
	}

	async load_ttl(p_file, k_dedicated) {
		let {
			terms_sn: kq_terms_sn,
			predicates: k_predicates,
			blanknodes: h_blanknodes,

			client: y_client,
		} = this;

		let k_self = this;

		// initial progress event
		k_dedicated.emit('progress', {
			bytes: 0,
			terms: 0,
			triples: 0,
			initial: true,
		});

		// connect
		await y_client.connect();

		// sync predicates and datatypes
		await this.predicates.sync('id in (select distinct(predicate) from triples)');
		await this.datatypes.sync('id in (select distinct(ltype) from nodes)', [
			// primitives
			P_XSD_BOOLEAN,
			P_XSD_INTEGER,
			P_XSD_DOUBLE,
			P_XSD_DECIMAL,
			P_XSD_DATE,
			P_XSD_DATETIME,
			P_GEOSPARQL_WKT_LITERAL,
		]);

		// pointer to previous subject term
		let kt_subject_prev = null;
		let h_term_subject_prev;

		let c_triples = 0;
		let c_terms_inserted = 0;
		let c_triples_inserted = 0;

		// parsing
		return new Promise((fk_parse) => {
			// open read stream on input file
			let ds_input = fs.createReadStream(p_file);
			ttl_read(ds_input, {
				data(y_quad) {
					let {
						subject: kt_subject,
						predicate: {
							value: p_predicate,
						},
						object: kt_object,
					} = y_quad;

					// triple handler
					let k_handler = new triple_handler(k_self, y_quad);


					// new subject
					if(kt_subject_prev !== kt_subject) {
						// previous subject was blank node
						if(kt_subject_prev && kt_subject_prev.isBlankNode) {
							// an anonymous blank node
							if(kt_subject_prev.isAnonymous) {
								// it is now self-contained
								h_blanknodes[kt_subject_prev.value].close(k_self);
							}
							else {
								throw new Error('bulk importing labeled blank nodes is not yet supported');
							}
						}

						// new subject is namednode
						if(kt_subject.isNamedNode) {
							// make term
							h_term_subject_prev = {
								svalue: kt_subject.value,
								ntype: 'uri',
							};

							// add term to nodes table
							kq_terms_sn.push(h_term_subject_prev);

							// for triple
							k_handler.subject(h_term_subject_prev);
						}
						// blanknode
						else {
							// push blank node
							k_self.push_blank_node(kt_subject, y_quad, k_handler, true);
						}

						// update subject pointer
						kt_subject_prev = kt_subject;
					}
					// same subject
					else {
						// blank node
						if(kt_subject.isBlankNode) {
							// push blank node
							k_self.push_blank_node(kt_subject, y_quad, k_handler, true);
						}
						// named node
						else {
							// for triple
							k_handler.subject(h_term_subject_prev);
						}
					}


					// predicate is already mapped
					if(k_predicates.has(p_predicate)) {
						k_handler.predicate_id(k_predicates.get(p_predicate));
					}
					// term not yet mapped
					else {
						// add to pending
						k_predicates.add(p_predicate);

						// for triple
						k_handler.predicate_value(p_predicate);
					}


					// object is blank node
					if(kt_object.isBlankNode) {
						throw new Error('blank nodes temporarily not supported :P');
						// k_self.push_blank_node(kt_object, y_quad, k_handler, false);
					}
					// object is named node
					else if(kt_object.isNamedNode) {
						// make term
						let h_term = {
							svalue: kt_object.value,
							ntype: 'uri',
						};

						// add to table
						kq_terms_sn.push(h_term);

						// for triple
						k_handler.object_sn(h_term);
					}
					// object is literal
					else {
						k_self.push_literal(kt_object, k_handler);
					}

					// check handler
					k_handler.check();


					// time for an update
					if(0 === ++c_triples % N_TRIPLES_BATCH) {
						// pause parse stream
						this.pause();

						// do updates
						k_self.update_tables().then((h_counts) => {
							let {
								terms: c_terms_upserted,
								triples: c_triples_upserted,
							} = h_counts;

							// increment counts
							c_terms_inserted += c_terms_upserted;
							c_triples_inserted += c_triples_upserted;

							// emit progress event
							k_dedicated.emit('progress', {
								bytes: ds_input.bytesRead,
								new_terms: c_terms_upserted,
								new_triples: c_triples_upserted,
								terms: c_terms_inserted,
								triples: c_triples_inserted,
							});

							// // print
							// console.log(`+ ${c_terms_upserted} new terms; + ${c_triples_upserted} new triples`.green);

							// resume parse stream
							this.resume();
						});
					}
				},

				eof() {
					// console.log('<<EOF>>');

					setTimeout(async () => {
						if(kt_subject_prev && kt_subject_prev.isBlankNode) {
							// an anonymous blank node
							if(kt_subject_prev.isAnonymous) {
								// it is now self-contained
								h_blanknodes[kt_subject_prev.value].close(k_self);
							}
							else {
								throw new Error('bulk importing labeled blank nodes is not yet supported');
							}
						}

						// finish updates
						let {
							terms: c_terms_upserted,
							triples: c_triples_upserted,
						} = await k_self.update_tables();

						// increment counts
						c_terms_inserted += c_terms_upserted;
						c_triples_inserted += c_triples_upserted;

						let h_final_update = {
							bytes: ds_input.bytesRead,
							new_terms: c_terms_upserted,
							new_triples: c_triples_upserted,
							terms: c_terms_inserted,
							triples: c_triples_inserted,
						};

						// progress
						k_dedicated.emit('progress', h_final_update);

						// // print
						// console.log(`+ ${c_terms_upserted} new terms; + ${c_triples_upserted} new triples`.yellow);

						// console.log(`${c_triples} parsed from file; ${c_triples_inserted} inserted`.blue);

						// release client
						y_client.end();

						// done
						fk_parse(h_final_update);
					}, 100);
				},
			});
		});
	}
}


worker.dedicated({

	async load(p_file) {
		// worker index
		let i_worker = +(process.env.WORKER_INDEX);

		// make new loader
		let k_loader = new kiwi_loader(i_worker);

		// wait til its done
		return await k_loader.load_ttl(p_file, this);
	},
});

