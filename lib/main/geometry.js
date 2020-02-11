const fs = require('fs');
const path = require('path');
const util = require('util');
const child_process = require('child_process');

require('colors');
const pg = require('pg');
const pg_copy_from = require('pg-copy-streams').from;

const exec = util.promisify(child_process.exec);

// open connection to database
let y_pool = new pg.Pool({
	max: require('os').cpus().length,
	connectionString: process.env.DATABASE_URL,
});


// each file
Promise.all(process.argv.slice(2).map(p_arg => (async () => {
	// open connection to database
	let y_client = await y_pool.connect();

	// resolve tsv file path
	let p_geoms = path.resolve(p_arg);
	let s_basename = path.basename(p_geoms, '.tsv');

	// count how many lines in file
	let {stdout:s_lines} = await exec(`wc -l "${p_geoms}"`);
	let n_lines = +s_lines.trim().replace(/^(\d+).*$/, '$1');

	// print
	console.info(`${s_basename}: updating ${n_lines} geometries...`.blue);

	// create temp table
	let s_table_tmp = 'ins_geoms_'+Math.random().toString(36).slice(2);
	await y_client.query(`create temp table ${s_table_tmp}(svalue text, gvalue geometry)`);

	// load lines into table
	let ds_copy = y_client.query(pg_copy_from(`copy ${s_table_tmp} from stdin`));
	let ds_input = fs.createReadStream(p_geoms);

	await new Promise((fk_input) => {
		ds_input.on('end', fk_input);
		ds_input.pipe(ds_copy);
	});

	// prep geom field cast
	let s_geom_cast = `${s_table_tmp}.gvalue`;

	// do updates
	let n_updates = (await y_client.query(`
		update nodes
		set gvalue = ${s_geom_cast}
		from ${s_table_tmp}
		where nodes.ntype = 'uri'
			and nodes.svalue = ${s_table_tmp}.svalue
	`)).rowCount;

	// make sure they match up
	if(n_updates !== n_lines) {
		// none!
		if(!n_updates) {
			console.error(`${s_basename}: no geometries updated`.red);
		}
		// partial
		else {
			console.warn(`${s_basename}: expected to update ${n_lines} geometries but only succeeded on ${n_updates}`.yellow);
		}
	}
	// print
	else {
		console.log(`${s_basename}: ${n_updates} geometries updated`.green);
	}

	// drop temp table
	await y_client.query(`drop table ${s_table_tmp}`);

	// close connection
	await y_client.release();
})()))
	.then(() => {
		// close pool
		y_pool.end();

		console.log('done'.green);
	});
