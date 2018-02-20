const fs = require('fs');

const worker = require('worker');
const progress = require('progress');

const F_SUM = (c, x) => c + x;

let k_pool = worker.pool('../workers/upsert.js', {
	node_args: ['--max_old_space_size=8192'],
	inspect: process.execArgv.filter(s => s.startsWith('--inspect')).length
		? {
			brk: true,
			range: [9230, 9242],
		}: {},
});

let a_files = process.argv.slice(2);
let n_files = a_files.length;
let a_sizes = a_files.map(p => fs.statSync(p).size);

let a_spin = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];
let i_spin = 0;
let n_spin = a_spin.length;

let y_bar = new progress('[:bar] :percent :spin :triples triples / :mib_read MiB; +:files_donef; -:files_remainf; +:elapseds; -:etas', {
	incomplete: ' ',
	complete: '∎', // 'Ξ',
	width: 40,
	total: a_sizes.reduce(F_SUM, 0),
});


// start loading
y_bar.start = new Date();
let c_terms = 0;
let c_triples = 0;
let c_files = 0;
let a_locals = Array(n_files).fill(0);
a_files.map((p_input, i_input) => {
	// load
	k_pool.run('load', [p_input], {
		// progress event
		progress(h_progress) {
			// cumulate
			if(!h_progress.initial) {
				let nb_read = h_progress.bytes;
				c_terms += h_progress.new_terms;
				c_triples += h_progress.new_triples;

				a_locals[i_input] = nb_read;
			}

			let nb_progress = a_locals.reduce(F_SUM, 0);

			y_bar.curr = nb_progress;
			y_bar.render({
				files_done: c_files,
				files_remain: n_files - c_files,
				triples: c_triples.toLocaleString(),
				mib_read: (nb_progress / 1048576).toFixed(2),
				spin: a_spin[i_spin++],  // ' ✓ '
			});

			// modulate spinner
			i_spin = i_spin % n_spin;
		},
	}).then(async (h_final) => {
		c_terms += h_final.new_terms;
		c_triples += h_final.new_triples;
		c_files += 1;

		// final update
		let nb_read = y_bar.curr = a_locals.reduce(F_SUM, 0);
		y_bar.render({
			files_done: c_files,
			files_remain: n_files - c_files,
			triples: c_triples.toLocaleString(),
			mib_read: (nb_read / 1048576).toFixed(2),
			spin: ' ✓ ',
		});

		// kill pool
		await k_pool.kill();
	});
});
