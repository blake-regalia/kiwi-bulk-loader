#!/bin/bash

pushd $1
	output_dir=$(pwd)
popd

cd "${BASH_SOURCE%/*}" || exit

pushd ../lib/main
	# bulk import ttl from gnis and nhd
	node bulk.js $output_dir/gnis/*.ttl
	node bulk.js $output_dir/nhd/*.ttl

	# gnis features are stored as wkt
	node geometry.js --wkt $output_dir/gnis/*.tsv

	# nhd features are binary
	node geometry.js $output_dir/nhd/*.tsv
popd
