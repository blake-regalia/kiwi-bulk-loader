#!/bin/bash

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# relative to script
cd "${BASH_SOURCE%/*}" || exit

# bulk import all ttl files
pushd ../lib/main
	node geometry.js $output_dir/**/*.tsv
popd
