#!/bin/bash

cd "${BASH_SOURCE%/*}" || exit

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# bulk import all ttl files
pushd ../lib/main
	node --max_old_space_size=8192 bulk.js $output_dir/**/*.ttl
popd
