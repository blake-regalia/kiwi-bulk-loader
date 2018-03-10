#!/bin/bash

cd "${BASH_SOURCE%/*}" || exit

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# bulk import all ttl files
pushd ../lib/main
	node bulk.js $output_dir/**/*.ttl
popd
