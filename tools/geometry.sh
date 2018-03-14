#!/bin/bash

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# relative to script
cd "${BASH_SOURCE%/*}" || exit

# bulk import all tsv files
pushd ../lib/main
	node --max_old_space_size=8192 geometry.js $(find "$output_dir" -type f | grep "\.tsv$")
popd
