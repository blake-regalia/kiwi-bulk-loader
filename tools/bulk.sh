#!/bin/bash

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# relative to script
cd "${BASH_SOURCE%/*}" || exit

# bulk import all ttl files
pushd ../lib/main
	node --max_old_space_size=8192 bulk.js $(find "$output_dir" -type f | grep "\.ttl$")
popd
