#!/bin/bash

# output dir
pushd $1
	output_dir=$(pwd)
popd

# relative to script
cd "${BASH_SOURCE%/*}" || exit

# invoke bulk and geometry imports
./bulk.sh $output_dir
./geometry.sh $output_dir
