#!/bin/bash

# resolve absolute path
pushd $1
	output_dir=$(pwd)
popd

# relative to script
cd "${BASH_SOURCE%/*}" || exit

# remove indexes
psql $PGDATABASE < lib/sql/remove-indexes.sql

# invoke bulk and geometry imports
./bulk.sh $output_dir
./geometry.sh $output_dir

# replace indexes
psql $PGDATABASE < lib/sql/add-indexes.sql
