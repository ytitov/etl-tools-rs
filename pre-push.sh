#!/bin/bash

echo "Generating documentation"
cargo doc --no-deps --workspace --release --target-dir docs --package etl_core --package etl_mysql --package etl_s3
cp docs_index.html ./docs/index.html
git add docs/
git commit -m "Update documentation ($(date +%F@%R))"
