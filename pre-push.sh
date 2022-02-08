#!/bin/bash

echo "Generating documentation"
cargo doc --no-deps --release --target-dir docs --package etl-core --package etl-mysql --package etl-aws-utils --package etl-sftp
cp docs_index.html ./docs/index.html
git add docs/
git commit -m "Update documentation ($(date +%F@%R))"
