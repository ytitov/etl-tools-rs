#!/bin/bash

echo "Generating documentation"
cargo doc --no-deps --workspace --release --target-dir docs
cp docs_index.html ./docs/index.html
git add docs/
git commit -m "Update documentation ($(date +%F@%R))"
