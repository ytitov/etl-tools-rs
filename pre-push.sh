#!/bin/bash

cargo doc --no-deps --workspace --release --target-dir docs
git add docs/
git commit -m "Update documentation ($(date +%F@%R))"
