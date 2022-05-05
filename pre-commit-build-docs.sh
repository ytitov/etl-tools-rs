#!/bin/bash
# add the hook: ln -s -f ../../pre-commit-build-docs.sh .git/hooks/pre-commit
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ $BRANCH == "master" ]; then
  echo "Generating documentation automatically, you may need to add some things to the commit"
  cargo doc --no-deps --release --target-dir docs --package etl-core --package etl-mysql --package etl-aws-utils --package etl-sftp
  cp docs_index.html ./docs/index.html
fi


#git add docs/
#git commit -m "Update documentation ($(date +%F@%R))"
