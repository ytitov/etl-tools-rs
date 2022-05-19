#!/bin/bash
set -e
DIR_NAME=${PWD##*/}
if [ $DIR_NAME != "python" ]; then
  echo ERROR: ensure that you are running gen-stubs.sh in the same folder because we are relying on relative directories
  kill -INT $$
fi
echo Running in folder name of $DIR_NAME
ROOT_DIR=$(git rev-parse --show-toplevel)
PY_PRJ_DIR=$ROOT_DIR/etl-grpc/python
PROTO_DIR=$ROOT_DIR/etl-grpc/proto
echo "ROOT_DIR=$ROOT_DIR"
#LIB_PATH=transform/lib
#echo "Generating transform libraries.  Grpc lib goes into $LIB_PATH"
#mkdir -p $LIB_PATH
# need to put this so can import
#touch $LIB_PATH."__init__.py"
#python -m grpc_tools.protoc -I$PROTO_DIR --python_out=$PY_PRJ_DIR/transform/lib --grpc_python_out=$PY_PRJ_DIR/transform/lib $PROTO_DIR/datastore.proto
#mkdir -p ./proto
rm ./proto -rf
cp ../proto ./proto -r
python -m grpc_tools.protoc -I proto \
  --python_out=./transform \
  --grpc_python_out=./transform \
  proto/etl_grpc/transformers/transform.proto proto/etl_grpc/basetypes/ds_error.proto

rm ./proto -rf
