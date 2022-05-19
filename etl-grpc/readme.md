# Python
## Install Toolchain
Install pip
`python -m pip install --upgrade pip`
Install gRPC
`python -m pip install grpcio`
Install gRPC tools
`python -m pip install grpcio-tools`
Reflection API if desired
`python -m pip install grpcio-reflection`
## Generate Stubs
Generate the code used by the server and client, the `I` option adds any protos to include path
`python -m grpc_tools.protoc -I../../protos --python_out=. --grpc_python_out=. ../../protos/helloworld.proto`

## Development
Default docs refer to synchronous API, recommend to use `asyncio` see [server example](https://github.com/grpc/grpc/blob/master/examples/python/route_guide/asyncio_route_guide_server.py)
