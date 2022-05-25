import asyncio
import logging
import math
import time
from typing import AsyncIterable, Iterable
import grpc
import os
from pathlib import Path
from grpc_reflection.v1alpha import reflection

from etl_grpc.basetypes import simplestore_error_pb2 as err_pb
from etl_grpc.simplestore import bytes_store_pb2 as pb
from etl_grpc.simplestore import bytes_store_pb2_grpc as pb_grpc

ReadRequest = pb.ReadRequest
ReadResponse = pb.ReadResponse
GrpcSimpleStoreError = err_pb.GrpcSimpleStoreError
NotExistError = err_pb.NotExistError

# see https://github.com/grpc/grpc/blob/master/examples/python/route_guide/asyncio_route_guide_server.py
def create_dir_if_not_exist(p: str):
    if not os.path.exists(p):
        os.makedirs(p)

class HandleReadWrite:
    async def read(self, key: str, ctx) -> bytes:
        raise NotImplementedError("This must be passed to the base server")
    async def write(self, key: str, payload: bytes, ctx) -> str:
        raise NotImplementedError("This must be passed to the base server")

class BytesSimpleStore(pb_grpc.BytesSimpleStoreServicer):
    enable_reflection = False

    def __init__(self, handler = HandleReadWrite(), storage_path = "./storage") -> None:
        self.handler = handler
        self.storage_path = storage_path
        create_dir_if_not_exist(self.storage_path)

    def set_enable_reflection(self, en: bool):
        self.enable_reflection = en

    async def Read(self, r: ReadRequest, ctx) -> ReadResponse:
        fp = os.path.join(self.storage_path, r.key);
        key = ""
        if r.key.startswith(os.path.sep):
           key = r.key.replace(os.path.sep, "", 1) 
        fp = os.path.join(self.storage_path, key);
        print(f"homedir: {self.storage_path}")
        print(f"fullpath: {fp}")
        if not os.path.exists(fp):
            create_dir_if_not_exist(fp)
        parent_path = Path(fp)
        print(f"parent_path = {parent_path}")
        return ReadResponse(key=r.key, \
                    error=GrpcSimpleStoreError(not_exist=NotExistError(key=r.key)))
        output = await self.handler.read(r.key, ctx)
        return ReadResponse(key=r.key, bytes_content = output)

    async def start_server(self, insecure_port: str, secure_port: str = None) -> None:
        server = grpc.aio.server()

        pb_grpc.add_BytesSimpleStoreServicer_to_server( \
                self, server)

        if self.enable_reflection == True:
            logging.info("Enabling reflection")
            SERVICE_NAMES = (
                pb.DESCRIPTOR.services_by_name['BytesSimpleStore'].full_name,
                reflection.SERVICE_NAME,
            )
            reflection.enable_server_reflection(SERVICE_NAMES, server)
        else:
            logging.info("Not enabling reflection")

        if insecure_port is not None:
            logging.info(f"Insecure start at port: {insecure_port}");
            server.add_insecure_port(insecure_port)
        elif secure_port is not None:
            logging.error(f"secure port not implemented");
            raise Exception("secure port not implemented")
        else:
            logging.error(f"Not starting server");
            raise Exception("at least one port must be specified")

        logging.info("query reflection: 'grpcurl -plaintext [::]:50051 describe'")
        logging.info("Call it: 'grpcurl -plaintext [::]:50051 list'")
        logging.info("grpcurl -d '{\"key\": \"somestring\"}' -plaintext [::]:50051 etl_grpc.simplestore.bytes_store.BytesSimpleStore/Read")
        await server.start()
        await server.wait_for_termination()

    def run(self, insecure_port: str, \
            secure_port: str = None, \
            logging_level = logging.INFO ) -> None:
        logging.basicConfig(level=logging_level)
        asyncio.get_event_loop().run_until_complete(self.start_server(insecure_port, secure_port))
