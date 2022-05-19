import asyncio
import logging
import math
import time
from typing import AsyncIterable, Iterable
import grpc
from etl_grpc.basetypes import ds_error_pb2 as ds_err_proto
from etl_grpc.transformers import transform_pb2 as transform_proto
from etl_grpc.transformers import transform_pb2_grpc as transform_grpc
from grpc_reflection.v1alpha import reflection

TransformPayload = transform_proto.TransformPayload
TransformResponse = transform_proto.TransformResponse

# see https://github.com/grpc/grpc/blob/master/examples/python/route_guide/asyncio_route_guide_server.py
class StringTransform:
    async def transform(self, payload: str, ctx) -> str:
        raise NotImplementedError("Default StringHandler")

class TransformerServicer(transform_grpc.TransformerServicer):
    enable_reflection = False

    def __init__(self, string_transform = StringTransform()) -> None:
        self.string_transform = string_transform

    def set_enable_reflection(self, en: bool):
        self.enable_reflection = en

    def Transform(self, r: TransformPayload, ctx) -> TransformResponse:
        try:
            if r.string_content is not None:
                result_str = asyncio.run(self.string_transform.transform(r.string_content, ctx))
                return TransformResponse(result=TransformPayload(string_content=result_str))
        except NotImplementedError as ni:
            logging.error("NotImplementedError")
            raise ni
        except Exception as e:
            # should be returning a server error
            print("Got an unexpected error: ")
            print(e)

    async def start_server(self, insecure_port: str, secure_port: str = None) -> None:
        server = grpc.aio.server()

        transform_grpc.add_TransformerServicer_to_server( \
                self, server)

        if self.enable_reflection == True:
            logging.info("Enabling reflection")
            SERVICE_NAMES = (
                transform_proto.DESCRIPTOR.services_by_name['Transformer'].full_name,
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
        logging.info("grpcurl -d '{\"string_content\": \"somestring\"}' -plaintext [::]:50051 etl_grpc.transformers.transform.Transformer/Transform")
        await server.start()
        await server.wait_for_termination()

    def run(self, insecure_port: str, \
            secure_port: str = None, \
            logging_level = logging.INFO ) -> None:
        logging.basicConfig(level=logging_level)
        asyncio.get_event_loop().run_until_complete(self.start_server(insecure_port, secure_port))
