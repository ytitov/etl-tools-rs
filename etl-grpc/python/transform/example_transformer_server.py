#!/bin/python3
import transform_base_server as t_server
import logging
import asyncio

from etl_grpc.transformers import transform_pb2 as transform_proto
TransformPayload = transform_proto.TransformPayload
TransformResponse = transform_proto.TransformResponse

class MyStringHandler(t_server.StringTransform):
    counter = 0
    async def transform(self, content: str, ctx) -> str:
        # this value will keep growing as long as the server lives
        self.counter += 1
        logging.info(f"INCOMING: {content}")
        return f"{self.counter} {content}" 

if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    a = MyStringHandler()
    s = t_server.TransformerServicer(string_transform=a)
    #s.set_string_transform(MyStringHandler())
    s.set_enable_reflection(True)
    s.run('[::]:50051')
