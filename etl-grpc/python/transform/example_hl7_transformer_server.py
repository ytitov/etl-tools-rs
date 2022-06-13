#!/bin/python3
# documentation: https://python-hl7.readthedocs.io/en/latest/
import hl7
import transform_base_server as t_server
import logging
import asyncio
import json

from etl_grpc.transformers import transform_pb2 as transform_proto
TransformPayload = transform_proto.TransformPayload
TransformResponse = transform_proto.TransformResponse

ACK_APPLICATION_ACCEPT = "AA"
ACK_APPLICATION_ERROR = "AE"
ACK_APPLICATION_REJECT = "AR"

class Response():
    def __init__(self, msg, ack_code = ACK_APPLICATION_ACCEPT, message_id = None) -> None:
        self.msg = hl7.parse(msg)
        self.ack_code = ack_code
        self.message_id = message_id
        self.application = "KPIPARSER"
        self.facility = "KPI"
    def __str__(self) -> str:
        ack_msg = self.msg.create_ack(ack_code=self.ack_code,\
                message_id=self.message_id,\
                application=self.application,\
                facility=self.facility)
        logging.info(f"{ack_msg}")
        data = {\
                "ackCode": self.ack_code,\
                "ackMessage": str(ack_msg),\
                "originalMessage": str(self.msg)\
                }
        return json.dumps(data)
    def as_bytes(self) -> bytes:
        return self.__str__().encode('utf8')


class MyBytesHandler(t_server.BytesTransform):
    counter = 0
    async def transform(self, content: bytes, ctx) -> bytes:
        # this value will keep growing as long as the server lives
        self.counter += 1
        #logging.info(f"bytes handler INCOMING: {content}")
        return Response(content, message_id = self.counter).as_bytes()

class MyStringHandler(t_server.StringTransform):
    counter = 0
    async def transform(self, content: str, ctx) -> str:
        # this value will keep growing as long as the server lives
        self.counter += 1
        logging.info(f"String handler INCOMING: {content}")
        return f"{self.counter} {content}" 

if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    s = t_server.TransformerServicer(\
            string_transform=MyStringHandler(), \
            bytes_transform=MyBytesHandler())
    s.set_enable_reflection(True)
    #s.run('[::]:50051')
    s.run('0.0.0.0:50051')
