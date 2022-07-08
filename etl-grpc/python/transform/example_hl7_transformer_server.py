# https://python-hl7.readthedocs.io/en/0.4.3/accessors.html
#!/bin/python3
# documentation: https://python-hl7.readthedocs.io/en/latest/
import hl7
import transform_base_server as t_server
import logging
import asyncio
import json
import traceback

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
        self.application = "APPLICATION"
        self.facility = "FACILITY"
    def __str__(self) -> str:
        ack_msg = self.msg.create_ack(ack_code=self.ack_code,\
                message_id=self.message_id,\
                application=self.application,\
                facility=self.facility)
        try:
            pid_list = []
            pid = {}
            try:
                pid = self.msg.segment('PID')
                pid_3 = pid(3)
                #pid_list = list(map(lambda elem: {"id": str(elem(1)), "oid": str(elem(6)(2)) }, pid_3))
                for idx, s in enumerate(pid_3):
                    item = {}
                    item["id"] = self.msg.extract_field('PID', 1,3,idx+1,1)
                    item["oid"] = self.msg.extract_field('PID', 1,3,idx+1,6)
                    # get pid-3.6.2:  can't do it with the above syntax 
                    item["oi2"] = self.msg['PID.F3.R'+str(idx+1)+'.C6.S2'] 
                    #item["oid"] = self.msg.extract_field('PID', )
                    pid_list.append(item)
            except Exception as e:
                pass

            data = {\
                    "code": self.ack_code,\
                    "msg": str(ack_msg),\
                    #"MSH": str(self.msg.segment('MSH')),\
                    #"patient_identifiers": pid_3,
                    "pid_3_list": pid_list,
                    "pid": pid,
                    }
            return json.dumps(data)
        except Exception as e:
            traceback.print_exc()
            raise e
            return json.dumps({"code": ACK_APPLICATION_ERROR,\
                    "msg": str(self.msg),\
                    "error": str(e)
                    })

    def as_bytes(self) -> bytes:
        return self.__str__().encode('utf8')


class MyBytesHandler(t_server.BytesTransform):
    counter = 0
    async def transform(self, content: bytes, ctx) -> bytes:
        # this value will keep growing as long as the server lives
        self.counter += 1
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
