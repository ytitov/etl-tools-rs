#!/bin/python3
import simplestore_bytes_base_server as t_server
import logging
import asyncio

class MySimpleStoreHandler(t_server.HandleReadWrite):
    counter = 0
    async def read(self, key: str, ctx) -> bytes:
        # this value will keep growing as long as the server lives
        self.counter += 1
        logging.info(f"INCOMING: {key}")
        b = bytes("whatever", "utf-8")
        return b

if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    a = MySimpleStoreHandler()
    s = t_server.BytesSimpleStore(handler=a)
    #s.set_string_transform(MyStringHandler())
    s.set_enable_reflection(True)
    s.run('[::]:50051')
