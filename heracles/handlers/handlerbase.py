# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from thrift.protocol import TJSONProtocol
from thrift.transport import TTransport


class HandlerBase:
    request_mapper = {}

    def __init_subclass__(cls, api_name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        # If API name is not provided, we use a snake-cased version of the class name
        if api_name is None:
            api_name = cls.__name__[0].lower() + cls.__name__[1:]
        cls.request_mapper[api_name] = cls

    @classmethod
    def get_class_for_api_name(cls, api_name):
        if api_name in cls.request_mapper:
            return cls.request_mapper[api_name]
        else:
            raise NotImplementedError

    def encode_response(self, obj):
        trans = TTransport.TMemoryBuffer()
        prot = TJSONProtocol.TJSONProtocolFactory().getProtocol(trans)
        obj.write(prot)

        return trans.getvalue()
