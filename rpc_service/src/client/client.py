import grpc
from google.protobuf.json_format import MessageToDict

import stonkinator_pb2
import stonkinator_pb2_grpc


channel = grpc.insecure_channel('localhost:5000')
stub = stonkinator_pb2_grpc.StonkinatorServiceStub(channel)

req = stonkinator_pb2.InsertTradingSystemRequest(name = "test")
res = stub.InsertTradingSystem(req)
print(res)

req = stonkinator_pb2.InsertTradingSystemMetricsRequest()
res = stub.InsertTradingSystemMetrics(req)
print(res)

# req = gobware_pb2.CheckAccessTokenRequest(encodedToken=access_token)
# res = stub.ParseTokenData(req)
# data_dict = MessageToDict(res)