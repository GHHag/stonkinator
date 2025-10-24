# sterunets

## Python Client Example

requirements.txt
```
pyarrow==20.0.0
pandas==2.3.0
```

client.py
```
import pyarrow.flight


client = pyarrow.flight.FlightClient("grpc+tcp://0.0.0.0:50051")

instrument_id = "instrument1"
trading_system_id = "trading_system_example"

ticket = pyarrow.flight.Ticket(
    f"trading_system:{trading_system_id}:instrument:{instrument_id}".encode())
headers = [
    (b"n-rows", b"10"),
    (b"exclude", b"instrument_id:volume"),
]
call_options = pyarrow.flight.FlightCallOptions(headers=headers)
reader = client.do_get(ticket, options=call_options)

schema = reader.schema
print(f"schema: {schema}\n")

table = reader.read_all()
print(f"table: {table}\n")

df = table.to_pandas()
print(f"df: {df.tail()}")
print(df.shape)
```
