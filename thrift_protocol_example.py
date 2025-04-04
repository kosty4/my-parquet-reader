import sys
sys.path.append("gen-py")
from example_metadata.ttypes import Metadata  # Import generated class

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

def serialize_metadata(metadata_obj):
    transport = TTransport.TMemoryBuffer()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    metadata_obj.write(protocol)  # Serialize
    return transport.getvalue()  # Get binary data

def deserialize_metadata(serialized_data):
    transport = TTransport.TMemoryBuffer(serialized_data)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    metadata = Metadata()
    metadata.read(protocol)  # Deserialize data into Metadata object

    return metadata


# Create metadata object
metadata_obj = Metadata(key="exampleKey", value="exampleValue")

# Serialize
serialized_data = serialize_metadata(metadata_obj)
print('Serialized data ', serialized_data)  # Ensure it's not empty

# Example usage:
# serialized_data = b"\x0b\x00\x01\x00\x00\x00\x03key\x0b\x00\x02\x00\x00\x00\x05value"  # Example binary data
metadata_obj = deserialize_metadata(serialized_data)

print(f"Deserialized:  Key: {metadata_obj.key}, Value: {metadata_obj.value}")
