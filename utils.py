import struct


from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


def read_parquet_metadata(parquet_file_path):
    with open(parquet_file_path, "rb") as f:

        f.seek(-8, 2)  # Seek to the last 8 bytes
        metadata_len_bytes = f.read(4)  # Read 4 bytes from last 8 bytes

        metadata_len = struct.unpack("<I", metadata_len_bytes)[0]
        # print('metadata length ', metadata_len)

        f.seek(-metadata_len-8,2)

        # read the part of metadata
        metadata_actual = f.read(metadata_len)
        
        return metadata_actual
    
def deserialize_metadata(serialized_data, FileMetaData):
    # Deserialize using Thrift Compact Protocol
    transport = TTransport.TMemoryBuffer(serialized_data)
    protocol = TCompactProtocol.TCompactProtocol(transport)

    metadata = FileMetaData()
    metadata.read(protocol)  # Deserialize data into Metadata object

    return metadata



def decode_rle_dictionary(data, dictionary):
    """Decode RLE_DICTIONARY encoded byte stream."""
    
    # Step 1: Read the bit width (first byte)
    bit_width = data[0]  # First byte stores bit width
    data = data[1:]  # Remove bit width byte

    # Ensure bit width is valid
    if bit_width > 32:
        raise ValueError(f"Invalid bit width: {bit_width}")

    indices = []
    offset = 0

    while offset < len(data):
        # Read control header (varint format) - first byte tells us mode
        control_byte = data[offset]
        offset += 1

        if control_byte & 1 == 0:  # RLE Mode (even control byte)
            run_length = control_byte >> 1  # Divide by 2 to get count
            value_bytes = data[offset:offset + 4]  # Read packed int32
            offset += 4
            value = struct.unpack('<i', value_bytes)[0]  # Little-endian int32

            indices.extend([value] * run_length)  # Repeat the value

        else:  # Bit-Packing Mode (odd control byte)
            num_groups = control_byte >> 1  # Number of 8-value groups
            total_values = num_groups * 8
            bit_mask = (1 << bit_width) - 1  # Mask for bit extraction

            bitstream = int.from_bytes(data[offset:offset + (bit_width * total_values + 7) // 8], 'little')
            offset += (bit_width * total_values + 7) // 8  # Move past read bytes

            for i in range(total_values):
                extracted_index = (bitstream >> (i * bit_width)) & bit_mask
                indices.append(extracted_index)

    # Step 3: Map indices to dictionary values
    decoded_values = [dictionary[i] for i in indices]
    return decoded_values

def decode_column_data(column_data, encoding, num_bytes=8, dicitionary=None):
    """
    Decode the column data based on the encoding type.
    For simplicity, we assume the PLAIN encoding for now.
    """
    if encoding == "PLAIN":
        return decode_plain_data(column_data, num_bytes=num_bytes)
    elif encoding == "RLE_DICTIONARY":
        return decode_rle_dictionary(column_data, dicitionary)
    else:
        raise NotImplementedError(f"Encoding {encoding} not supported.")
    
def decode_plain_data(column_data, num_bytes):
    """
    This function decodes column data with PLAIN encoding.
    The data may need to be deserialized based on its type (e.g., INT32, INT64, FLOAT).
    For simplicity, we assume the data is in a simple format, such as a series of integers.
    """
    # Example: Assuming the column data is a sequence of little-endian INT64 values
    num_values = len(column_data) // num_bytes
    values = struct.unpack(f"<{num_values}Q", column_data)
    return values

def parse_byte_array_string(encoded_string):
    """
    Parse a Parquet BYTE_ARRAY encoded string into a list of byte arrays.
    
    Args:
        encoded_string (bytes): The raw encoded string from the Parquet page
    
    Returns:
        list: A list of decoded byte arrays
    """
    values = []
    i = 0
    
    while i < len(encoded_string):
        # Read the length (4 bytes, little-endian)
        length = int.from_bytes(encoded_string[i:i+4], byteorder='little')
        i += 4
        
        # Extract the byte array based on the length
        value = encoded_string[i:i+length]
        values.append(value)
        
        # Move to the next byte array
        i += length
    
    return values

# print('plain ', decode_plain_data(b'\x15\x00\x15<\x15<'))
# print('plain ', decode_plain_data(b'\x15\x06\x15\x00\x15\x06\x15\x06\x1c\x18\x08\xe0\x07\x00\x00\x00\x00\x00\x00\x18\x08\xde\x07\x00\x00\x00\x00\x00\x00\x16\x00(\x08\xe0\x07\x00\x00\x00\x00\x00\x00\x18\x08\xde\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x06\x01\xde\x07\x00\x00\x00\x00\x00\x00\xdf\x07\x00\x00\x00\x00\x00\x00\xe0\x07\x00\x00\x00\x00\x00\x00'))


# print('plain', decode_plain_data(b'\x15\x06\x15\x00\x15\x06\x15\x06\x1c\x18\x08d\x00\x00\x00\x00\x00\x00\x00\x18\x08\n\x00\x00\x00\x00\x00\x00\x00\x16\x00(\x08d\x00\x00\x00\x00\x00\x00\x00\x18\x08\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x06\x01\n\x00\x00\x00\x00\x00\x00\x002\x00\x00\x00\x00\x00\x00\x00d\x00\x00\x00\x00\x00\x00\x00'))
# '\x15\x00\x15<\x15<'