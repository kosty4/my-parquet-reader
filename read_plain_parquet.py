import sys
sys.path.append("gen-py")

import parquet  # Import the generated Parquet Thrift module

# Import the generated Parquet Thrift module
import parquet.ttypes as parquet_types

from utils import read_parquet_metadata, deserialize_metadata, decode_column_data, parse_byte_array_string

filename = "data/plain/c83f91d3f54b4f9c8d49859e9f3831d8-0.parquet"


# Example usage
serialized_metadata = read_parquet_metadata(filename)

# print('serialized ', serialized_metadata)
deserialized_metadata = deserialize_metadata(serialized_metadata, parquet_types.FileMetaData)

parquet_type_to_bytes = {
    1:4,  #INT32 = 1;
    2:8,  #INT64 = 2;
    4:4,  #FLOAT = 4;
    5:8,  #DOUBLE = 5;
    # 6:101, #BYTE_ARRAY = 6;
    # 7:102 #FIXED_LEN_BYTE_ARRAY = 7;
}

# /** Default encoding.
# * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
# * INT32 - 4 bytes per value.  Stored as little-endian.
# * INT64 - 8 bytes per value.  Stored as little-endian.
# * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
# * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
# * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
# * FIXED_LEN_BYTE_ARRAY - Just the bytes.
# */

# Seek to the data position for the column chunk
# whence:
#     0: Start of file (default).
#     1: Current position.
#     2: End of file.

def read_row_group_data(file_path, metadata, row_group=0):

    output = {}
    with open(file_path, "rb") as f:
        row_group_metadata = metadata.row_groups[row_group]

        for column_chunk in row_group_metadata.columns:

            print("=== Column Chunk ===")

            print(f"  Column: {column_chunk.meta_data.path_in_schema}")
            print(f"  Type: {column_chunk.meta_data.type}")
            print(f"  Encodings: {column_chunk.meta_data.encodings}")

            print(f"  Offset: {column_chunk.meta_data.data_page_offset}") # Offset where data for this column starts
            print(f"  Compressed size: {column_chunk.meta_data.total_compressed_size}") # Length of the data for the column
            print(f"  Unompressed size: {column_chunk.meta_data.total_uncompressed_size}") # Length of the data for the column


            data_page_offset = column_chunk.meta_data.data_page_offset  # Offset where data for this column starts
            chunk_total_compressed_size = column_chunk.meta_data.total_compressed_size  # Length of the data for the column

            f.seek(data_page_offset, 0)

            # Read the page (it contains the PageHeader + Repetion Levels + Definition Levels + values)
            page_header_content = f.read(chunk_total_compressed_size)

            # Decode into the PageHeader
            page_header_deserialized = deserialize_metadata(page_header_content, parquet_types.PageHeader)

            print('=== Page Header ===')
            page_uncompressed_size = page_header_deserialized.uncompressed_page_size
            page_compressed_size = page_header_deserialized.compressed_page_size
            page_repetition_level_encoding = page_header_deserialized.data_page_header.repetition_level_encoding
            page_definition_level_encoding = page_header_deserialized.data_page_header.definition_level_encoding

            print('HEADER CONTENT START')
            print(page_header_deserialized)
            print('HEADER CONTENT END')



            print('Page uncompressed size ', page_uncompressed_size)
            print('Page compressed size ', page_compressed_size)

            print('repetition + definition levels ', page_repetition_level_encoding+page_definition_level_encoding)

            values_start = data_page_offset + chunk_total_compressed_size - page_uncompressed_size + page_repetition_level_encoding + page_definition_level_encoding
            values_end = data_page_offset + chunk_total_compressed_size

            bytes_to_read = values_end - values_start
            print('values start ', values_start, '  values end ', values_end)
            print('!! bytes to read ',values_end - values_start )

            # Go to start of the page
            f.seek(values_start, 0)

            # Read the bytes containing the data
            column_data = f.read(bytes_to_read)
            print('encoded data:', column_data)
            
            # BYTE_ARRAY Type
            if column_chunk.meta_data.type == 6: 
                decoded_data = parse_byte_array_string(column_data)

            # ALL OTHER TYPES
            else:
                encoding_to_bytes = parquet_type_to_bytes[column_chunk.meta_data.type]
                decoded_data = decode_column_data(column_data, 'PLAIN', num_bytes=encoding_to_bytes)
            print('decoded data:', decoded_data)


            print('------ COL END ------')
            print()

            output[column_chunk.meta_data.path_in_schema[0]] = decoded_data[0]
    
    return output



if __name__ == "__main__":
    
    read_row_group_data(filename,deserialized_metadata,0)