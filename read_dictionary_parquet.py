import sys
sys.path.append("gen-py")

import parquet  # Import the generated Parquet Thrift module

# Import the generated Parquet Thrift module
import parquet.ttypes as parquet_types

from utils import read_parquet_metadata, deserialize_metadata, decode_column_data, parse_byte_array_string

filename = "data/dictionairy/a652c59cd8d84a4783887e1782ec99f0-0.parquet"


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

encoding_num_to_human_value = {
    0: 'PLAIN',
    8: 'RLE_DICTIONARY'
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

def calculate_level_size(num_values, encoding_type):
    """Calculate size in bytes needed for repetition or definition levels."""
    if encoding_type == parquet_types.Encoding.PLAIN:
        bit_width = 1  # Usually small bit-widths
    elif encoding_type == parquet_types.Encoding.RLE:
        bit_width = 4  # Typically bit-packed or RLE encoded
    else:
        return 0  # No levels present

    return ((num_values * bit_width) + 7) // 8  # Convert bits to bytes


def read_row_group_data(file_path, metadata, row_group=0):

    output = {}
    with open(file_path, "rb") as f:
        row_group_metadata = metadata.row_groups[row_group]

        for column_chunk in row_group_metadata.columns:

            dict_decoded_data = None

            print("=== Column Chunk ===")

            print(f"  Column: {column_chunk.meta_data.path_in_schema}")
            print(f"  Type: {column_chunk.meta_data.type}")
            print(f"  Encodings: {column_chunk.meta_data.encodings}")

            print(f"  Offset: {column_chunk.meta_data.data_page_offset}") # Offset where data for this column starts
            print(f"  Compressed size: {column_chunk.meta_data.total_compressed_size}") # Length of the data for the column
            print(f"  Unompressed size: {column_chunk.meta_data.total_uncompressed_size}") # Length of the data for the column


            data_page_offset = column_chunk.meta_data.data_page_offset  # Offset where data for this column starts
            dictionary_page_offset = column_chunk.meta_data.dictionary_page_offset # Offset where dicitionary encoding for this column starts
            chunk_total_compressed_size = column_chunk.meta_data.total_compressed_size  # Length of the data for the column
            
            print('Dict page offset ',dictionary_page_offset )
            print('Data page offset', data_page_offset)
            print('Chunk Total compressed size ', chunk_total_compressed_size)

            # ==== Read Dictionary Page (if exists) ====
            if dictionary_page_offset:
                f.seek(dictionary_page_offset, 0)
                dict_page_header_content = f.read(chunk_total_compressed_size)  # Read full chunk
                dict_header_deserialized = deserialize_metadata(dict_page_header_content, parquet_types.PageHeader)

                dict_page_values_start = dictionary_page_offset + 14  # Adjust as needed
                dict_page_size = dict_header_deserialized.dictionary_page_header.num_values * 8

                f.seek(dict_page_values_start, 0)
                dict_encodings = f.read(dict_page_size)
                dict_decoded_data = decode_column_data(dict_encodings, 'PLAIN', num_bytes=8)

                print('Decoded Dictionary:', dict_decoded_data)


            # ==== Read page header ====

            f.seek(data_page_offset, 0)
            # Read the page (it contains the PageHeader + Repetion Levels + Definition Levels + values)
            data_page_header_content = f.read(chunk_total_compressed_size)
            
            # Decode into the PageHeader
            data_page_header_deserialized = deserialize_metadata(data_page_header_content, parquet_types.PageHeader)

            encoding_type = encoding_num_to_human_value[data_page_header_deserialized.data_page_header.encoding]
            data_page_uncompressed_size = data_page_header_deserialized.uncompressed_page_size
            data_page_num_values = data_page_header_deserialized.data_page_header.num_values

            # Handle repetition and definition levels
            # Now the values here are in bytes
            rep_levels_size = calculate_level_size(data_page_num_values, data_page_header_deserialized.data_page_header.repetition_level_encoding)
            def_levels_size = calculate_level_size(data_page_num_values, data_page_header_deserialized.data_page_header.definition_level_encoding)

            print('rep_levels_size ', rep_levels_size, ' def_levels_size ', def_levels_size)

            print('DATA PAGE HEADER CONTENT START')
            print(data_page_header_deserialized)
            print('DATA PAGE HEADER CONTENT END')


            # data_page_header_size =  chunk_total_compressed_size - data_page_uncompressed_size
            data_page_header_size = 18

            print('data page header size ', data_page_header_size)

            data_size = data_page_uncompressed_size
            # data_size = data_page_uncompressed_size - ( rep_levels_size + def_levels_size )

            values_start = data_page_offset + data_page_header_size + rep_levels_size + def_levels_size
            values_end = values_start + data_size

            # Need help here...

            bytes_to_read = values_end - values_start
            print('values start ', values_start, '  values end ', values_end)
            print('!! bytes to read ',bytes_to_read )

            # # Go to start of the page
            # f.seek(values_start, 0)

            # # Read the bytes containing the data
            # column_data = f.read(bytes_to_read)
            # print('encoded data:', column_data)
            
            # # BYTE_ARRAY Type
            # if column_chunk.meta_data.type == 6: 
            #     decoded_data = parse_byte_array_string(column_data)

            # # ALL OTHER TYPES
            # else:
            #     encoding_to_bytes = parquet_type_to_bytes[column_chunk.meta_data.type]
            #     decoded_data = decode_column_data(column_data, encoding_type, num_bytes=encoding_to_bytes, dicitionary=dict_decoded_data)
            # print('decoded data:', decoded_data)


            print('------ COL END ------')
            print()
    
    return output



if __name__ == "__main__":
    
    read_row_group_data(filename,deserialized_metadata,0)

    # column_data = b'\x02\x00\x00\x00\x06\x01\x02\x03$\x00'
    # encoding = 'RLE_DICTIONARY'
    # dictionary = [2014,2015,2016]
    # decode_column_data(column_data, encoding=encoding,dicitionary=dictionary)
    