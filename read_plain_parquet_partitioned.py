import os
import sys
sys.path.append("gen-py")

import pandas as pd
import parquet  # Import the generated Parquet Thrift module

# Import the generated Parquet Thrift module
import parquet.ttypes as parquet_types

from read_plain_parquet import read_row_group_data
from utils import read_parquet_metadata, deserialize_metadata


def parse_partitioned_parquet(base_path: str) -> pd.DataFrame:
    """
    Parses partitioned Parquet files from different folders and returns a combined DataFrame.

    :param base_path: Root directory containing partitioned Parquet files.
    :return: A Pandas DataFrame with the combined data.
    """

    data_total = []

    # Walk through the base directory
    for root, folders, files in os.walk(base_path):
        if root != base_path:
            print('root ', root)

            folder_name = root.split('/')[-1]
            column_name = folder_name.split('=')[0]
            column_value = folder_name.split('=')[1]
            print('col name ', column_name, column_value)

            for ix, f in enumerate(files):
                print(f)
                if f.endswith(".parquet"):

                    # read the parquet
                    filename = root + '/' + f
                    print('filename to read ', filename)

                    # Read metadata of the file

                    # Example usage
                    serialized_metadata = read_parquet_metadata(filename)

                    # print('serialized ', serialized_metadata)
                    deserialized_metadata = deserialize_metadata(serialized_metadata, parquet_types.FileMetaData)

                    # print(deserialized_metadata)
                    cdata = read_row_group_data(filename, deserialized_metadata)

                    # append the current folder
                    cdata[column_name] = column_value
                    data_total.append(cdata)

    df_out = pd.DataFrame.from_records(data_total)

    return df_out


base_directory = "data/plain_part"
out_list = parse_partitioned_parquet(base_directory)

print(out_list)
