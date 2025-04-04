

# Parquet Reader Using Thrift Protocol

This project is a simple implementation of a Parquet file reader built from scratch using the Thrift protocol in Python. 
It was created to have a better understanding of underworkings of a parquet file, how the data is stored in it and how its read out.
It provides basic functionality for reading Parquet files with different encodings and handling partitioned datasets.

## Scenarios for Reading Parquet Files

1. **read_plain_parquet.py**  
   Reads a Parquet file with PLAIN encoding.

2. **read_plain_parquet_partitioned.py**  
   Reads a partitioned Parquet file by navigating through folders and extracting the relevant data.

3. **read_dictionary_parquet.py**  
   An attempt to read a Parquet file that uses dictionary encoding with RLE (Run-Length Encoding) or Bitpacking.

## What is Thrift Protocol?

The Thrift protocol is a framework for cross-language services development, created by Facebook. It allows for efficient and compact serialization and communication of data structures, providing support for multiple programming languages. Thrift enables developers to define data types and service interfaces in a language-neutral file, then automatically generate code in different programming languages. In this project, Thrift is used for serializing and deserializing data within Parquet files, making it easier to parse the data efficiently.

Note: You can generate your own python classes for Thrift protocol by using one of those commands:

thrift --gen py example_metadata.thrift
thrift --gen py parquet.thrift

## Parquet file Layout

The general layout of a parquet file is the following:
[alt text](images/file_format.gif "Format")

And the metadata:
[alt text](images/file_layout.gif "Metadata")



## RLE (Run-Length Encoding) and Bitpacking

### Run-Length Encoding (RLE)
RLE is a simple and effective compression technique used to store repeated data elements more efficiently. 
In RLE, consecutive identical data elements are stored as a single value and a count. For example, the sequence:

AAAABBBCCDAAA can be compressed as: 4A3B2C1D3A

This significantly reduces the space needed when there are many repeated values, as we store the value and its count rather than repeating the value multiple times.

### Bitpacking
Bitpacking is a technique used to store data in a more compact form by using the smallest number of bits necessary to represent values. 
Instead of using the default 8, 16, or 32 bits to store integer values, Bitpacking optimizes storage by using only the number of bits required to represent each value. 
For example, if you have a set of values between 0 and 15, you can store them in 4 bits instead of 8 bits.

### How RLE and Bitpacking Enhance Storage Efficiency

Both RLE and Bitpacking are widely used in Parquet to achieve efficient data compression, which significantly reduces storage space and improves query performance. By applying RLE, Parquet files can store repeated values compactly, and with Bitpacking, values are stored using fewer bits, further saving space. These techniques are particularly effective in large datasets, as they reduce the amount of storage required without losing any data.

