# rsfake 

`rsfake` is a command-line tool for generating fake data based on a provided JSON schema file. It allows you to quickly generate large datasets with realistic-looking data for testing, benchmarking, or populating databases.

## Features

- Generate fake data based on a JSON schema file
- Supports various data types and generators from the `fake` crate
- Parallel processing using Rayon for improved performance
- Read input data from Parquet, JSON, or CSV files
- Write generated data to Parquet, JSON, or CSV files
- Customizable number of rows and threads
- Optional features for additional data types and generators

## Usage

```shell
rsfake [OPTIONS]
```

### Options

- `-s, --schema <SCHEMA>`: Specify the JSON schema file to use for data generation (default: "schema.json")
- `-r, --rows <ROWS>`: Specify the number of rows to generate (default: 10000)
- `-t, --threads <THREADS>`: Specify the number of threads to use for parallel processing (default: 1)
- `-o, --output <OUTPUT>`: Specify the output file path for the generated data
- `-i, --input <INPUT>`: Specify the input file path for reading existing data
- `-f, --format <FORMAT>`: Specify the output file format (default: "parquet")

### Examples

Generate 100,000 rows of fake data using 4 threads and write to a Parquet file:

```shell
rsfake -s schema.json -r 100000 -t 4 -o output.parquet
```

Read data from a CSV file, generate additional columns, and write to a JSON file:

```shell
rsfake -i input.csv -s schema.json -o output.json -f json
```

## JSON Schema

The JSON schema file defines the structure and types of the data to be generated. Each column in the schema represents a field in the resulting dataset.

Example schema:

```json
{
  "columns": [
    {
      "name": "id",
      "type": "u64"
    },
    {
      "name": "name",
      "type": "Name"
    },
    {
      "name": "email",
      "type": "SafeEmail"
    },
    {
      "name": "age",
      "type": "u32",
      "args": {
        "range": {
          "start": 18,
          "end": 80
        }
      }
    }
  ]
}
```

## Supported Data Types

`rsfake` supports a wide range of data types and generators provided by the `fake` crate. Some commonly used types include:

- Numeric types: `u32`, `u64`, `i32`, `i64`, `f32`, `f64`
- String types: `Word`, `Sentence`, `Paragraph`
- Person-related types: `FirstName`, `LastName`, `Name`, `Username`, `Email`
- Address-related types: `Country`, `City`, `StreetName`, `ZipCode`
- Date and time types: `Date`, `Time`, `DateTime` (requires `chrono` feature)
- Internet-related types: `IPv4`, `IPv6`, `DomainName`, `URL`
- Custom string formats using the `NumberWithFormat` type

For a complete list of supported types and their usage, please refer to the documentation of the `fake` crate.

## Optional Features

`rsfake` provides optional features that can be enabled to access additional data types and generators:

- `chrono`: Enables date and time-related types from the `chrono` crate
- `random_color`: Enables color-related types for generating random colors
- `http`: Enables HTTP-related types for generating HTTP status codes
- `uuid`: Enables UUID generation using the `uuid` crate
- `rust_decimal`: Enables decimal number generation using the `rust_decimal` crate
- `bigdecimal`: Enables arbitrary-precision decimal number generation using the `bigdecimal` crate

To enable a feature, pass it as a command-line argument when installing or running `rsfake`. For example:

```shell
cargo install rsfake --features "chrono uuid"
```

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgements

Heavily influenced by/derivative of https://github.com/aprxi/faster-data-generation 😀