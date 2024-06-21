use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use polars::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtractError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
}

pub fn read_file(file_path: &str) -> Result<DataFrame, ExtractError> {
    let path = Path::new(file_path);
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    match extension {
        "parquet" => {
            if path.is_dir() {
                read_partitioned_parquet(file_path)
            } else {
                read_single_parquet_file(file_path)
            }
        }
        "json" => read_json_file(file_path),
        "csv" => read_csv_file(file_path),
        _ => Err(ExtractError::UnsupportedFormat(extension.to_string())),
    }
}

pub fn write_dataframe(df: &mut DataFrame, file_path: &str, format: &str) -> Result<(), ExtractError> {
    match format {
        "parquet" => write_dataframe_to_parquet(df, file_path),
        "json" => write_dataframe_to_json(df, file_path),
        "csv" => write_dataframe_to_csv(df, file_path),
        _ => Err(ExtractError::UnsupportedFormat(format.to_string())),
    }
}

fn read_single_parquet_file(file_path: &str) -> Result<DataFrame, ExtractError> {
    let file = File::open(file_path)?;
    Ok(ParquetReader::new(file).finish()?)
}

fn read_partitioned_parquet(base_dir: &str) -> Result<DataFrame, ExtractError> {
    let mut dataframes = Vec::new();

    for entry in fs::read_dir(base_dir)? {
        let path = entry?.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let df = ParquetReader::new(File::open(path)?).finish()?;
            dataframes.push(df);
        }
    }

    dataframes
        .into_iter()
        .reduce(|acc, df| acc.vstack(&df).unwrap())
        .ok_or_else(|| ExtractError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "No dataframes found")))
}

fn read_json_file(file_path: &str) -> Result<DataFrame, ExtractError> {
    let file = File::open(file_path)?;
    Ok(JsonReader::new(file).finish()?)
}

fn read_csv_file(file_path: &str) -> Result<DataFrame, ExtractError> {
    let file = File::open(file_path)?;
    Ok(CsvReader::new(file).finish()?)
}

fn write_dataframe_to_parquet(df: &mut DataFrame, file_path: &str) -> Result<(), ExtractError> {
    let file = File::create(file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df)?;
    Ok(())
}

fn write_dataframe_to_json(df: &mut DataFrame, file_path: &str) -> Result<(), ExtractError> {
    let mut file = File::create(file_path)?;
    JsonWriter::new(&mut file)
        .with_json_format(JsonFormat::Json)
        .finish(df)?;
    Ok(())
}

fn write_dataframe_to_csv(df: &mut DataFrame, file_path: &str) -> Result<(), ExtractError> {
    let mut file = File::create(file_path)?;
    CsvWriter::new(&mut file).finish(df)?;
    Ok(())
}