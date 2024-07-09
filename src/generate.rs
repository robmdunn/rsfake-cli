use fake::Fake;
use fake::faker::company::raw::{Profession, Industry};
#[cfg(feature="rust_decimal")]
use fake::decimal::{Decimal, PositiveDecimal, NegativeDecimal, NoDecimalPoints};
#[cfg(feature="bigdecimal")]
use fake::bigdecimal::{BigDecimal, PositiveBigDecimal, NegativeBigDecimal, NoBigDecimalPoints};
use polars::prelude::*;
use rand::Rng;
use rayon::prelude::*;
use serde_json::Value;
use std::fs;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Duration, Utc};

#[derive(Error, Debug)]
pub enum GenerateError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

pub fn generate_from_json(json_file: &str, no_rows: usize) -> Result<DataFrame, GenerateError> {
    let json: Value = serde_json::from_str(&fs::read_to_string(json_file)?)?;

    let columns = json
        .get("columns")
        .and_then(|c| c.as_array())
        .ok_or_else(|| GenerateError::InvalidArgument("Missing or invalid 'columns' array in JSON schema".to_string()))?
        .par_iter()
        .map(|col_def| {
            let col_name = col_def
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();
            let col_type = col_def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            create_series_from_type(col_type, col_name, no_rows, col_def)
        })
        .collect::<Result<Vec<Series>, GenerateError>>()?;

    Ok(DataFrame::new(columns)?)
}

fn create_series_from_type(
    type_name: &str,
    col_name: &str,
    no_rows: usize,
    col_def: &Value,
) -> Result<Series, GenerateError> {
    use fake::faker::*;
    use fake::locales::EN;

    macro_rules! generate_series {
        ($faker:expr) => {{
            let data: Vec<String> = (0..no_rows).into_par_iter().map(|_| $faker.fake()).collect();
            Series::new(col_name, data)
        }};
    }

    macro_rules! generate_range_series {
        ($type:ty, $default_start:expr, $default_end:expr) => {{
            let (start, end) = get_range_args::<$type>(col_def, $default_start, $default_end)?;
            let data: Vec<$type> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(start..=end))
                .collect();
            Series::new(col_name, data)
        }};
    }
    
    macro_rules! generate_datetime_series {
        ($faker_type:expr) => {{
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| $faker_type.fake::<DateTime<Utc>>().to_rfc3339())
                .collect();
            Series::new(col_name, data)
        }};
    }

    macro_rules! generate_uuid_series {
        ($faker_type:expr) => {{
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| $faker_type.fake::<uuid::Uuid>().to_string())
                .collect();
            Series::new(col_name, data)
        }};
    }

    macro_rules! generate_decimal_series {
        ($faker_type:expr, $decimal_type:ty) => {{
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| $faker_type.fake::<$decimal_type>().to_string())
                .collect();
            Series::new(col_name, data)
        }};
    }

    fn generate_boolean_series(ratio: u8, no_rows: usize, col_name: &str) -> Series {
        let data: Vec<bool> = (0..no_rows)
            .into_par_iter()
            .map(|_| boolean::raw::Boolean(EN, ratio).fake())
            .collect();
        Series::new(col_name, data)
    }

    Ok(match type_name {
        "u32" => generate_range_series!(u32, u32::MIN, u32::MAX),
        "u64" => generate_range_series!(u64, u64::MIN, u64::MAX),
        "i32" => generate_range_series!(i32, i32::MIN, i32::MAX),
        "i64" => generate_range_series!(i64, i64::MIN, i64::MAX),
        "f32" => generate_range_series!(f32, 0.0, 1.0),
        "f64" => generate_range_series!(f64, 0.0, 1.0),
        "Boolean" => {
            let ratio = get_args_u8(col_def, "ratio")?;
            generate_boolean_series(ratio, no_rows, col_name)
        },
        "Word" => generate_series!(lorem::raw::Word(EN)),
        "Sentence" => {
            let (start, end) = get_range_args(col_def, 3, 10)?;
            generate_series!(lorem::raw::Sentence(EN, start..end))
        }
        "Paragraph" => {
            let (start, end) = get_range_args(col_def, 3, 7)?;
            generate_series!(lorem::raw::Paragraph(EN, start..end))
        }
        "FirstName" => generate_series!(name::raw::FirstName(EN)),
        "LastName" => generate_series!(name::raw::LastName(EN)),
        "Title" => generate_series!(name::raw::Title(EN)),
        "Suffix" => generate_series!(name::raw::Suffix(EN)),
        "Name" => generate_series!(name::raw::Name(EN)),
        "NameWithTitle" => generate_series!(name::raw::NameWithTitle(EN)),
        "Seniority" => generate_series!(job::raw::Seniority(EN)),
        "Field" => generate_series!(job::raw::Field(EN)),
        "Position" => generate_series!(job::raw::Position(EN)),
        "JobTitle" => generate_series!(job::raw::Title(EN)),
        "Digit" => generate_series!(number::raw::Digit(EN)),
        "NumberWithFormat" => {
            let fmt = get_args_string(col_def, "fmt")?;
            generate_series!(number::raw::NumberWithFormat(EN, &fmt))
        },
        "FreeEmailProvider" => generate_series!(internet::raw::FreeEmailProvider(EN)),
        "DomainSuffix" => generate_series!(internet::raw::DomainSuffix(EN)),
        "FreeEmail" => generate_series!(internet::raw::FreeEmail(EN)),
        "SafeEmail" => generate_series!(internet::raw::SafeEmail(EN)),
        "Username" => generate_series!(internet::raw::Username(EN)),
        "Password" => {
            let (start, end) = get_range_args(col_def, 8, 20)?;
            generate_series!(internet::raw::Password(EN, start..end))
        }
        "IPv4" => generate_series!(internet::raw::IPv4(EN)),
        "IPv6" => generate_series!(internet::raw::IPv6(EN)),
        "IP" => generate_series!(internet::raw::IP(EN)),
        "MACAddress" => generate_series!(internet::raw::MACAddress(EN)),
        "UserAgent" => generate_series!(internet::raw::UserAgent(EN)),
        #[cfg(feature = "http")]
        "RfcStatusCode" => generate_series!(http::raw::RfcStatusCode(EN)),
        #[cfg(feature = "http")]
        "ValidStatusCode" => generate_series!(http::raw::ValidStatusCode(EN)),
        #[cfg(feature = "random_color")]
        "HexColor" => generate_series!(color::raw::HexColor(EN)),
        #[cfg(feature = "random_color")]
        "RgbColor" => generate_series!(color::raw::RgbColor(EN)),
        #[cfg(feature = "random_color")]
        "RgbaColor" => generate_series!(color::raw::RgbaColor(EN)),
        #[cfg(feature = "random_color")]
        "HslColor" => generate_series!(color::raw::HslColor(EN)),
        #[cfg(feature = "random_color")]
        "HslaColor" => generate_series!(color::raw::HslaColor(EN)),
        #[cfg(feature = "random_color")]
        "Color" => generate_series!(color::raw::Color(EN)),
        "CompanySuffix" => generate_series!(company::raw::CompanySuffix(EN)),
        "CompanyName" => generate_series!(company::raw::CompanyName(EN)),
        "Buzzword" => generate_series!(company::raw::Buzzword(EN)),
        "BuzzwordMiddle" => generate_series!(company::raw::BuzzwordMiddle(EN)),
        "BuzzwordTail" => generate_series!(company::raw::BuzzwordTail(EN)),
        "CatchPhrase" => generate_series!(company::raw::CatchPhase(EN)),
        "BsVerb" => generate_series!(company::raw::BsVerb(EN)),
        "BsAdj" => generate_series!(company::raw::BsAdj(EN)),
        "BsNoun" => generate_series!(company::raw::BsNoun(EN)),
        "Bs" => generate_series!(company::raw::Bs(EN)),
        "Profession" => generate_series!(Profession(EN)),
        "Industry" => generate_series!(Industry(EN)),
        "CityPrefix" => generate_series!(address::raw::CityPrefix(EN)),
        "CitySuffix" => generate_series!(address::raw::CitySuffix(EN)),
        "CityName" => generate_series!(address::raw::CityName(EN)),
        "CountryName" => generate_series!(address::raw::CountryName(EN)),
        "CountryCode" => generate_series!(address::raw::CountryCode(EN)),
        "StreetSuffix" => generate_series!(address::raw::StreetSuffix(EN)),
        "StreetName" => generate_series!(address::raw::StreetName(EN)),
        "TimeZone" => generate_series!(address::raw::TimeZone(EN)),
        "StateName" => generate_series!(address::raw::StateName(EN)),
        "StateAbbr" => generate_series!(address::raw::StateAbbr(EN)),
        "SecondaryAddressType" => generate_series!(address::raw::SecondaryAddressType(EN)),
        "SecondaryAddress" => generate_series!(address::raw::SecondaryAddress(EN)),
        "ZipCode" => generate_series!(address::raw::ZipCode(EN)),
        "PostCode" => generate_series!(address::raw::PostCode(EN)),
        "BuildingNumber" => generate_series!(address::raw::BuildingNumber(EN)),
        "Latitude" => generate_series!(address::raw::Latitude(EN)),
        "Longitude" => generate_series!(address::raw::Longitude(EN)),
        "Geohash" => {
            let precision = get_args_u8(col_def, "precision")?;
            generate_series!(address::raw::Geohash(EN, precision))
        }
        "LicencePlate" => generate_series!(automotive::raw::LicencePlate(fake::locales::FR_FR)),
        "Isbn" => generate_series!(barcode::raw::Isbn(EN)),
        "Isbn13" => generate_series!(barcode::raw::Isbn13(EN)),
        "Isbn10" => generate_series!(barcode::raw::Isbn10(EN)),
        "PhoneNumber" => generate_series!(phone_number::raw::PhoneNumber(EN)),
        "CellNumber" => generate_series!(phone_number::raw::CellNumber(EN)),
        #[cfg(feature = "chrono")]
        "Time" => generate_series!(chrono::raw::Time(EN)),
        #[cfg(feature = "chrono")]
        "Date" => generate_series!(chrono::raw::Date(EN)),
        #[cfg(feature = "chrono")]
        "DateTime" => generate_series!(chrono::raw::DateTime(EN)),
        #[cfg(feature = "chrono")]
        "Duration" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let duration: Duration = chrono::raw::Duration(EN).fake();
                    duration.to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        #[cfg(feature = "chrono")]
        "DateTimeBefore" => {
            let dt = get_args_datetime(col_def, "dt")?;
            generate_datetime_series!(chrono::raw::DateTimeBefore(EN, dt))
        },
        #[cfg(feature = "chrono")]
        "DateTimeAfter" => {
            let dt = get_args_datetime(col_def, "dt")?;
            generate_datetime_series!(chrono::raw::DateTimeAfter(EN, dt))
        },
        #[cfg(feature = "chrono")]
        "DateTimeBetween" => {
            let (start, end) = get_args_datetimerange(col_def)?;
            generate_datetime_series!(chrono::raw::DateTimeBetween(EN, start, end))
        },
        "FilePath" => generate_series!(filesystem::raw::FilePath(EN)),
        "FileName" => generate_series!(filesystem::raw::FileName(EN)),
        "FileExtension" => generate_series!(filesystem::raw::FileExtension(EN)),
        "DirPath" => generate_series!(filesystem::raw::DirPath(EN)),
        "Bic" => generate_series!(finance::raw::Bic(EN)),
        #[cfg(feature = "uuid")]
        "UUIDv1" => generate_uuid_series!(fake::uuid::UUIDv1),
        #[cfg(feature = "uuid")]
        "UUIDv3" => generate_uuid_series!(fake::uuid::UUIDv3),
        #[cfg(feature = "uuid")]
        "UUIDv4" => generate_uuid_series!(fake::uuid::UUIDv4),
        #[cfg(feature = "uuid")]
        "UUIDv5" => generate_uuid_series!(fake::uuid::UUIDv5),
        "CurrencyCode" => generate_series!(currency::raw::CurrencyCode(EN)),
        "CurrencyName" => generate_series!(currency::raw::CurrencyName(EN)),
        "CurrencySymbol" => generate_series!(currency::raw::CurrencySymbol(EN)),
        "CreditCardNumber" => generate_series!(creditcard::raw::CreditCardNumber(EN)),
        #[cfg(feature = "rust_decimal")]
        "Decimal" => generate_decimal_series!(Decimal, rust_decimal::Decimal),
        #[cfg(feature = "rust_decimal")]
        "PositiveDecimal" => generate_decimal_series!(PositiveDecimal, rust_decimal::Decimal),
        #[cfg(feature = "rust_decimal")]
        "NegativeDecimal" => generate_decimal_series!(NegativeDecimal, rust_decimal::Decimal),
        #[cfg(feature = "rust_decimal")]
        "NoDecimalPoints" => generate_decimal_series!(NoDecimalPoints, rust_decimal::Decimal),
        #[cfg(feature = "bigdecimal")]
        "BigDecimal" => generate_decimal_series!(BigDecimal, bigdecimal::BigDecimal),
        #[cfg(feature = "bigdecimal")]
        "PositiveBigDecimal" => generate_decimal_series!(PositiveBigDecimal, bigdecimal::BigDecimal),
        #[cfg(feature = "bigdecimal")]
        "NegativeBigDecimal" => generate_decimal_series!(NegativeBigDecimal, bigdecimal::BigDecimal),
        #[cfg(feature = "bigdecimal")]
        "NoBigDecimalPoints" => generate_decimal_series!(NoBigDecimalPoints, bigdecimal::BigDecimal),
        _ => return Err(GenerateError::UnsupportedType(type_name.to_string())),
    })
}

fn get_range_args<T>(col_def: &Value, default_start: T, default_end: T) -> Result<(T, T), GenerateError>
where
    T: FromStr + PartialOrd,
    <T as FromStr>::Err: std::fmt::Debug,
{
    let range = col_def.get("args").and_then(|a| a.get("range"));

    let parse_value = |v: &Value| -> Result<T, GenerateError> {
        match v {
            Value::String(s) => s.parse().map_err(|_| GenerateError::InvalidArgument("Invalid string range value".to_string())),
            Value::Number(n) => n.to_string().parse().map_err(|_| GenerateError::InvalidArgument("Invalid numeric range value".to_string())),
            _ => Err(GenerateError::InvalidArgument("Invalid range value type".to_string())),
        }
    };

    let start = range
        .and_then(|r| r.get("start"))
        .map(parse_value)
        .transpose()?
        .unwrap_or(default_start);

    let end = range
        .and_then(|r| r.get("end"))
        .map(parse_value)
        .transpose()?
        .unwrap_or(default_end);

    if start <= end {
        Ok((start, end))
    } else {
        Err(GenerateError::InvalidArgument("'start' must be less than or equal to 'end'".to_string()))
    }
}

fn get_args_string(col_def: &Value, key: &str) -> Result<String, GenerateError> {
    col_def
        .get("args")
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| GenerateError::InvalidArgument(format!("Missing '{}' argument", key)))
}

fn get_args_u8(col_def: &Value, key: &str) -> Result<u8, GenerateError> {
    col_def
        .get("args")
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_u64())
        .map(|v| v as u8)
        .ok_or_else(|| GenerateError::InvalidArgument(format!("Invalid '{}' argument", key)))
}

#[cfg(feature = "chrono")]
fn get_args_datetime(col_def: &Value, key: &str) -> Result<DateTime<Utc>, GenerateError> {
    col_def
        .get("args")
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| GenerateError::InvalidArgument(format!("Invalid '{}' datetime", key)))
}

#[cfg(feature = "chrono")]
fn get_args_datetimerange(col_def: &Value) -> Result<(DateTime<Utc>, DateTime<Utc>), GenerateError> {
    let start = get_args_datetime(col_def, "start")?;
    let end = get_args_datetime(col_def, "end")?;
    if start <= end {
        Ok((start, end))
    } else {
        Err(GenerateError::InvalidArgument("Invalid datetime range".to_string()))
    }
}