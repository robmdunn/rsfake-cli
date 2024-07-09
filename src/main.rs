use std::path::Path;
use std::time::Instant;

use clap::{builder::{styling::AnsiColor, Styles}, Parser};

mod extract;
mod generate;

use extract::{read_file, write_dataframe};
use generate::generate_from_json;

const V3_STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Magenta.on_default())
    .placeholder(AnsiColor::Cyan.on_default());

#[derive(Parser)]
#[command(name = "rsfake")]
#[command(styles=V3_STYLES)]
#[command(about = "Generates fake data based on the provided schema file.")]
#[command(long_about = "This program generates fake data based on a JSON schema file. You can specify the number of rows, the number of threads for parallel processing, and the schema file to be used.")]
struct Cli {
    #[arg(short, long, env = "FAKER_SCHEMA_FILE", default_value = "schema.json")]
    schema: String,

    #[arg(short, long, env = "FAKER_NUM_ROWS", default_value = "10000")]
    rows: usize,

    #[arg(short, long, env = "RAYON_NUM_THREADS", default_value = "1")]
    threads: usize,

    #[arg(short, long, env = "FAKER_OUTPUT_PATH")]
    output: Option<String>,

    #[arg(short, long, env = "FAKER_INPUT_PATH")]
    input: Option<String>,

    #[arg(short, long, default_value = "parquet")]
    format: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads)
        .build_global()?;

    let mut df = if let Some(input_path) = cli.input {
        let start_time = Instant::now();
        let df = read_file(&input_path)?;
        let elapsed = start_time.elapsed().as_secs_f64();
        println!("{:?}", df);
        println!(
            "Time taken to read from {}: {:.3} seconds",
            Path::new(&input_path).extension().unwrap_or_default().to_str().unwrap_or("unknown"),
            elapsed
        );
        df
    } else {
        let start_time = Instant::now();
        let df = generate_from_json(&cli.schema, cli.rows)?;
        let elapsed = start_time.elapsed().as_secs_f64();
        println!("{:?}", df);
        println!(
            "Time taken to generate {} rows into a dataframe using {} threads: {:.3} seconds",
            cli.rows, cli.threads, elapsed
        );
        df
    };

    if let Some(output_path) = cli.output {
        let start_time = Instant::now();
        write_dataframe(&mut df, &output_path, &cli.format)?;
        let elapsed = start_time.elapsed().as_secs_f64();
        println!(
            "Time taken to write to {}: {:.3} seconds",
            cli.format, elapsed
        );
    }

    Ok(())
}