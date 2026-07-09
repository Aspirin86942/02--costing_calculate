mod args;
mod run;

use std::process::ExitCode;

use clap::Parser;

use args::CliArgs;

fn main() -> ExitCode {
    let args = CliArgs::parse();
    match run::run(args) {
        Ok(summary) => {
            println!("{}", serde_json::to_string_pretty(&summary).expect("serialize run summary"));
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error);
            ExitCode::FAILURE
        }
    }
}
