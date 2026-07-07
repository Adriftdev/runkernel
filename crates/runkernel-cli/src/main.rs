use runkernel_cli::{run_cli, Cli};

fn main() {
    let cli = Cli::parse_args();
    match run_cli(cli) {
        Ok(outcome) => {
            if !outcome.output.is_empty() {
                println!("{}", outcome.output);
            }
            if outcome.exit_code != 0 {
                std::process::exit(outcome.exit_code);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
