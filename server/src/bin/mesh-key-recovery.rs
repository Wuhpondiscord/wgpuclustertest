use mesh_coordinator::key_recovery::{generate_identity, recover_to_file, validate_recipient};
use std::path::Path;

fn run(args: &[String]) -> Result<(), &'static str> {
    match args {
        [command, file] if command == "validate-config" => {
            let bytes = std::fs::read(file).map_err(|_| "Could not read public recovery configuration")?;
            let recipients: std::collections::HashMap<String, String> = serde_json::from_slice(&bytes).map_err(|_| "Invalid public recovery configuration")?;
            for recipient in recipients.values() { validate_recipient(recipient)?; }
            println!("Public operator recovery recipients validated");
            Ok(())
        }
        [command, directory] if command == "init" => {
            let recipient = generate_identity(Path::new(directory))?;
            println!("Recovery identity saved locally. Public recipient: {recipient}");
            Ok(())
        }
        [command, identity, encrypted, output] if command == "decrypt" => {
            recover_to_file(Path::new(identity), Path::new(encrypted), Path::new(output))?;
            println!("Recovered encryption values saved to the private output file. No values were printed.");
            Ok(())
        }
        _ => Err("Usage: mesh-key-recovery init <private-directory> | decrypt <identity-file> <backup.age> <new-private-output.json>"),
    }
}
fn main() {
    if let Err(error) = run(&std::env::args().skip(1).collect::<Vec<_>>()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
