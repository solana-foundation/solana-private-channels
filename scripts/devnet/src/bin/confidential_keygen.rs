//! Generate confidential keys: the escrow instance's `ESCROW_CONFIDENTIAL_KEYS`, or a
//! user's keys for `confidential_deposit`. The secret goes to a new owner-only file.
use devnet_scripts::confidential::{
    generate_keys_json, parse_keys, pinned_pubkey_line, write_keys_file,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <output-file>", args[0]);
        eprintln!("Example: {} ./keypairs/escrow-confidential.json", args[0]);
        eprintln!("\nWrites {{\"elgamal_secret\":..,\"aes_key\":..}} to a new file (mode 0600).");
        eprintln!(
            "For the escrow instance, set its content, single-quoted, as ESCROW_CONFIDENTIAL_KEYS"
        );
        eprintln!(
            "(secrets.yml: escrow_confidential_keys), and the printed ESCROW_CONFIDENTIAL_PUBKEY"
        );
        eprintln!("line as is (escrow_confidential_pubkey). One key per instance; never rotate it");
        eprintln!("while an escrow account is configured.");
        std::process::exit(1);
    }
    let path = Path::new(&args[1]);
    let json = generate_keys_json();
    write_keys_file(path, &json)?;
    let keys = parse_keys(&json)?;
    println!("Wrote {}", path.display());
    // Public: for the escrow instance, record it and set this line before the first configure.
    println!("{}", pinned_pubkey_line(&keys.keys));
    Ok(())
}
