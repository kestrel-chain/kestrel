use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result, bail};
use consensus::Validator;
use crypto::{Bls12381Scheme, SignatureScheme};
use node::{GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator};
use types::Hash;

fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("validator-init") => validator_init(&arguments[1..]),
        Some("genesis-create") => genesis_create(&arguments[1..]),
        Some("genesis-validate") => genesis_validate(&arguments[1..]),
        _ => {
            println!(concat!(
                "cli ",
                env!("CARGO_PKG_VERSION"),
                "\n",
                "commands:\n",
                "  validator-init NAME STAKE NETWORK_ADDRESS RPC_ADDRESS GOSSIP_ADDRESS OUTPUT_DIR\n",
                "  genesis-create CHAIN_ID GENESIS_UNIX_MS VALIDATORS_JSON OUTPUT_JSON\n",
                "  genesis-validate GENESIS_JSON"
            ));
            Ok(())
        }
    }
}

fn validator_init(arguments: &[String]) -> Result<()> {
    if arguments.len() != 6 {
        bail!(
            "validator-init requires NAME STAKE NETWORK_ADDRESS RPC_ADDRESS GOSSIP_ADDRESS OUTPUT_DIR"
        );
    }
    let name = arguments[0].clone();
    let stake = arguments[1]
        .parse::<u64>()
        .context("stake must be an integer")?;
    if stake == 0 {
        bail!("stake must be nonzero");
    }
    arguments[4]
        .parse::<libp2p::Multiaddr>()
        .context("gossip address must be a valid multiaddr")?;
    let output = Path::new(&arguments[5]);
    fs::create_dir_all(output)?;
    let private_key = Bls12381Scheme::generate_os_private_key();
    let public_key = Bls12381Scheme.public_key(&private_key)?;
    let mut identity = b"kestrel/validator/id/v1".to_vec();
    identity.extend_from_slice(&public_key);
    let gossip_identity = libp2p::identity::Keypair::generate_ed25519();
    let profile = GenesisValidator {
        name,
        validator: Validator {
            id: Hash::digest(identity),
            stake,
            public_key,
            proof_of_possession: Bls12381Scheme.proof_of_possession(&private_key)?,
        },
        network_address: arguments[2].clone(),
        rpc_address: arguments[3].clone(),
        gossip_peer_id: gossip_identity.public().to_peer_id().to_string(),
        gossip_address: arguments[4].clone(),
    };
    write_secret(
        &output.join("validator.key"),
        hex::encode(private_key).as_bytes(),
    )?;
    write_secret(
        &output.join("gossip.key"),
        &gossip_identity.to_protobuf_encoding()?,
    )?;
    let profile_bytes = serde_json::to_vec_pretty(&profile)?;
    fs::write(output.join("validator.json"), profile_bytes)?;
    println!(
        "validator profile written; validator and gossip private keys are mode 0600 and were not printed"
    );
    Ok(())
}

fn genesis_create(arguments: &[String]) -> Result<()> {
    if arguments.len() != 4 {
        bail!("genesis-create requires CHAIN_ID GENESIS_UNIX_MS VALIDATORS_JSON OUTPUT_JSON");
    }
    let validators = serde_json::from_slice::<Vec<GenesisValidator>>(&fs::read(&arguments[2])?)?;
    let document = GenesisDocument {
        format_version: GENESIS_FORMAT_VERSION,
        chain_id: arguments[0].clone(),
        genesis_unix_ms: arguments[1]
            .parse()
            .context("genesis time must be an integer")?,
        blocks_per_epoch: 100,
        state_config: state::StateConfig::default(),
        active_signature_schemes: vec![1, 2],
        equivocation_slash_basis_points: 5_000,
        validators,
        initial_objects: Vec::new(),
    };
    document.write_json(&arguments[3])?;
    let validated = document.validate()?;
    println!(
        "genesis={} state_root={}",
        validated.genesis_hash, validated.state_root
    );
    Ok(())
}

fn genesis_validate(arguments: &[String]) -> Result<()> {
    if arguments.len() != 1 {
        bail!("genesis-validate requires GENESIS_JSON");
    }
    let document = GenesisDocument::load_json(&arguments[0])?;
    let validated = document.validate()?;
    println!(
        "valid chain={} genesis={} state_root={} validators={} stake={}",
        document.chain_id,
        validated.genesis_hash,
        validated.state_root,
        validated.validators.validators().len(),
        validated.validators.total_stake()
    );
    Ok(())
}

#[cfg(unix)]
fn write_secret(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("refusing to overwrite secret at {}", path.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// `validator-init` deliberately supports only Unix hosts. An owner-only
/// permission bit has no equivalent that this crate can verify without a
/// Windows test environment, and shipping unverified ACL-handling code for a
/// secret-key file would risk a false sense of security rather than a real
/// one. Run `validator-init` on a Unix host (including a Linux container or
/// WSL) and transfer the resulting `validator.json`/`gossip.key` files if the
/// validator itself runs on Windows.
#[cfg(not(unix))]
fn write_secret(_path: &Path, _bytes: &[u8]) -> Result<()> {
    bail!("validator-init requires a Unix host for secure key-file permissions")
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt};

    #[cfg(unix)]
    use crypto::Bls12381Scheme;
    #[cfg(unix)]
    use node::GenesisValidator;
    #[cfg(unix)]
    use tempfile::TempDir;

    #[cfg(unix)]
    use super::validator_init;

    #[test]
    fn help_identifies_as_kestrel() {
        assert_eq!(env!("CARGO_PKG_NAME"), "cli");
    }

    #[cfg(unix)]
    #[test]
    fn onboarding_writes_a_private_key_with_owner_only_permissions() {
        let directory = TempDir::new().unwrap();
        validator_init(&[
            "operator-one".to_owned(),
            "100".to_owned(),
            "127.0.0.1:9001".to_owned(),
            "127.0.0.1:10001".to_owned(),
            "/ip4/127.0.0.1/tcp/9101".to_owned(),
            directory.path().display().to_string(),
        ])
        .unwrap();
        let key_path = directory.path().join("validator.key");
        assert_eq!(
            fs::metadata(&key_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let gossip_key_path = directory.path().join("gossip.key");
        assert_eq!(
            fs::metadata(&gossip_key_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let profile = serde_json::from_slice::<GenesisValidator>(
            &fs::read(directory.path().join("validator.json")).unwrap(),
        )
        .unwrap();
        profile.gossip_peer_id.parse::<libp2p::PeerId>().unwrap();
        libp2p::identity::Keypair::from_protobuf_encoding(&fs::read(&gossip_key_path).unwrap())
            .unwrap();
        Bls12381Scheme
            .verify_proof_of_possession(
                &profile.validator.public_key,
                &profile.validator.proof_of_possession,
            )
            .unwrap();
    }
}
