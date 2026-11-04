//! Operator backups use the standard age file format. Only the public recipient
//! belongs on the server; the recovery identity and decryption stay offline.
use age::secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
};
use zeroize::{Zeroize, Zeroizing};

const FORMAT: &str = "mesh-operator-keys-v1";
pub const MAX_BACKUP_BYTES: usize = 16 * 1024;
const INVALID: &str = "Invalid operator key backup";

pub fn validate_recipient(value: &str) -> Result<(), &'static str> {
    value
        .parse::<age::x25519::Recipient>()
        .map(|_| ())
        .map_err(|_| "Invalid age recovery public key")
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageLocation {
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub path: String,
}

// Intentionally no Debug: these values must never enter logs.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeyBackup {
    format: String,
    created_at: u64,
    #[serde(rename = "MESH_STATE_ENCRYPTION_KEY")]
    state_key: String,
    #[serde(rename = "MESH_AUTH_ENCRYPTION_KEY")]
    auth_key: Option<String>,
    storage: StorageLocation,
}
impl Drop for KeyBackup {
    fn drop(&mut self) {
        self.state_key.zeroize();
        if let Some(key) = self.auth_key.as_mut() {
            key.zeroize();
        }
    }
}
impl KeyBackup {
    pub fn new(
        state: &[u8; 32],
        auth: Option<&[u8; 32]>,
        storage: StorageLocation,
        at: u64,
    ) -> Self {
        Self {
            format: FORMAT.into(),
            created_at: at,
            state_key: data_encoding::HEXLOWER.encode(state),
            auth_key: auth.map(|key| data_encoding::HEXLOWER.encode(key)),
            storage,
        }
    }
    fn validate(&self) -> Result<(), &'static str> {
        let key = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.format != FORMAT
            || !key(&self.state_key)
            || self.auth_key.as_deref().is_some_and(|v| !key(v))
        {
            return Err(INVALID);
        }
        Ok(())
    }
}

pub fn encrypt(recipient: &str, backup: &KeyBackup) -> Result<Vec<u8>, &'static str> {
    backup.validate()?;
    let recipient: age::x25519::Recipient = recipient
        .parse()
        .map_err(|_| "Invalid age recovery public key")?;
    let plaintext = Zeroizing::new(serde_json::to_vec(backup).map_err(|_| INVALID)?);
    if plaintext.len() > MAX_BACKUP_BYTES / 2 {
        return Err(INVALID);
    }
    age::encrypt(&recipient, &plaintext).map_err(|_| "Could not encrypt operator key backup")
}

pub fn decrypt(identity: &str, ciphertext: &[u8]) -> Result<KeyBackup, &'static str> {
    if ciphertext.len() > MAX_BACKUP_BYTES {
        return Err(INVALID);
    }
    let identity: age::x25519::Identity = identity
        .trim()
        .parse()
        .map_err(|_| "Invalid age recovery identity")?;
    let plaintext = Zeroizing::new(
        age::decrypt(&identity, ciphertext)
            .map_err(|_| "Backup authentication failed or recovery key does not match")?,
    );
    let backup: KeyBackup = serde_json::from_slice(&plaintext).map_err(|_| INVALID)?;
    backup.validate()?;
    Ok(backup)
}

/// Never overwrite an existing private key or recovered secrets file. On Windows,
/// restrict the empty file before writing secrets; on Unix create it with 0600.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<(), &'static str> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| "Could not create private output; choose a new path")?;
    #[cfg(windows)]
    {
        // Fixed script and an environment value avoid shell interpolation of paths.
        let secured = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", "$ErrorActionPreference='Stop'; $sid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User; $acl=New-Object System.Security.AccessControl.FileSecurity; $acl.SetOwner($sid); $acl.SetAccessRuleProtection($true,$false); foreach($id in @($sid.Value,'S-1-5-18','S-1-5-32-544')) { $rule=New-Object System.Security.AccessControl.FileSystemAccessRule([System.Security.Principal.SecurityIdentifier]::new($id),'FullControl','Allow'); $acl.AddAccessRule($rule) }; [System.IO.File]::SetAccessControl($env:MESH_PRIVATE_OUTPUT,$acl)"])
            .env("MESH_PRIVATE_OUTPUT", path)
            .output().is_ok_and(|result| result.status.success());
        if !secured {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err("Could not restrict private output permissions");
        }
    }
    let result = file
        .write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| "Could not write private output");
    drop(file);
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

pub fn generate_identity(directory: &Path) -> Result<String, &'static str> {
    // The caller chooses a private local directory. Private file creation never
    // falls back to a less restrictive mode or overwrites an existing identity.
    std::fs::create_dir_all(directory).map_err(|_| "Could not create recovery directory")?;
    let identity = age::x25519::Identity::generate();
    let recipient = identity.to_public().to_string();
    let private = identity.to_string();
    write_private(
        &directory.join("mesh-recovery-identity.txt"),
        private.expose_secret().as_bytes(),
    )?;
    let public_path = directory.join("mesh-recovery-recipient.txt");
    let mut public = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(public_path)
        .map_err(|_| "Could not create public recipient file; private identity was saved")?;
    public
        .write_all(recipient.as_bytes())
        .map_err(|_| "Could not save recovery public key")?;
    Ok(recipient)
}

pub fn recover_to_file(
    identity_path: &Path,
    encrypted_path: &Path,
    output: &Path,
) -> Result<(), &'static str> {
    let mut identity = Zeroizing::new(String::new());
    std::fs::File::open(identity_path)
        .map_err(|_| "Could not read recovery identity")?
        .take(1025)
        .read_to_string(&mut identity)
        .map_err(|_| "Could not read recovery identity")?;
    if identity.len() > 1024 {
        return Err("Invalid recovery identity file");
    }
    let mut ciphertext = Vec::new();
    std::fs::File::open(encrypted_path)
        .map_err(|_| "Could not read encrypted backup")?
        .take((MAX_BACKUP_BYTES + 1) as u64)
        .read_to_end(&mut ciphertext)
        .map_err(|_| "Could not read encrypted backup")?;
    let backup = decrypt(&identity, &ciphertext)?;
    let json = Zeroizing::new(serde_json::to_vec_pretty(&backup).map_err(|_| INVALID)?);
    write_private(output, &json)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn backup() -> KeyBackup {
        KeyBackup::new(
            &[19; 32],
            Some(&[27; 32]),
            StorageLocation {
                repository: Some("owner/private-state".into()),
                branch: Some("main".into()),
                path: "mesh-state.json".into(),
            },
            100,
        )
    }

    #[test]
    fn only_the_pinned_identity_recovers_both_exact_values_and_context() {
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let first = encrypt(&recipient, &backup()).unwrap();
        let second = encrypt(&recipient, &backup()).unwrap();
        assert_ne!(first, second);
        assert!(!String::from_utf8_lossy(&first).contains("MESH_STATE_ENCRYPTION_KEY"));
        let restored = decrypt(identity.to_string().expose_secret(), &first).unwrap();
        assert_eq!(
            restored.state_key,
            data_encoding::HEXLOWER.encode(&[19; 32])
        );
        assert_eq!(
            restored.auth_key,
            Some(data_encoding::HEXLOWER.encode(&[27; 32]))
        );
        assert_eq!(
            restored.storage.repository.as_deref(),
            Some("owner/private-state")
        );
        let wrong = age::x25519::Identity::generate();
        assert!(decrypt(wrong.to_string().expose_secret(), &first).is_err());
        let mut tampered = first;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(decrypt(identity.to_string().expose_secret(), &tampered).is_err());
    }

    #[test]
    fn invalid_recipients_and_oversized_or_invalid_backups_are_rejected() {
        assert!(encrypt("not-a-public-key", &backup()).is_err());
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let invalid = age::encrypt(&identity.to_public(), b"{}").unwrap();
        assert!(decrypt(identity.to_string().expose_secret(), &invalid).is_err());
        assert!(decrypt(
            identity.to_string().expose_secret(),
            &vec![0; MAX_BACKUP_BYTES + 1]
        )
        .is_err());
        let mut invalid = backup();
        invalid.state_key = "short".into();
        assert!(encrypt(&recipient, &invalid).is_err());
    }

    #[test]
    fn local_recovery_does_not_overwrite_existing_private_outputs() {
        let directory =
            std::env::temp_dir().join(format!("mesh-key-recovery-{}", uuid::Uuid::new_v4()));
        let recipient = generate_identity(&directory).unwrap();
        assert!(generate_identity(&directory).is_err());
        let encrypted = directory.join("backup.age");
        std::fs::write(&encrypted, encrypt(&recipient, &backup()).unwrap()).unwrap();
        let output = directory.join("recovered.json");
        recover_to_file(
            &directory.join("mesh-recovery-identity.txt"),
            &encrypted,
            &output,
        )
        .unwrap();
        assert!(recover_to_file(
            &directory.join("mesh-recovery-identity.txt"),
            &encrypted,
            &output
        )
        .is_err());
        let decoded: KeyBackup = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(decoded.state_key, data_encoding::HEXLOWER.encode(&[19; 32]));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(output).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        for file in [
            "mesh-recovery-identity.txt",
            "mesh-recovery-recipient.txt",
            "backup.age",
            "recovered.json",
        ] {
            let _ = std::fs::remove_file(directory.join(file));
        }
        let _ = std::fs::remove_dir(directory);
    }
}
