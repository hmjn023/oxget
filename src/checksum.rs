use anyhow::{Result, bail, ensure};
use std::{collections::BTreeMap, path::Path};

pub fn normalize(hash: &str) -> Result<String> {
    ensure!(
        hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "SHA-256 must contain exactly 64 hexadecimal characters"
    );
    Ok(hash.to_ascii_lowercase())
}

pub fn parse_sums(text: &str) -> Result<BTreeMap<String, String>> {
    let mut sums = BTreeMap::new();
    for (index, line) in text.trim_start_matches('\u{feff}').lines().enumerate() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        ensure!(
            line.len() >= 67 && line.is_char_boundary(64),
            "Invalid SHA256SUMS line {}",
            index + 1
        );
        let hash = normalize(&line[..64])?;
        let rest = &line[64..];
        ensure!(
            rest.starts_with("  ") || rest.starts_with(" *"),
            "Invalid SHA256SUMS separator on line {}",
            index + 1
        );
        let name = rest[2..].trim_start_matches("./").to_string();
        ensure!(
            !name.is_empty() && !name.starts_with('/') && !name.split('/').any(|p| p == ".."),
            "Invalid checksum filename"
        );
        if let Some(previous) = sums.insert(name.clone(), hash.clone()) {
            ensure!(previous == hash, "Conflicting hashes for {name}");
        }
    }
    ensure!(!sums.is_empty(), "SHA256SUMS contains no checksums");
    Ok(sums)
}

pub fn for_urls(
    urls: &[String],
    hash: Option<&str>,
    manifest: Option<&Path>,
) -> Result<BTreeMap<String, String>> {
    let unique: std::collections::BTreeSet<_> = urls.iter().collect();
    if hash.is_some() {
        ensure!(unique.len() == 1, "--sha256 requires exactly one URL");
    }
    let sums = manifest
        .map(|path| {
            std::fs::read_to_string(path)
                .and_then(|text| parse_sums(&text).map_err(std::io::Error::other))
        })
        .transpose()?;
    let direct = hash.map(normalize).transpose()?;
    let mut result = BTreeMap::new();
    for url in unique {
        let parsed = url::Url::parse(url)?;
        let path = percent_encoding::percent_decode_str(parsed.path()).decode_utf8()?;
        let mut expected = direct.clone();
        if let Some(sums) = &sums {
            let matches: Vec<_> = sums
                .iter()
                .filter(|(name, _)| path.ends_with(&format!("/{name}")))
                .collect();
            ensure!(
                matches.len() == 1,
                "Expected exactly one SHA256SUMS match for {url}, found {}",
                matches.len()
            );
            let found = matches[0].1;
            if let Some(hash) = &expected {
                ensure!(hash == found, "Conflicting SHA-256 values for {url}");
            }
            expected = Some(found.clone());
        }
        if let Some(hash) = expected {
            result.insert(url.clone(), hash);
        }
    }
    if result.is_empty() && (hash.is_some() || manifest.is_some()) {
        bail!("No URLs matched checksums");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sums_validate_and_map_unicode_spaces_and_binary_markers() {
        let hash = "aB".repeat(32);
        let text = format!("{hash}  model one.bin\n{hash} *日本語.bin\n");
        let sums = parse_sums(&text).unwrap();
        assert_eq!(sums["model one.bin"], hash.to_lowercase());
        assert!(normalize("abc").is_err());
        assert!(parse_sums(&format!("{hash}  ../escape")).is_err());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("SHA256SUMS");
        std::fs::write(&file, text).unwrap();
        assert_eq!(
            for_urls(
                &["https://example.org/model%20one.bin".into()],
                None,
                Some(&file)
            )
            .unwrap()
            .len(),
            1
        );
        assert!(for_urls(&["https://example.org/missing".into()], None, Some(&file)).is_err());
    }
}
