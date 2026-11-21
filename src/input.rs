use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use url::Url;

/// Positional arguments may be HTTP(S) URLs or UTF-8, one-URL-per-line files.
pub fn expand(inputs: &[String]) -> Result<Vec<String>> {
    let mut urls = Vec::new();
    for input in inputs {
        if input.contains("://") {
            urls.push(validate_url(input)?);
        } else {
            let text = fs::read_to_string(input)
                .with_context(|| format!("Cannot read URL list: {input}"))?;
            urls.extend(parse_list(&text, input)?);
        }
    }
    Ok(urls)
}

pub fn parse_list(text: &str, source: &str) -> Result<Vec<String>> {
    text.trim_start_matches('\u{feff}')
        .lines()
        .enumerate()
        .filter_map(|(line, value)| {
            let value = value.trim();
            (!value.is_empty() && !value.starts_with('#'))
                .then(|| validate_url(value).with_context(|| format!("{source}:{}", line + 1)))
        })
        .collect()
}

pub fn validate_url(value: &str) -> Result<String> {
    let mut url = Url::parse(value).context("Invalid URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("Only HTTP(S) URLs are supported: {value}");
    }
    url.set_fragment(None);
    Ok(url.into())
}

pub fn file_name(value: &str) -> String {
    let url = Url::parse(value).expect("URLs are validated before enqueueing");
    let last = url
        .path_segments()
        .and_then(|mut p| p.next_back())
        .unwrap_or("");
    let decoded = percent_decode_str(last).decode_utf8_lossy();
    let name: String = decoded
        .chars()
        .take(150)
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let name = name.trim().trim_matches('.');
    if name.is_empty() {
        "download".into()
    } else {
        name.into()
    }
}

pub fn numbered_name(name: &str, n: usize) -> String {
    let path = Path::new(name);
    match (path.file_stem(), path.extension()) {
        (Some(stem), Some(ext)) => {
            format!("{}.{}.{}", stem.to_string_lossy(), n, ext.to_string_lossy())
        }
        _ => format!("{name}.{n}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_cannot_escape_output_directory() {
        assert_eq!(
            file_name("https://example.org/%2F..%2Fhello%5Cfile.zip"),
            "_.._hello_file.zip"
        );
        assert_eq!(file_name("https://example.org/.."), "download");
        assert_eq!(file_name("https://example.org/a%0Ab"), "a_b");
        assert_eq!(file_name("https://example.org/"), "download");
    }

    #[test]
    fn list_accepts_bom_comments_and_preserves_url_hash_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("urls.txt");
        fs::write(
            &file,
            "\u{feff}# comment\n\n https://example.org/a%23b \nhttps://example.org/c#fragment\n",
        )
        .unwrap();
        assert_eq!(
            expand(&[file.display().to_string()]).unwrap(),
            ["https://example.org/a%23b", "https://example.org/c"]
        );
        fs::write(&file, "ftp://example.org/a\n").unwrap();
        assert!(expand(&[file.display().to_string()]).is_err());
    }
}
