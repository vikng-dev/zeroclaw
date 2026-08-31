//! `include = [...]` expansion for `config.toml`.
//!
//! A config file may pull in shared fragments before its own keys are read:
//!
//! ```toml
//! include = ["fleet.toml"]
//! ```
//!
//! Paths resolve relative to the directory of the file carrying the key.
//! Fragments are merged as raw TOML — before any schema deserialization —
//! so a fragment need not be a complete, valid `Config` on its own.
//!
//! Precedence: every include is merged in array order, then the including
//! file on top. Tables deep-merge; scalars and arrays replace wholesale.
//! Nested includes are expanded depth-first, so a fragment's own includes
//! land underneath it. Env-var overrides apply later, against the typed
//! `Config`, and therefore still win over everything merged here.

use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

/// Top-level key holding the include list. Stripped during expansion so it
/// never reaches serde, which would flag it as an unknown key.
pub const INCLUDE_KEY: &str = "include";

/// Expand the `include` list in `contents`, read from `path`.
///
/// Returns `contents` verbatim when there is nothing to include, so a config
/// that does not use the feature is byte-identical to what was read. A root
/// file that fails to parse is also returned verbatim: the caller's migration
/// and salvage path reports TOML syntax errors with far better context than
/// this module could. Once an `include` key is present the expansion fails
/// closed — a missing, unreadable, malformed, or cyclic fragment is an error,
/// never a silently degraded config.
pub fn expand(contents: &str, path: &Path) -> Result<String> {
    let Ok(root) = contents.parse::<toml::Table>() else {
        return Ok(contents.to_string());
    };
    if !root.contains_key(INCLUDE_KEY) {
        return Ok(contents.to_string());
    }

    let resolved = resolve(path);
    let dir = resolved
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let mut merged = toml::Table::new();
    let mut chain = vec![resolved];
    merge_source(&mut merged, root, &dir, &mut chain)?;

    toml::to_string(&merged).with_context(|| {
        format!(
            "Failed to re-serialize {} after include expansion",
            path.display()
        )
    })
}

/// Merge one already-parsed source into `out`: its includes first (in array
/// order, depth-first), then its own keys.
fn merge_source(
    out: &mut toml::Table,
    mut table: toml::Table,
    dir: &Path,
    chain: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in take_includes(&mut table, chain)? {
        let target = dir.join(&entry);
        let resolved = std::fs::canonicalize(&target).with_context(|| {
            format!(
                "Config include \"{entry}\" from {} does not resolve to a readable file at {}",
                current(chain),
                target.display()
            )
        })?;

        if chain.contains(&resolved) {
            chain.push(resolved);
            bail!("Config include cycle: {}", chain_display(chain));
        }

        let raw = std::fs::read_to_string(&resolved)
            .with_context(|| format!("Failed to read config include {}", resolved.display()))?;
        let parsed: toml::Table = raw
            .parse()
            .with_context(|| format!("Failed to parse config include {}", resolved.display()))?;
        let child_dir = resolved
            .parent()
            .map_or_else(|| dir.to_path_buf(), Path::to_path_buf);

        chain.push(resolved);
        merge_source(out, parsed, &child_dir, chain)?;
        chain.pop();
    }

    deep_merge(out, table);
    Ok(())
}

/// Remove the `include` key and validate it as an array of path strings.
fn take_includes(table: &mut toml::Table, chain: &[PathBuf]) -> Result<Vec<String>> {
    let Some(raw) = table.remove(INCLUDE_KEY) else {
        return Ok(Vec::new());
    };
    let Some(items) = raw.as_array() else {
        bail!(
            "`{INCLUDE_KEY}` in {} must be an array of paths",
            current(chain)
        );
    };
    let mut paths = Vec::with_capacity(items.len());
    for item in items {
        let Some(entry) = item.as_str() else {
            bail!(
                "`{INCLUDE_KEY}` in {} must contain only path strings",
                current(chain)
            );
        };
        paths.push(entry.to_string());
    }
    Ok(paths)
}

/// Overlay `src` onto `dst`: tables recurse, everything else replaces.
fn deep_merge(dst: &mut toml::Table, src: toml::Table) {
    for (key, value) in src {
        let merged = match (dst.remove(&key), value) {
            (Some(toml::Value::Table(mut base)), toml::Value::Table(overlay)) => {
                deep_merge(&mut base, overlay);
                toml::Value::Table(base)
            }
            (_, overlay) => overlay,
        };
        dst.insert(key, merged);
    }
}

/// Absolute, symlink-resolved form of `path`, falling back to `path` itself
/// so cycle detection still works on platforms or paths that resist it.
fn resolve(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn current(chain: &[PathBuf]) -> String {
    chain
        .last()
        .map_or_else(|| "config".to_string(), |p| p.display().to_string())
}

fn chain_display(chain: &[PathBuf]) -> String {
    chain
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(" -> ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Write `body` to `dir/name`, creating parent directories.
    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, body).unwrap();
        path
    }

    fn expand_file(path: &Path) -> Result<toml::Table> {
        let raw = fs::read_to_string(path).unwrap();
        Ok(expand(&raw, path)?.parse().unwrap())
    }

    #[test]
    fn no_include_key_returns_contents_verbatim() {
        let dir = TempDir::new().unwrap();
        let body = "# comment kept\nschema_version = 3\n\n[agents.a]\nname = \"a\"\n";
        let path = write(dir.path(), "config.toml", body);
        assert_eq!(expand(body, &path).unwrap(), body);
    }

    #[test]
    fn including_file_wins_over_include() {
        let dir = TempDir::new().unwrap();
        write(
            dir.path(),
            "fleet.toml",
            "schema_version = 3\nname = \"fleet\"\nshared_only = \"kept\"\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"fleet.toml\"]\nname = \"bot\"\n",
        );

        let merged = expand_file(&path).unwrap();
        assert_eq!(merged["name"].as_str(), Some("bot"));
        assert_eq!(merged["shared_only"].as_str(), Some("kept"));
        assert_eq!(merged["schema_version"].as_integer(), Some(3));
        assert!(
            !merged.contains_key(INCLUDE_KEY),
            "include key must be stripped"
        );
    }

    #[test]
    fn later_include_wins_over_earlier() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "a.toml", "who = \"a\"\nfrom_a = true\n");
        write(dir.path(), "b.toml", "who = \"b\"\nfrom_b = true\n");
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"a.toml\", \"b.toml\"]\n",
        );

        let merged = expand_file(&path).unwrap();
        assert_eq!(merged["who"].as_str(), Some("b"));
        assert_eq!(merged["from_a"].as_bool(), Some(true));
        assert_eq!(merged["from_b"].as_bool(), Some(true));
    }

    #[test]
    fn tables_deep_merge_while_scalars_and_arrays_replace() {
        let dir = TempDir::new().unwrap();
        write(
            dir.path(),
            "fleet.toml",
            r#"
[security]
autonomy_level = "supervised"
allowed_commands = ["ls", "cat"]

[security.rate_limit]
per_minute = 10
per_hour = 100

[observability]
backend = "otlp"
"#,
        );
        let path = write(
            dir.path(),
            "config.toml",
            r#"
include = ["fleet.toml"]

[security]
allowed_commands = ["git"]

[security.rate_limit]
per_minute = 99
"#,
        );

        let merged = expand_file(&path).unwrap();
        let security = merged["security"].as_table().unwrap();
        // Sibling key from the fragment survives the deep merge.
        assert_eq!(security["autonomy_level"].as_str(), Some("supervised"));
        // Arrays replace wholesale — no concatenation.
        let commands = security["allowed_commands"].as_array().unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].as_str(), Some("git"));
        // Nested table merges: overridden scalar wins, sibling is kept.
        let rate_limit = security["rate_limit"].as_table().unwrap();
        assert_eq!(rate_limit["per_minute"].as_integer(), Some(99));
        assert_eq!(rate_limit["per_hour"].as_integer(), Some(100));
        // Untouched fragment table survives whole.
        assert_eq!(
            merged["observability"].as_table().unwrap()["backend"].as_str(),
            Some("otlp")
        );
    }

    #[test]
    fn paths_resolve_relative_to_the_including_file() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "shared/fleet.toml", "layer = \"fleet\"\n");
        // The nested fragment's own include is relative to `shared/`, not to
        // the root config's directory.
        write(
            dir.path(),
            "shared/base.toml",
            "include = [\"fleet.toml\"]\nlayer = \"base\"\nbase_only = true\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"shared/base.toml\"]\n",
        );

        let merged = expand_file(&path).unwrap();
        assert_eq!(merged["layer"].as_str(), Some("base"));
        assert_eq!(merged["base_only"].as_bool(), Some(true));
    }

    #[test]
    fn nested_includes_expand_depth_first() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "deep.toml", "tier = \"deep\"\nd = true\n");
        write(
            dir.path(),
            "mid.toml",
            "include = [\"deep.toml\"]\ntier = \"mid\"\nm = true\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"mid.toml\"]\ntier = \"root\"\n",
        );

        let merged = expand_file(&path).unwrap();
        assert_eq!(merged["tier"].as_str(), Some("root"));
        assert_eq!(merged["m"].as_bool(), Some(true));
        assert_eq!(merged["d"].as_bool(), Some(true));
    }

    #[test]
    fn diamond_include_is_allowed() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "base.toml", "base = true\n");
        write(
            dir.path(),
            "left.toml",
            "include = [\"base.toml\"]\nleft = true\n",
        );
        write(
            dir.path(),
            "right.toml",
            "include = [\"base.toml\"]\nright = true\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"left.toml\", \"right.toml\"]\n",
        );

        let merged = expand_file(&path).unwrap();
        assert_eq!(merged["base"].as_bool(), Some(true));
        assert_eq!(merged["left"].as_bool(), Some(true));
        assert_eq!(merged["right"].as_bool(), Some(true));
    }

    #[test]
    fn cycle_is_an_error() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "a.toml", "include = [\"b.toml\"]\n");
        write(dir.path(), "b.toml", "include = [\"a.toml\"]\n");
        let path = write(dir.path(), "config.toml", "include = [\"a.toml\"]\n");

        let err = format!("{:#}", expand_file(&path).unwrap_err());
        assert!(err.contains("include cycle"), "{err}");
        assert!(err.contains("a.toml"), "{err}");
    }

    #[test]
    fn self_include_is_a_cycle() {
        let dir = TempDir::new().unwrap();
        let path = write(dir.path(), "config.toml", "include = [\"config.toml\"]\n");

        let err = format!("{:#}", expand_file(&path).unwrap_err());
        assert!(err.contains("include cycle"), "{err}");
    }

    #[test]
    fn missing_include_is_a_hard_error() {
        let dir = TempDir::new().unwrap();
        let path = write(dir.path(), "config.toml", "include = [\"nope.toml\"]\n");

        let err = format!("{:#}", expand_file(&path).unwrap_err());
        assert!(err.contains("nope.toml"), "{err}");
        assert!(err.contains("does not resolve to a readable file"), "{err}");
    }

    #[test]
    fn malformed_include_is_a_hard_error() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "broken.toml", "this is not = = toml\n");
        let path = write(dir.path(), "config.toml", "include = [\"broken.toml\"]\n");

        let err = format!("{:#}", expand_file(&path).unwrap_err());
        assert!(err.contains("Failed to parse config include"), "{err}");
    }

    #[test]
    fn non_array_include_is_an_error() {
        let dir = TempDir::new().unwrap();
        let path = write(dir.path(), "config.toml", "include = \"fleet.toml\"\n");

        let err = format!("{:#}", expand_file(&path).unwrap_err());
        assert!(err.contains("must be an array of paths"), "{err}");
    }

    #[test]
    fn malformed_root_is_left_to_the_caller() {
        let dir = TempDir::new().unwrap();
        let body = "include = [\"fleet.toml\"]\nbroken = = \n";
        let path = write(dir.path(), "config.toml", body);
        assert_eq!(expand(body, &path).unwrap(), body);
    }

    /// A shared fragment carrying real schema sections must survive
    /// deserialization: `include` is gone, and nothing trips the
    /// unknown-key path.
    #[test]
    fn expanded_output_deserializes_into_config() {
        let dir = TempDir::new().unwrap();
        write(
            dir.path(),
            "fleet.toml",
            "schema_version = 3\n\n[gateway]\nrequest_timeout_secs = 45\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"fleet.toml\"]\nschema_version = 3\n",
        );

        let expanded = expand(&fs::read_to_string(&path).unwrap(), &path).unwrap();
        let config = crate::migration::migrate_to_current(&expanded).expect("deserializes");
        assert_eq!(config.gateway.request_timeout_secs, 45);
        assert!(
            crate::schema::Config::unknown_keys(&expanded).is_empty(),
            "expanded config must not carry unknown keys"
        );
    }

    /// Env overrides run against the typed `Config` after expansion, so they
    /// outrank anything an include supplied.
    #[tokio::test]
    async fn env_override_beats_an_included_value() {
        let _guard = crate::env_overrides::env_test_lock().await;
        let dir = TempDir::new().unwrap();
        write(
            dir.path(),
            "fleet.toml",
            "schema_version = 3\n\n[gateway]\nrequest_timeout_secs = 45\n",
        );
        let path = write(
            dir.path(),
            "config.toml",
            "include = [\"fleet.toml\"]\nschema_version = 3\n",
        );

        let expanded = expand(&fs::read_to_string(&path).unwrap(), &path).unwrap();
        let mut config = crate::migration::migrate_to_current(&expanded).unwrap();
        assert_eq!(config.gateway.request_timeout_secs, 45);

        // SAFETY: env-mutating tests serialize on `env_test_lock()`.
        unsafe { std::env::set_var("ZEROCLAW_gateway__request_timeout_secs", "120") };
        let applied = crate::env_overrides::apply_env_overrides(&mut config);
        // SAFETY: as above.
        unsafe { std::env::remove_var("ZEROCLAW_gateway__request_timeout_secs") };

        let applied = applied.unwrap();
        assert!(applied.paths.contains("gateway.request_timeout_secs"));
        assert_eq!(config.gateway.request_timeout_secs, 120);
    }
}
