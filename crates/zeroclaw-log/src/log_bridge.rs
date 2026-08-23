//! Fail-closed bridge from the `log` facade into `tracing`.
//!
//! Dependencies log through `log`, not `tracing` — `whatsapp-rust` and
//! friends. Installing a `tracing` subscriber does nothing for them: `log`
//! keeps its own global logger slot, and while that slot is empty every
//! `log::warn!` in the dependency tree is discarded at the macro's own
//! max-level check. Those records reach neither stderr nor the JSONL trace,
//! so a transport failure inside a dependency leaves no evidence at any
//! verbosity.
//!
//! Filling that slot with a bare [`tracing_log::LogTracer`] would recover the
//! diagnostics *and* hand every third-party string on the record to
//! [`crate::layer::LogCaptureLayer`], which materializes them as event text
//! and attributes and persists them to `runtime-trace.jsonl` (rolling
//! persistence is on by default, at an `INFO` floor). Third-party call sites
//! are not ours to review: at the locked `whatsapp-rust` revision,
//! `src/pair_code.rs` logs the configured phone number and the generated pair
//! code at `INFO`, and other sites log JIDs. Those strings would bypass the
//! deliberate `LoginEvent::PairCode` → `ephemeral_attrs` boundary and
//! [`crate::writer::record_event`]'s guarantee that pairing credentials never
//! reach disk.
//!
//! # What crosses the boundary
//!
//! A `log::Record` carries four string-shaped channels — `args`, `target`,
//! `module_path`, `file` — plus a numeric `line`. None of the four is a
//! static-only channel: [`log::RecordBuilder`] takes a borrowed `&str` for
//! each of them, and the `log!` macros take a `target:` *expression*, not a
//! literal. So none of them may be forwarded on the assumption that the
//! dependency put a constant there.
//!
//! - **`args` (the message body) is dropped.** It is the record's free-text
//!   channel, written by code this workspace does not review, and nothing
//!   here can tell a harmless sentence from a name, a brand, an identifier or
//!   a credential. Every bridged record carries the fixed
//!   [`REDACTED_MESSAGE`] marker instead. There is no heuristic and no
//!   allowlist, so there is no rule to get wrong.
//! - **`module_path` and `file` are dropped.** They are unbounded strings and
//!   they are redundant: for a record logged without an explicit `target:`,
//!   `log` uses the module path *as* the target, so the target below already
//!   carries the same provenance in a bounded form.
//! - **`line` is forwarded.** It is a `u32`; it has no text channel to carry.
//! - **`target` is reduced to the safe representation below.** It is the one
//!   field the filters read, so dropping it would take `RUST_LOG` target
//!   selection — `RUST_LOG=whatsapp_rust=debug` — with it.
//!
//! # The safe target representation
//!
//! A target crosses **verbatim** only if it is non-empty, at most
//! [`MAX_TARGET_LEN`] bytes, and made entirely of ASCII alphanumerics, `_`,
//! `:` and `/`. Anything else is replaced *whole* by the constant
//! [`REDACTED_TARGET`] — never truncated and never rewritten character by
//! character, because a partial target leaks the fragments it kept.
//!
//! That charset is the union of the two target shapes this workspace's
//! dependency tree actually produces: Rust module paths (`whatsapp_rust`,
//! `whatsapp_rust::socket`), which are the default target of every `log!`
//! call without a `target:` argument, and the slash-separated component
//! targets the pinned `whatsapp-rust` revision writes by hand
//! (`Client/PairCode`, `Client/UnifiedSession` — 21 bytes at its longest).
//!
//! State the limit of this plainly: the charset bounds the target's *shape*,
//! not its meaning. An identifier-shaped word is an identifier-shaped word,
//! so a dependency that put `Alice` in a runtime target would still see
//! `Alice` cross. What the rule guarantees is that the surviving field is a
//! short, bounded, identifier-shaped token rather than an arbitrary string:
//! no whitespace, no punctuation, no `@`, no non-ASCII, nothing over
//! [`MAX_TARGET_LEN`] bytes, and so no room for a formatted sentence, a
//! phone number, a JID, a path or a credential. The three genuinely unbounded
//! channels are gone entirely; this one is kept, bounded, because the
//! filtering contract needs it.
//!
//! Note the deliberate contrast with [`zeroclaw_memory::redact`], which is
//! allow-by-default: it rewrites *recognized* patterns in user content the
//! operator opted into storing. Here the input is unreviewed third-party text
//! entering a credential-adjacent sink, so the text channels are not passed.
//!
//! [`zeroclaw_memory::redact`]: https://docs.rs/zeroclaw-memory

use tracing::level_filters::LevelFilter;
use tracing_log::AsTrace;

/// Fixed text substituted for every third-party `log` message body.
///
/// Not a per-token placeholder: the message is never inspected, so this
/// replaces the whole of it every time. Its presence in a record is the
/// signal that a dependency logged at that call site and that the wording was
/// withheld by design rather than lost.
pub(crate) const REDACTED_MESSAGE: &str = "[third-party message redacted]";

/// Fixed text substituted for a `log` target that is not in the safe
/// representation documented at the module level.
///
/// Whole-target replacement, for the same reason the message body is replaced
/// whole: a truncated or character-scrubbed target would still carry the
/// fragments it kept. Its own shape is deliberately outside the safe charset,
/// so it cannot be mistaken for a target a dependency chose and so
/// [`safe_target`] is idempotent over it.
pub(crate) const REDACTED_TARGET: &str = "[third-party target redacted]";

/// Byte ceiling for a target that crosses verbatim. Generous next to the
/// real ones — the longest target in the pinned `whatsapp-rust` revision is
/// 21 bytes — and small enough that nothing sentence-shaped fits.
pub(crate) const MAX_TARGET_LEN: usize = 128;

/// True for the bytes a verbatim target may contain: ASCII alphanumerics,
/// `_` and `:` for Rust module paths, `/` for the pinned dependency's
/// hand-written component targets.
fn is_safe_target_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'/')
}

/// The target as it is allowed to cross: the dependency's own value when it
/// is in the safe representation, [`REDACTED_TARGET`] otherwise.
///
/// Borrowing rather than allocating keeps this on the hot path of every
/// dependency record, including the ones the filters are about to discard.
fn safe_target(target: &str) -> &str {
    let bytes = target.as_bytes();
    if !bytes.is_empty()
        && bytes.len() <= MAX_TARGET_LEN
        && bytes.iter().all(|byte| is_safe_target_byte(*byte))
    {
        target
    } else {
        REDACTED_TARGET
    }
}

/// The `log` logger installed in the process-global slot. Forwards each
/// record into `tracing` through [`tracing_log::format_trace`], which is the
/// same dispatch [`tracing_log::LogTracer`] performs — identical callsite,
/// identical `log.target` / `log.line` normalization — except that the
/// message it carries is always [`REDACTED_MESSAGE`], its target is the safe
/// representation, and `module_path` / `file` are not forwarded at all.
struct RedactingLogBridge;

static BRIDGE: RedactingLogBridge = RedactingLogBridge;

impl log::Log for RedactingLogBridge {
    /// The same contract [`tracing_log::LogTracer`] implements: the global
    /// max level, then the active dispatcher. Without it `log_enabled!`
    /// answers `true` for every level and target — the bridge sets `log`'s
    /// own max level to `Trace` — and a dependency guarding an expensive
    /// diagnostic behind `log_enabled!` builds it even when the `EnvFilter`
    /// is `off`.
    ///
    /// The dispatcher is asked about the *safe* target, because that is the
    /// target the record will actually be dispatched under; asking about the
    /// raw one would let `log_enabled!` disagree with what gets recorded.
    ///
    /// How far that agreement reaches is bounded by `tracing-subscriber`, not
    /// by this bridge. `log` can only ask `Subscriber::enabled`, and a
    /// *per-layer* filter — what [`install_global_subscriber`] uses, since
    /// stderr and the recorded trace filter differently — deliberately does
    /// not answer through it: `Filtered::enabled` returns `true` so the other
    /// layers still get their say, and drops the event later in `on_event`.
    /// So under per-layer filters this answers from the process-wide max
    /// level, which still turns away the common case of a dependency's chatty
    /// `DEBUG`/`TRACE` tiers while the floor is `INFO`, but not a
    /// target-specific exclusion. A globally filtered subscriber is answered
    /// exactly.
    ///
    /// [`install_global_subscriber`]: crate::install_global_subscriber
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        if metadata.level().as_trace() > LevelFilter::current() {
            return false;
        }
        let metadata = log::Metadata::builder()
            .level(metadata.level())
            .target(safe_target(metadata.target()))
            .build();
        tracing::dispatcher::get_default(|dispatch| dispatch.enabled(&metadata.as_trace()))
    }

    fn log(&self, record: &log::Record<'_>) {
        let target = safe_target(record.target());
        // Ask the subscriber first so a record no layer wants costs nothing
        // to dispatch. `format_trace` repeats this check; doing it here keeps
        // a dependency's chatty `DEBUG`/`TRACE` tiers off the callsite
        // machinery when the filter floor is `INFO`.
        if !self.enabled(
            &log::Metadata::builder()
                .level(record.level())
                .target(target)
                .build(),
        ) {
            return;
        }
        // Rebuilt field by field rather than copied from the incoming record:
        // this list is the whole of what may cross the boundary, so a future
        // `log` release that grows another payload channel (structured
        // key-values, say) cannot ride along unreviewed. `args` is overwritten
        // rather than forwarded, `target` is the safe representation, and
        // `module_path` / `file` are left unset — the builder defaults them to
        // `None` and `tracing_log` then omits the fields entirely.
        let _ = tracing_log::format_trace(
            &log::Record::builder()
                .args(format_args!("{REDACTED_MESSAGE}"))
                .level(record.level())
                .target(target)
                .line(record.line())
                .build(),
        );
    }

    fn flush(&self) {}
}

/// Install the redacting bridge into the process-global `log` slot.
///
/// Fails when another logger already owns that slot; `log` permits exactly
/// one per process.
fn install() -> Result<(), log::SetLoggerError> {
    log::set_logger(&BRIDGE)?;
    // The bridge decides nothing about verbosity, so let every record reach
    // it and let the tracing filters do the filtering.
    log::set_max_level(log::LevelFilter::Trace);
    Ok(())
}

/// Production install: panics when the bridge cannot take the `log` slot.
///
/// A silent failure here is worse than a crash. The tracing subscriber is
/// installed by the time this runs, so discarding the error would leave the
/// daemon looking healthy while the dependency records this bridge exists to
/// recover stay missing — the exact invisible-failure mode the bridge was
/// added to end.
pub(crate) fn install_or_panic() {
    if let Err(err) = install() {
        panic!(
            "installing the `log` -> tracing bridge failed ({err}): another logger already \
             owns the process-global `log` slot, so dependency diagnostics would be lost \
             silently. Remove the competing `log::set_logger` call."
        );
    }
}

/// Test-only install: tolerates the slot already being taken.
///
/// Test binaries call [`crate::try_install_capture_subscriber`] once per test,
/// and `log` allows a single logger per process, so every call after the first
/// necessarily fails. The already-installed logger is this same bridge, so
/// ignoring the error is correct *here and only here*. Production goes through
/// [`install_or_panic`].
pub(crate) fn install_best_effort_for_tests() {
    let _ = install();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shapes the pinned dependency tree actually emits must cross
    /// untouched, or the bridge stops being addressable by `RUST_LOG` and
    /// stops naming which dependency spoke.
    #[test]
    fn real_dependency_targets_cross_verbatim() {
        for target in [
            "whatsapp_rust",
            "whatsapp_rust::socket",
            "Client/PairCode",
            "Client/UnifiedSession",
            "usync",
            "h2",
        ] {
            assert_eq!(
                safe_target(target),
                target,
                "a target the dependency tree really uses must cross verbatim"
            );
        }
    }

    /// Everything outside the documented representation is replaced whole:
    /// no truncation, no character scrubbing, so no fragment survives.
    #[test]
    fn targets_outside_the_safe_representation_are_replaced_whole() {
        let over_long = "a".repeat(MAX_TARGET_LEN + 1);
        for target in [
            "",
            "has space",
            "972501234567@s.whatsapp.net",
            "/etc/passwd\n",
            "Zoë Müller",
            "target=Alice",
            "quoted\"target",
            over_long.as_str(),
        ] {
            assert_eq!(
                safe_target(target),
                REDACTED_TARGET,
                "a target outside the safe representation must be replaced whole: {target:?}"
            );
        }
        // The boundary itself, so the cap is a cap and not an off-by-one.
        let at_limit = "a".repeat(MAX_TARGET_LEN);
        assert_eq!(safe_target(&at_limit), at_limit);
    }

    /// The replacement is itself outside the representation, so re-running
    /// the rule over an already-replaced target cannot resurrect anything.
    #[test]
    fn the_target_replacement_is_idempotent() {
        assert_eq!(safe_target(REDACTED_TARGET), REDACTED_TARGET);
    }
}
