//! WhatsApp Web channel using wa-rs (native Rust implementation)

use super::whatsapp_storage::RusqliteStore;
use anyhow::{Context, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::select;
use waproto::whatsapp::device_props::PlatformType;
use zeroclaw_api::channel::{
    Channel, ChannelConversationScope, ChannelMessage, FetchedMedia, SendMessage,
};
#[cfg(feature = "whatsapp-web")]
use zeroclaw_api::media::MediaAttachment;
use zeroclaw_runtime::i18n;

#[cfg(feature = "whatsapp-web")]
pub struct WhatsAppWebChannel {
    /// Session database path
    session_path: String,
    /// Phone number for pair code linking (optional)
    pair_phone: Option<String>,
    /// Custom pair code (optional)
    pair_code: Option<String>,
    /// Override WebSocket URL (test / proxy setups). Sourced from
    /// `[whatsapp.ws_url]` — replaces the legacy `WHATSAPP_WS_URL` env-var
    /// read.
    ws_url: Option<String>,
    /// Display name announced to contacts (optional)
    push_name: Option<String>,
    /// The alias key under `[channels.whatsapp.<alias>]` this handle is
    /// bound to. Used to scope peer-group writes and resolver lookups.
    alias: String,
    /// Resolves inbound external peers from canonical state at message-time.
    /// No cache (see AGENTS.md "ABSOLUTE RULE — SINGLE SOURCE OF TRUTH").
    peer_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    /// When true, only respond to messages that @-mention the bot in groups
    mention_only: bool,
    /// When true, allowed unaddressed group messages become context-only
    /// history entries instead of being dropped.
    passive_group_context: bool,
    /// Bot phone number (digits only), resolved from pair_phone or device identity at runtime
    bot_phone: Arc<Mutex<Option<String>>>,
    /// Bot LID number (digits only), resolved from device identity at runtime
    bot_lid: Arc<Mutex<Option<String>>>,
    /// Usage mode (business vs personal policy filtering)
    mode: zeroclaw_config::schema::WhatsAppWebMode,
    /// DM policy when mode = personal
    dm_policy: zeroclaw_config::schema::WhatsAppChatPolicy,
    /// Group policy when mode = personal
    group_policy: zeroclaw_config::schema::WhatsAppChatPolicy,
    /// Whether to always respond in self-chat when mode = personal
    self_chat_mode: bool,
    /// Bot handle for shutdown.
    /// whatsapp-rust 0.6: `Bot::run()` now returns `BotHandle` (a Future + abort)
    /// rather than a tokio JoinHandle directly (oxidezap/whatsapp-rust BotHandle wrapper).
    bot_handle: Arc<Mutex<Option<whatsapp_rust::bot::BotHandle>>>,
    /// Client handle for sending messages and typing indicators
    client: Arc<Mutex<Option<Arc<whatsapp_rust::Client>>>>,
    /// Message sender channel
    tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<ChannelMessage>>>>,
    /// Voice transcription (STT) config
    transcription: Option<zeroclaw_config::schema::TranscriptionConfig>,
    transcription_manager: Option<std::sync::Arc<super::transcription::TranscriptionManager>>,
    /// Text-to-speech runtime for voice replies (built from
    /// `tts_providers.<type>.<alias>`).
    tts_manager: Option<Arc<super::tts::TtsManager>>,
    /// Chats awaiting a voice reply — maps chat JID to the latest substantive
    /// reply text. A background task debounces and sends the voice note after
    /// the agent finishes its turn (no new send() for 3 seconds).
    pending_voice:
        Arc<std::sync::Mutex<std::collections::HashMap<String, (String, std::time::Instant)>>>,
    /// Chats whose last incoming message was a voice note.
    voice_chats: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Compiled mention patterns for DM mention gating.
    dm_mention_patterns: Arc<Vec<regex::Regex>>,
    /// Compiled mention patterns for group-chat mention gating.
    /// When non-empty, only group messages matching at least one pattern are
    /// processed; matched fragments are stripped from the forwarded content.
    group_mention_patterns: Arc<Vec<regex::Regex>>,
    /// Resolved channel workspace root used to bound outbound local media
    /// marker reads. The source of truth remains
    /// `Config::channel_workspace_dir("whatsapp.<alias>")`; this is the
    /// runtime trust boundary for file delivery.
    workspace_dir: Option<PathBuf>,
    /// Resolves allowed group chats from canonical config at message-time.
    /// Empty = all groups permitted. Direct messages bypass.
    allowed_groups_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    /// Optional pairing-persist handle to the canonical shared `Config`.
    /// `None` in tests; `Some` in the long-running daemon, wired via
    /// `.with_persistence(config)`. Same contract as WeChat's handle: on
    /// connect, the linked account is persisted into `peer_groups` through
    /// `crate::identity_persist` (no channel-local allowlist cache).
    persist: Option<Arc<parking_lot::RwLock<zeroclaw_config::schema::Config>>>,
}

impl WhatsAppWebChannel {
    #[cfg(feature = "whatsapp-web")]
    pub fn new(
        config: &zeroclaw_config::schema::WhatsAppConfig,
        alias: impl Into<String>,
        peer_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
        allowed_groups_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    ) -> Self {
        let session_path = config.session_path.clone().unwrap_or_default();
        let pair_phone = config.pair_phone.clone();
        let pair_code = config.pair_code.clone();
        let ws_url = config.ws_url.clone();
        let push_name = config.push_name.clone();
        let mention_only = config.mention_only;
        let passive_group_context = config.passive_group_context;
        let mode = config.mode.clone();
        let dm_policy = config.dm_policy.clone();
        let group_policy = config.group_policy.clone();
        let self_chat_mode = config.self_chat_mode;

        // Seed bot_phone from pair_phone (digits only)
        let bot_phone = pair_phone
            .as_ref()
            .map(|p| p.chars().filter(|c| c.is_ascii_digit()).collect::<String>())
            .filter(|digits| !digits.is_empty());

        if mention_only && bot_phone.is_none() {
            ::zeroclaw_log::record!(
                WARN,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                "mention_only enabled but pair_phone not set. \
                Bot identity will be resolved after connection. Group messages \
                will be skipped until identity is known."
            );
        }

        Self {
            session_path,
            pair_phone,
            pair_code,
            ws_url,
            push_name,
            alias: alias.into(),
            peer_resolver,
            mention_only,
            passive_group_context,
            bot_phone: Arc::new(Mutex::new(bot_phone)),
            bot_lid: Arc::new(Mutex::new(None)),
            mode,
            dm_policy,
            group_policy,
            self_chat_mode,
            allowed_groups_resolver,
            bot_handle: Arc::new(Mutex::new(None)),
            client: Arc::new(Mutex::new(None)),
            tx: Arc::new(Mutex::new(None)),
            transcription: None,
            transcription_manager: None,
            tts_manager: None,
            pending_voice: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            voice_chats: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            dm_mention_patterns: Arc::new(Vec::new()),
            group_mention_patterns: Arc::new(Vec::new()),
            workspace_dir: None,
            persist: None,
        }
    }

    /// Return the alias under `[channels.whatsapp.<alias>]` that this
    /// channel handle is bound to.
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Wire the shared Config handle so a completed pairing can persist the
    /// linked account into `peer_groups` and save — the same contract as
    /// `WeChatChannel::with_persistence`. The long-running daemon sets this
    /// from the orchestrator; tests and one-shot callers leave it unset
    /// (pairing works at runtime, doesn't persist).
    #[cfg(feature = "whatsapp-web")]
    pub fn with_persistence(
        mut self,
        config: Arc<parking_lot::RwLock<zeroclaw_config::schema::Config>>,
    ) -> Self {
        self.persist = Some(config);
        self
    }

    #[cfg(feature = "whatsapp-web")]
    pub fn with_workspace_dir(mut self, dir: PathBuf) -> Self {
        self.workspace_dir = Some(dir);
        self
    }

    /// Configure voice transcription (STT) for incoming voice notes.
    #[cfg(feature = "whatsapp-web")]
    pub fn with_transcription(
        mut self,
        config: zeroclaw_config::schema::TranscriptionConfig,
    ) -> Self {
        if !config.enabled {
            return self;
        }
        match super::transcription::TranscriptionManager::new(&config) {
            Ok(m) => {
                // `new` leaves the dispatch alias empty and this channel has no
                // agent to resolve one from, so bind the single configured
                // legacy provider — otherwise every `transcribe` call bails.
                self.transcription_manager =
                    Some(std::sync::Arc::new(m.with_single_legacy_provider(&config)));
                self.transcription = Some(config);
            }
            Err(e) => {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({"e": e.to_string()})),
                    "transcription manager init failed, voice transcription disabled"
                );
            }
        }
        self
    }

    #[cfg(feature = "whatsapp-web")]
    pub fn with_tts(mut self, config: &zeroclaw_config::schema::Config) -> Self {
        if config.tts.enabled {
            let owner = config.agent_for_channel(&format!("whatsapp.{}", self.alias));
            match super::tts::TtsManager::from_config_for_agent(config, owner) {
                Ok(m) => self.tts_manager = Some(Arc::new(m)),
                Err(e) => ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                    "TTS disabled"
                ),
            }
        }
        self
    }

    /// Set mention patterns for DM mention gating.
    /// Each pattern string is compiled as a case-insensitive regex.
    /// Invalid patterns are logged and skipped.
    #[cfg(feature = "whatsapp-web")]
    pub fn with_dm_mention_patterns(mut self, patterns: Vec<String>) -> Self {
        self.dm_mention_patterns = Arc::new(
            super::whatsapp::WhatsAppChannel::compile_mention_patterns(&patterns),
        );
        self
    }

    /// Set mention patterns for group-chat mention gating.
    /// Each pattern string is compiled as a case-insensitive regex.
    /// Invalid patterns are logged and skipped.
    #[cfg(feature = "whatsapp-web")]
    pub fn with_group_mention_patterns(mut self, patterns: Vec<String>) -> Self {
        self.group_mention_patterns = Arc::new(
            super::whatsapp::WhatsAppChannel::compile_mention_patterns(&patterns),
        );
        self
    }

    /// Check if a phone number is allowed (E.164 format: +1234567890)
    #[cfg(feature = "whatsapp-web")]
    fn is_number_allowed(&self, phone: &str) -> bool {
        let peers = (self.peer_resolver)();
        Self::is_number_allowed_for_list(&peers, phone)
    }

    #[cfg(feature = "whatsapp-web")]
    fn is_number_allowed_for_list(allowed_numbers: &[String], phone: &str) -> bool {
        // This channel historically accepted a surrounding-whitespace wildcard
        // (`entry.trim() == "*"`), which is broader than the shared helper's
        // exact `"*"` check, so keep that pre-check here.
        if allowed_numbers.iter().any(|entry| entry.trim() == "*") {
            return true;
        }
        crate::allowlist::is_user_allowed_by(allowed_numbers, phone, |entry, phone| {
            match (
                Self::normalize_phone_token(entry),
                Self::normalize_phone_token(phone),
            ) {
                (Some(entry_norm), Some(phone_norm)) => entry_norm == phone_norm,
                _ => false,
            }
        })
    }

    /// Normalize a phone-like token to canonical E.164 (`+<digits>`).
    /// Accepts raw numbers, `+` numbers, and JIDs (uses the user part before `@`).
    #[cfg(feature = "whatsapp-web")]
    fn normalize_phone_token(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        let user_part = trimmed
            .split_once('@')
            .map(|(user, _)| user)
            .unwrap_or(trimmed)
            .trim();

        let digits: String = user_part.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            None
        } else {
            Some(format!("+{digits}"))
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn lid_rejection_diagnostic(
        sender: &wacore_binary::jid::Jid,
        mapped_phone: Option<&str>,
    ) -> String {
        if !sender.is_lid() {
            return String::new();
        }
        if mapped_phone.is_none() {
            format!(
                " (LID→phone resolution returned None for sender {sender}; \
                 allowlist phone-number entries cannot match. Workaround: \
                 add the LID-form (+{}) to allowed_numbers, or wait for the \
                 in-memory LID cache to populate for this contact.)",
                sender.user
            )
        } else {
            " (sender is LID; resolved phone did not match any allowlist entry)".to_string()
        }
    }

    /// Build normalized sender candidates from sender JID, optional alt JID, and optional LID->PN mapping.
    #[cfg(feature = "whatsapp-web")]
    fn sender_phone_candidates(
        sender: &wacore_binary::jid::Jid,
        sender_alt: Option<&wacore_binary::jid::Jid>,
        mapped_phone: Option<&str>,
    ) -> Vec<String> {
        let mut candidates = Vec::new();

        let mut add_candidate = |candidate: Option<String>| {
            if let Some(candidate) = candidate
                && !candidates.iter().any(|existing| existing == &candidate)
            {
                candidates.push(candidate);
            }
        };

        add_candidate(Self::normalize_phone_token(&sender.to_string()));
        if let Some(alt) = sender_alt {
            add_candidate(Self::normalize_phone_token(&alt.to_string()));
        }
        if let Some(mapped_phone) = mapped_phone {
            add_candidate(Self::normalize_phone_token(mapped_phone));
        }

        candidates
    }

    #[cfg(feature = "whatsapp-web")]
    fn compute_reply_target(chat_jid: &str) -> String {
        // Pass through unchanged - library handles LID resolution internally
        chat_jid.to_string()
    }

    /// Resolve an outbound recipient. With whatsapp-rust 0.6+ and
    /// LID JIDs are handled internally by the library, so we pass through unchanged.
    #[cfg(feature = "whatsapp-web")]
    fn resolve_outbound_recipient(recipient: &str) -> String {
        // Pass through unchanged - library handles LID resolution internally
        recipient.trim().to_string()
    }

    /// Normalize phone number to E.164 format
    #[cfg(feature = "whatsapp-web")]
    fn normalize_phone(&self, phone: &str) -> String {
        if let Some(normalized) = Self::normalize_phone_token(phone) {
            return normalized;
        }

        let trimmed = phone.trim();
        let user_part = trimmed
            .split_once('@')
            .map(|(user, _)| user)
            .unwrap_or(trimmed);
        let normalized_user = user_part.trim_start_matches('+');
        format!("+{normalized_user}")
    }

    /// Whether the recipient string is a WhatsApp JID (contains a domain suffix).
    #[cfg(feature = "whatsapp-web")]
    fn is_jid(recipient: &str) -> bool {
        recipient.trim().contains('@')
    }

    /// Render a WhatsApp pairing QR payload into terminal-friendly text.
    #[cfg(feature = "whatsapp-web")]
    fn render_pairing_qr(code: &str) -> Result<String> {
        let payload = code.trim();
        if payload.is_empty() {
            anyhow::bail!("QR payload is empty");
        }

        let qr = qrcode::QrCode::new(payload.as_bytes()).map_err(|err| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"error": format!("{}", err)})),
                "Failed to encode WhatsApp Web QR payload"
            );
            anyhow::Error::msg(format!("Failed to encode WhatsApp Web QR payload: {err}"))
        })?;

        Ok(qr
            .render::<qrcode::render::unicode::Dense1x2>()
            .quiet_zone(true)
            .build())
    }

    #[cfg(feature = "whatsapp-web")]
    fn recipient_to_jid(&self, recipient: &str) -> Result<wacore_binary::jid::Jid> {
        let trimmed = recipient.trim();
        if trimmed.is_empty() {
            anyhow::bail!("Recipient cannot be empty");
        }

        if trimmed.contains('@') {
            return trimmed.parse::<wacore_binary::jid::Jid>().map_err(|e| {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({
                            "trimmed": trimmed,
                            "error": format!("{}", e),
                        })),
                    "whatsapp_web: invalid JID"
                );
                anyhow::Error::msg(format!("Invalid WhatsApp JID `{trimmed}`: {e}"))
            });
        }

        let digits: String = trimmed.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            anyhow::bail!("Recipient `{trimmed}` does not contain a valid phone number");
        }

        Ok(wacore_binary::jid::Jid::pn(digits))
    }

    // ── Reconnect state-machine helpers (used by listen() and tested directly) ──

    /// Reconnect retry constants.
    const MAX_RETRIES: u32 = 10;
    const BASE_DELAY_SECS: u64 = 3;
    const MAX_DELAY_SECS: u64 = 300;

    /// Compute the exponential-backoff delay for a given 1-based attempt number.
    /// Doubles each attempt from `BASE_DELAY_SECS`, capped at `MAX_DELAY_SECS`.
    fn compute_retry_delay(attempt: u32) -> u64 {
        std::cmp::min(
            Self::BASE_DELAY_SECS.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1))),
            Self::MAX_DELAY_SECS,
        )
    }

    /// Determine whether session files should be purged.
    /// Returns `true` only when `Event::LoggedOut` was explicitly observed.
    fn should_purge_session(session_revoked: &std::sync::atomic::AtomicBool) -> bool {
        session_revoked.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a reconnect attempt and return `(attempt_number, exceeded_max)`.
    fn record_retry(retry_count: &std::sync::atomic::AtomicU32) -> (u32, bool) {
        let attempts = retry_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        (attempts, attempts > Self::MAX_RETRIES)
    }

    /// Reset the retry counter (called on `Event::Connected`).
    fn reset_retry(retry_count: &std::sync::atomic::AtomicU32) {
        retry_count.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Expand `~` in a configured `session_path`. Single source of truth
    /// for the on-disk location — the run loop and the readiness probe
    /// must agree on the file they are looking at.
    fn expand_session_path(session_path: &str) -> String {
        shellexpand::tilde(session_path).to_string()
    }

    /// Channel-owned persisted-login probe: reports whether the session
    /// database at the configured `session_path` holds a device linked to a
    /// WhatsApp account (`device.pn` written by a completed QR pairing).
    /// Stricter than the run loop's resume check on purpose — a channel
    /// waiting for its QR scan persists an unregistered device row, which
    /// must not read as an authenticated login. Read-only; never creates
    /// the database or its sidecar files.
    pub fn has_persisted_session(session_path: &str) -> bool {
        if session_path.is_empty() {
            return false;
        }
        super::whatsapp_storage::persisted_device_exists(Self::expand_session_path(session_path))
    }

    /// Return the session file paths to remove (primary + WAL + SHM sidecars).
    fn session_file_paths(expanded_session_path: &str) -> [String; 3] {
        [
            expanded_session_path.to_string(),
            format!("{expanded_session_path}-wal"),
            format!("{expanded_session_path}-shm"),
        ]
    }

    /// Channel-owned relink hook: delete the persisted session so the next
    /// channel start finds no device and begins a fresh QR pairing.
    ///
    /// Removes the same triple the logged-out purge path removes —
    /// [`Self::session_file_paths`] is the single source of truth for both.
    /// Returns the paths actually removed; already absent files are not an
    /// error, so relinking an unpaired channel is a safe no-op that returns
    /// an empty list. Never creates the database.
    ///
    /// This only clears disk state. A currently running channel keeps its
    /// live connection until it is restarted; callers own scheduling that
    /// restart (e.g. a daemon reload).
    pub fn clear_persisted_session(session_path: &str) -> std::io::Result<Vec<String>> {
        let mut removed = Vec::new();
        if session_path.is_empty() {
            return Ok(removed);
        }
        let expanded = Self::expand_session_path(session_path);
        for path in Self::session_file_paths(&expanded) {
            match std::fs::remove_file(&path) {
                Ok(()) => removed.push(path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(removed)
    }

    /// Attempt to download and transcribe a WhatsApp voice note.
    /// Returns `None` if transcription is disabled, download fails, or
    /// transcription fails (all logged as warnings).
    #[cfg(feature = "whatsapp-web")]
    async fn try_transcribe_voice_note(
        client: &whatsapp_rust::Client,
        audio: &waproto::whatsapp::message::AudioMessage,
        transcription_config: Option<&zeroclaw_config::schema::TranscriptionConfig>,
        transcription_manager: Option<&super::transcription::TranscriptionManager>,
    ) -> Option<String> {
        let config = transcription_config?;
        let manager = transcription_manager?;

        // Enforce duration limit
        if let Some(seconds) = audio.seconds
            && u64::from(seconds) > config.max_duration_secs
        {
            ::zeroclaw_log::record!(
                INFO,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                &format!(
                    "skipping voice note ({}s exceeds {}s limit)",
                    seconds, config.max_duration_secs
                )
            );
            return None;
        }

        // Download the encrypted audio
        use whatsapp_rust::download::Downloadable;
        let audio_data = match client.download(audio as &dyn Downloadable).await {
            Ok(data) => data,
            Err(e) => {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                    "failed to download voice note"
                );
                return None;
            }
        };

        // Determine filename from mimetype for transcription API
        let file_name = match audio.mimetype.as_deref() {
            Some(m) if m.contains("opus") || m.contains("ogg") => "voice.ogg",
            Some(m) if m.contains("mp4") || m.contains("m4a") => "voice.m4a",
            Some(m) if m.contains("mpeg") || m.contains("mp3") => "voice.mp3",
            Some(m) if m.contains("webm") => "voice.webm",
            _ => "voice.ogg", // WhatsApp default
        };

        ::zeroclaw_log::record!(
            INFO,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!(
                "transcribing voice note ({} bytes, file={})",
                audio_data.len(),
                file_name
            )
        );

        match manager.transcribe(&audio_data, file_name).await {
            Ok(text) if text.trim().is_empty() => {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "voice transcription returned empty text, skipping"
                );
                None
            }
            Ok(text) => {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    &format!("voice note transcribed ({} chars)", text.len())
                );
                Some(text)
            }
            Err(e) => {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                    "voice transcription failed"
                );
                None
            }
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn extract_context_info(
        msg: &waproto::whatsapp::Message,
    ) -> Option<&waproto::whatsapp::ContextInfo> {
        use wacore::proto_helpers::MessageExt;
        let base = msg.get_base_message();

        if let Some(ref ext) = base.extended_text_message
            && let Some(ref ctx) = ext.context_info
        {
            return Some(ctx);
        }
        if let Some(ref img) = base.image_message
            && let Some(ref ctx) = img.context_info
        {
            return Some(ctx);
        }
        if let Some(ref vid) = base.video_message
            && let Some(ref ctx) = vid.context_info
        {
            return Some(ctx);
        }
        if let Some(ref doc) = base.document_message
            && let Some(ref ctx) = doc.context_info
        {
            return Some(ctx);
        }
        if let Some(ref aud) = base.audio_message
            && let Some(ref ctx) = aud.context_info
        {
            return Some(ctx);
        }
        if let Some(ref stk) = base.sticker_message
            && let Some(ref ctx) = stk.context_info
        {
            return Some(ctx);
        }

        None
    }

    #[cfg(feature = "whatsapp-web")]
    fn extract_quoted_message(
        msg: &waproto::whatsapp::Message,
    ) -> Option<&waproto::whatsapp::Message> {
        Self::extract_context_info(msg).and_then(|ctx| ctx.quoted_message.as_deref())
    }

    /// Maximum quoted characters carried into the `[Reply to …]` prefix.
    #[cfg(feature = "whatsapp-web")]
    const QUOTED_REPLY_PREFIX_MAX_CHARS: usize = 200;

    /// `[Reply to <id>: <quoted text>]` for a native WhatsApp reply, or `None`
    /// when the message quotes nothing. Without it a bare "yes" in a busy group
    /// reaches the model with no indication of what it answers — the quote is
    /// in `ContextInfo`, which nothing else reads.
    ///
    /// `ContextInfo` names the quoted author by `participant` JID and carries
    /// no push name, so the prefix states that id and leaves correlating it —
    /// against the bot's own number, or the ids on group sender labels — to the
    /// model. An id-less quote keeps the bare `[Reply to: …]`.
    #[cfg(feature = "whatsapp-web")]
    fn quoted_reply_prefix(msg: &waproto::whatsapp::Message) -> Option<String> {
        use wacore::proto_helpers::MessageExt;

        let ctx = Self::extract_context_info(msg)?;
        let quoted = ctx.quoted_message.as_deref()?;
        let text = Self::media_fallback_content(
            quoted.text_content().unwrap_or("").trim().to_string(),
            quoted,
        );
        // One line: the prefix sits above the message body, and a multi-line
        // quote would read as part of it.
        let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if text.is_empty() {
            return None;
        }
        let author = ctx
            .participant
            .as_deref()
            .map(Self::jid_digits)
            .filter(|id| !id.is_empty())
            .map(|id| format!(" {id}"))
            .unwrap_or_default();
        Some(format!(
            "[Reply to{author}: {}]",
            crate::util::truncate_with_ellipsis(&text, Self::QUOTED_REPLY_PREFIX_MAX_CHARS)
        ))
    }

    /// Prepend the reply prefix to a turn body, when there is one to prepend.
    #[cfg(feature = "whatsapp-web")]
    fn with_quoted_reply_prefix(content: String, msg: &waproto::whatsapp::Message) -> String {
        match Self::quoted_reply_prefix(msg) {
            Some(prefix) => format!("{prefix}\n{content}"),
            None => content,
        }
    }

    /// `[Your id: <digits>[, <digits>]]` — the bot's own phone and LID as the
    /// live session knows them, or `None` before the session resolves either.
    ///
    /// Sender labels and reply prefixes state ids; the model can only answer
    /// "is that one me?" if its own ids are in the turn too. They come from
    /// session state rather than an authored identity file so they cannot go
    /// stale or belong to another instance.
    #[cfg(feature = "whatsapp-web")]
    fn self_id_disclosure(
        bot_phone: &Mutex<Option<String>>,
        bot_lid: &Mutex<Option<String>>,
    ) -> Option<String> {
        let mut ids: Vec<String> = Vec::new();
        for known in [bot_phone.lock().clone(), bot_lid.lock().clone()]
            .into_iter()
            .flatten()
        {
            let id = Self::jid_digits(&known);
            if !id.is_empty() && !ids.contains(&id) {
                ids.push(id);
            }
        }
        (!ids.is_empty()).then(|| format!("[Your id: {}]", ids.join(", ")))
    }

    /// The turn body the agent sees: what the message says, what it quotes,
    /// and which ids are the bot's own.
    #[cfg(feature = "whatsapp-web")]
    fn with_turn_context(
        content: String,
        msg: &waproto::whatsapp::Message,
        bot_phone: &Mutex<Option<String>>,
        bot_lid: &Mutex<Option<String>>,
    ) -> String {
        let content = Self::with_quoted_reply_prefix(content, msg);
        match Self::self_id_disclosure(bot_phone, bot_lid) {
            Some(disclosure) => format!("{disclosure}\n{content}"),
            None => content,
        }
    }

    /// Longest sender-supplied document name kept as the attachment name.
    #[cfg(feature = "whatsapp-web")]
    const DOCUMENT_NAME_MAX_CHARS: usize = 100;

    /// A bare, bounded file name for a document. The sender chooses this
    /// string, so path separators and control characters are stripped before
    /// it reaches anything that might write it out.
    #[cfg(feature = "whatsapp-web")]
    fn document_file_name(supplied: Option<&str>, mime: &str) -> String {
        let base_name = supplied
            .unwrap_or_default()
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or_default();
        let cleaned: String = base_name
            .chars()
            .filter(|ch| !ch.is_control())
            .take(Self::DOCUMENT_NAME_MAX_CHARS)
            .collect();
        let cleaned = cleaned.trim().trim_start_matches('.').trim();

        if cleaned.is_empty() {
            format!("whatsapp-document.{}", Self::mime_extension(mime, "bin"))
        } else {
            cleaned.to_string()
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn mime_extension(mime: &str, fallback: &str) -> String {
        let subtype = mime
            .split(';')
            .next()
            .and_then(|clean| clean.split_once('/').map(|(_, subtype)| subtype))
            .and_then(|subtype| subtype.split('+').next())
            .filter(|subtype| {
                !subtype.is_empty()
                    && subtype
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
            })
            .unwrap_or(fallback);

        match subtype {
            "jpeg" => "jpg".to_string(),
            "svg+xml" => "svg".to_string(),
            other => other.to_string(),
        }
    }

    #[cfg(feature = "whatsapp-web")]
    async fn push_downloaded_attachment(
        client: &whatsapp_rust::Client,
        downloadable: &dyn whatsapp_rust::download::Downloadable,
        file_name: String,
        mime_type: Option<String>,
        attachments: &mut Vec<MediaAttachment>,
    ) {
        let data = match client.download(downloadable).await {
            Ok(data) => data,
            Err(e) => {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({
                            "file": file_name,
                            "error": format!("{}", e),
                        })),
                    "failed to download WhatsApp media attachment"
                );
                return;
            }
        };

        attachments.push(MediaAttachment {
            file_name,
            data,
            mime_type,
        });
    }

    #[cfg(feature = "whatsapp-web")]
    async fn collect_media_attachments(
        client: &whatsapp_rust::Client,
        msg: &waproto::whatsapp::Message,
        file_prefix: &str,
        include_audio: bool,
        attachments: &mut Vec<MediaAttachment>,
    ) {
        use wacore::proto_helpers::MessageExt;
        use whatsapp_rust::download::Downloadable;

        let base = msg.get_base_message();

        if let Some(ref image) = base.image_message {
            let mime = image
                .mimetype
                .clone()
                .unwrap_or_else(|| "image/jpeg".to_string());
            let file_name = format!(
                "{file_prefix}whatsapp-image.{}",
                Self::mime_extension(&mime, "jpg")
            );
            Self::push_downloaded_attachment(
                client,
                image.as_ref() as &dyn Downloadable,
                file_name,
                Some(mime),
                attachments,
            )
            .await;
        }

        if let Some(ref video) = base.video_message {
            let mime = video
                .mimetype
                .clone()
                .unwrap_or_else(|| "video/mp4".to_string());
            let file_name = format!(
                "{file_prefix}whatsapp-video.{}",
                Self::mime_extension(&mime, "mp4")
            );
            Self::push_downloaded_attachment(
                client,
                video.as_ref() as &dyn Downloadable,
                file_name,
                Some(mime),
                attachments,
            )
            .await;
        }

        if include_audio && let Some(ref audio) = base.audio_message {
            let mime = audio
                .mimetype
                .clone()
                .unwrap_or_else(|| "audio/ogg".to_string());
            let file_name = format!(
                "{file_prefix}whatsapp-audio.{}",
                Self::mime_extension(&mime, "ogg")
            );
            Self::push_downloaded_attachment(
                client,
                audio.as_ref() as &dyn Downloadable,
                file_name,
                Some(mime),
                attachments,
            )
            .await;
        }

        if let Some(ref document) = base.document_message {
            let mime = document
                .mimetype
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string());
            let file_name = format!(
                "{file_prefix}{}",
                Self::document_file_name(
                    document.file_name.as_deref().or(document.title.as_deref()),
                    &mime,
                )
            );
            Self::push_downloaded_attachment(
                client,
                document.as_ref() as &dyn Downloadable,
                file_name,
                Some(mime),
                attachments,
            )
            .await;
        }

        if let Some(ref sticker) = base.sticker_message {
            let mime = sticker
                .mimetype
                .clone()
                .unwrap_or_else(|| "image/webp".to_string());
            let file_name = format!(
                "{file_prefix}whatsapp-sticker.{}",
                Self::mime_extension(&mime, "webp")
            );
            Self::push_downloaded_attachment(
                client,
                sticker.as_ref() as &dyn Downloadable,
                file_name,
                Some(mime),
                attachments,
            )
            .await;
        }
    }

    /// `[Marker]`, or `[Marker]\n\n<caption>` when the media carries one —
    /// the shape the Telegram channel already uses for captioned attachments.
    /// `text_content()` never reads a caption, so without this the question
    /// an operator typed alongside a photo is dropped.
    #[cfg(feature = "whatsapp-web")]
    fn captioned_marker(marker: &str, caption: Option<&str>) -> String {
        match caption.map(str::trim).filter(|c| !c.is_empty()) {
            Some(caption) => format!("{marker}\n\n{caption}"),
            None => marker.to_string(),
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content(content: String, msg: &waproto::whatsapp::Message) -> String {
        if !content.is_empty() {
            return content;
        }

        use wacore::proto_helpers::MessageExt;
        let base = msg.get_base_message();

        if base.sticker_message.is_some() {
            return "[Sticker]".to_string();
        }
        if let Some(ref image) = base.image_message {
            return Self::captioned_marker("[Image]", image.caption.as_deref());
        }
        if let Some(ref video) = base.video_message {
            return Self::captioned_marker("[Video]", video.caption.as_deref());
        }
        if let Some(ref document) = base.document_message {
            return Self::captioned_marker("[Document]", document.caption.as_deref());
        }
        // Reached only when transcription is off or failed — the transcribed
        // text is spliced in as `[Voice] <text>` before this runs. Without a
        // marker the content stays empty, the caller returns early, and the
        // bot answers a voice note with silence.
        if let Some(ref audio) = base.audio_message {
            return if audio.ptt == Some(true) {
                "[Voice]".to_string()
            } else {
                "[Audio]".to_string()
            };
        }
        if let Some(ref loc) = base.location_message {
            // Live locations are silently ignored — they stream
            // periodic updates and have no meaningful static content.
            if loc.is_live == Some(true) {
                return String::new();
            }
            let lat = match loc.degrees_latitude {
                Some(l) => l,
                None => return String::new(),
            };
            let lng = match loc.degrees_longitude {
                Some(l) => l,
                None => return String::new(),
            };
            return crate::util::format_location_content(lat, lng, loc.name.as_deref());
        }

        String::new()
    }

    /// Build a media claim ticket for one message's own media: a base64 JSON
    /// descriptor carrying exactly what `Client::download_from_params` needs
    /// to redeem it later. Audio, image and document — the kinds the redeem
    /// path can fetch. View-once media never becomes a ticket: a durable,
    /// replayable claim would defeat the sender's one-view intent.
    #[cfg(feature = "whatsapp-web")]
    fn media_ref_descriptor(msg: &waproto::whatsapp::Message) -> Option<String> {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use wacore::proto_helpers::MessageExt;
        if msg.is_view_once() {
            return None;
        }
        let base = msg.get_base_message();
        let (kind, mime, dp, mk, es, fs, len) = if let Some(ref a) = base.audio_message {
            (
                "audio",
                a.mimetype.clone(),
                a.direct_path.clone(),
                a.media_key.clone(),
                a.file_enc_sha256.clone(),
                a.file_sha256.clone(),
                a.file_length,
            )
        } else if let Some(ref i) = base.image_message {
            (
                "image",
                i.mimetype.clone(),
                i.direct_path.clone(),
                i.media_key.clone(),
                i.file_enc_sha256.clone(),
                i.file_sha256.clone(),
                i.file_length,
            )
        } else if let Some(ref d) = base.document_message {
            (
                "document",
                d.mimetype.clone(),
                d.direct_path.clone(),
                d.media_key.clone(),
                d.file_enc_sha256.clone(),
                d.file_sha256.clone(),
                d.file_length,
            )
        } else {
            return None;
        };
        let doc = serde_json::json!({
            "v": 1,
            "k": kind,
            "dp": dp?,
            "mk": STANDARD.encode(mk?),
            "es": STANDARD.encode(es?),
            "fs": fs.map(|b| STANDARD.encode(b)),
            "len": len.unwrap_or(0),
            "mime": mime,
        });
        Some(STANDARD.encode(doc.to_string()))
    }

    /// Claim tickets for everything redeemable a message carries: its own
    /// media and its quoted message's media. Both lanes mint these — the
    /// passive lane because it never downloads, the active lane so media
    /// stays redeemable after the landed file ages out of retention.
    #[cfg(feature = "whatsapp-web")]
    fn media_ref_descriptors(msg: &waproto::whatsapp::Message) -> Vec<String> {
        let mut descriptors = Vec::new();
        if let Some(desc) = Self::media_ref_descriptor(msg) {
            descriptors.push(desc);
        }
        if let Some(quoted) = Self::extract_quoted_message(msg)
            && let Some(desc) = Self::media_ref_descriptor(quoted)
        {
            descriptors.push(desc);
        }
        descriptors
    }

    /// Append claim tickets under the content, one line each.
    #[cfg(feature = "whatsapp-web")]
    fn append_media_refs(content: String, msg: &waproto::whatsapp::Message) -> String {
        let mut content = content;
        for desc in Self::media_ref_descriptors(msg) {
            content = format!(
                "{content}\n{}{desc}]",
                zeroclaw_api::media_ref::MEDIA_REF_PREFIX
            );
        }
        content
    }

    #[cfg(feature = "whatsapp-web")]
    fn group_context_scope(
        passive_group_context: bool,
        is_group: bool,
    ) -> ChannelConversationScope {
        if passive_group_context && is_group {
            ChannelConversationScope::ReplyTarget
        } else {
            ChannelConversationScope::Sender
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn should_record_passive_group_context(
        passive_group_context: bool,
        is_group: bool,
        addressed_to_bot: bool,
    ) -> bool {
        passive_group_context && is_group && !addressed_to_bot
    }

    #[cfg(feature = "whatsapp-web")]
    async fn send_inbound_channel_message(
        tx: &tokio::sync::mpsc::Sender<ChannelMessage>,
        alias: &str,
        sender: &str,
        sender_display_name: Option<&str>,
        reply_target: String,
        content: String,
        attachments: Vec<MediaAttachment>,
        passive_context: bool,
        explicitly_addressed: bool,
        conversation_scope: ChannelConversationScope,
    ) {
        if let Err(e) = tx
            .send(ChannelMessage {
                id: uuid::Uuid::new_v4().to_string(),
                channel: "whatsapp".to_string(),
                channel_alias: Some(alias.to_string()),
                sender: sender.to_string(),
                sender_display_name: sender_display_name
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(ToOwned::to_owned),
                // Reply to the originating chat JID (DM or group), passed
                // through unchanged (library handles LID addressing internally).
                reply_target,
                content,
                timestamp: chrono::Utc::now().timestamp() as u64,
                thread_ts: None,
                interruption_scope_id: None,
                attachments,
                subject: None,
                internal_sop_event: None,
                passive_context,
                explicitly_addressed,
                conversation_scope,
            })
            .await
        {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                "failed to send message to channel"
            );
        }
    }

    /// Synthesize text to speech and send as a WhatsApp voice note (static version for spawned tasks).
    #[cfg(feature = "whatsapp-web")]
    async fn synthesize_voice_static(
        client: &whatsapp_rust::Client,
        to: &wacore_binary::jid::Jid,
        text: &str,
        tts_manager: &super::tts::TtsManager,
    ) -> Result<()> {
        let audio_bytes = tts_manager.synthesize_opus(text).await?;
        let audio_len = audio_bytes.len();
        ::zeroclaw_log::record!(
            INFO,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!("TTS: synthesized {} bytes of audio", audio_len)
        );

        if audio_bytes.is_empty() {
            anyhow::bail!("TTS returned empty audio");
        }

        use wacore::download::MediaType;
        use whatsapp_rust::upload::UploadOptions;
        let upload = client
            .upload(audio_bytes, MediaType::Audio, UploadOptions::default())
            .await
            .map_err(|e| {
                ::zeroclaw_log::record!(
                    ERROR,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                    "Failed to upload TTS audio"
                );
                anyhow::Error::msg(format!("Failed to upload TTS audio: {e}"))
            })?;

        ::zeroclaw_log::record!(
            INFO,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!(
                "TTS: uploaded audio (url_len={}, file_length={})",
                upload.url.len(),
                upload.file_length
            )
        );

        // Estimate duration from file size: Opus at ~32 kbps → bytes / 4000 ≈ seconds
        #[allow(clippy::cast_possible_truncation)]
        let estimated_seconds = std::cmp::max(1, (upload.file_length / 4000) as u32);

        // whatsapp-rust 0.6: UploadResponse cryptographic fields became
        // `[u8; 32]` for type safety. Pull the Vec<u8> copies before
        // consuming the strings so the partial-move on `upload.direct_path`
        // doesn't bite.
        let media_key = upload.media_key_vec();
        let file_enc_sha256 = upload.file_enc_sha256_vec();
        let file_sha256 = upload.file_sha256_vec();
        let voice_msg = waproto::whatsapp::Message {
            audio_message: Some(Box::new(waproto::whatsapp::message::AudioMessage {
                url: Some(upload.url),
                direct_path: Some(upload.direct_path),
                media_key: Some(media_key),
                file_enc_sha256: Some(file_enc_sha256),
                file_sha256: Some(file_sha256),
                file_length: Some(upload.file_length),
                mimetype: Some("audio/ogg; codecs=opus".to_string()),
                ptt: Some(true),
                seconds: Some(estimated_seconds),
                ..Default::default()
            })),
            ..Default::default()
        };

        Box::pin(client.send_message(to.clone(), voice_msg))
            .await
            .map_err(|e| {
                ::zeroclaw_log::record!(
                    ERROR,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                    "Failed to send voice note"
                );
                anyhow::Error::msg(format!("Failed to send voice note: {e}"))
            })?;
        ::zeroclaw_log::record!(
            INFO,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!(
                "TTS: sent voice note ({} bytes, ~{}s)",
                audio_len, estimated_seconds
            )
        );
        Ok(())
    }

    #[cfg(feature = "whatsapp-web")]
    async fn send_media_marker(
        client: &whatsapp_rust::Client,
        to: &wacore_binary::jid::Jid,
        marker: &WhatsAppMediaMarker,
        path: &Path,
    ) -> Result<()> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("read WhatsApp marker target {}", path.display()))?;
        if bytes.is_empty() {
            anyhow::bail!("WhatsApp marker target {} is empty", path.display());
        }

        let media_type = marker.kind.media_type();
        let mime = marker.kind.mime_for_path(path);

        use whatsapp_rust::upload::UploadOptions;
        let upload = client
            .upload(bytes, media_type, UploadOptions::default())
            .await
            .map_err(|e| anyhow::Error::msg(format!("WhatsApp media upload failed: {e}")))?;

        let media_key = upload.media_key_vec();
        let file_enc_sha256 = upload.file_enc_sha256_vec();
        let file_sha256 = upload.file_sha256_vec();
        let outgoing = match marker.kind {
            WhatsAppMediaKind::Image => waproto::whatsapp::Message {
                image_message: Some(Box::new(waproto::whatsapp::message::ImageMessage {
                    url: Some(upload.url),
                    direct_path: Some(upload.direct_path),
                    media_key: Some(media_key),
                    file_enc_sha256: Some(file_enc_sha256),
                    file_sha256: Some(file_sha256),
                    file_length: Some(upload.file_length),
                    mimetype: Some(mime),
                    ..Default::default()
                })),
                ..Default::default()
            },
            WhatsAppMediaKind::Video => waproto::whatsapp::Message {
                video_message: Some(Box::new(waproto::whatsapp::message::VideoMessage {
                    url: Some(upload.url),
                    direct_path: Some(upload.direct_path),
                    media_key: Some(media_key),
                    file_enc_sha256: Some(file_enc_sha256),
                    file_sha256: Some(file_sha256),
                    file_length: Some(upload.file_length),
                    mimetype: Some(mime),
                    ..Default::default()
                })),
                ..Default::default()
            },
            WhatsAppMediaKind::Audio | WhatsAppMediaKind::Voice => {
                #[allow(clippy::cast_possible_truncation)]
                let estimated_seconds = std::cmp::max(1, (upload.file_length / 4000) as u32);
                waproto::whatsapp::Message {
                    audio_message: Some(Box::new(waproto::whatsapp::message::AudioMessage {
                        url: Some(upload.url),
                        direct_path: Some(upload.direct_path),
                        media_key: Some(media_key),
                        file_enc_sha256: Some(file_enc_sha256),
                        file_sha256: Some(file_sha256),
                        file_length: Some(upload.file_length),
                        mimetype: Some(mime),
                        ptt: Some(matches!(marker.kind, WhatsAppMediaKind::Voice)),
                        seconds: Some(estimated_seconds),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }
            WhatsAppMediaKind::Document => {
                let file_name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("attachment")
                    .to_string();
                waproto::whatsapp::Message {
                    document_message: Some(Box::new(waproto::whatsapp::message::DocumentMessage {
                        url: Some(upload.url),
                        direct_path: Some(upload.direct_path),
                        media_key: Some(media_key),
                        file_enc_sha256: Some(file_enc_sha256),
                        file_sha256: Some(file_sha256),
                        file_length: Some(upload.file_length),
                        mimetype: Some(mime),
                        file_name: Some(file_name.clone()),
                        title: Some(file_name),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }
        };

        Box::pin(client.send_message(to.clone(), outgoing))
            .await
            .map_err(|e| anyhow::Error::msg(format!("WhatsApp media send failed: {e}")))?;
        Ok(())
    }

    /// Send a native location pin. No file read or media upload is involved —
    /// the coordinates and labels travel inline in a `LocationMessage`.
    #[cfg(feature = "whatsapp-web")]
    async fn send_location(
        client: &whatsapp_rust::Client,
        to: &wacore_binary::jid::Jid,
        loc: &WhatsAppLocation,
    ) -> Result<()> {
        let outgoing = waproto::whatsapp::Message {
            location_message: Some(Box::new(waproto::whatsapp::message::LocationMessage {
                degrees_latitude: Some(loc.lat),
                degrees_longitude: Some(loc.lng),
                name: loc.name.clone(),
                address: loc.address.clone(),
                ..Default::default()
            })),
            ..Default::default()
        };
        Box::pin(client.send_message(to.clone(), outgoing))
            .await
            .map_err(|e| anyhow::Error::msg(format!("WhatsApp location send failed: {e}")))?;
        Ok(())
    }

    /// The display name to push, or `None` when it is unset, blank, or
    /// already what the device announces.
    #[cfg(feature = "whatsapp-web")]
    fn push_name_to_apply<'a>(configured: Option<&'a str>, current: &str) -> Option<&'a str> {
        let desired = configured?.trim();
        (!desired.is_empty() && desired != current.trim()).then_some(desired)
    }

    // ── Mention detection helpers (used when mention_only is enabled) ──

    /// Extract digits from a JID string (e.g. "919211916069@s.whatsapp.net" -> "919211916069").
    #[cfg(feature = "whatsapp-web")]
    fn jid_digits(jid: &str) -> String {
        let user_part = jid.split_once('@').map(|(u, _)| u).unwrap_or(jid);
        let user_part = user_part
            .split_once(':')
            .map(|(u, _)| u)
            .unwrap_or(user_part);
        user_part.chars().filter(|c| c.is_ascii_digit()).collect()
    }

    #[cfg(feature = "whatsapp-web")]
    fn store_jid_digits(slot: &Arc<Mutex<Option<String>>>, jid: &str) -> Option<String> {
        let digits = Self::jid_digits(jid);
        if digits.is_empty() {
            None
        } else {
            *slot.lock() = Some(digits.clone());
            Some(digits)
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn jid_matches_bot(jid: &str, bot_phone: &str, bot_lid: Option<&str>) -> bool {
        let digits = Self::jid_digits(jid);
        !digits.is_empty()
            && ((!bot_phone.is_empty() && digits == bot_phone)
                || bot_lid.is_some_and(|lid| !lid.is_empty() && digits == lid))
    }

    /// Extract mentioned JIDs from the base (unwrapped) message's context_info.
    #[cfg(feature = "whatsapp-web")]
    fn extract_mentioned_jids(msg: &waproto::whatsapp::Message) -> Vec<String> {
        Self::extract_context_info(msg)
            .map(|ctx| ctx.mentioned_jid.clone())
            .unwrap_or_default()
    }

    /// Check whether the bot is mentioned -- either structurally or via text fallback.
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention(
        text: &str,
        mentioned_jids: &[String],
        bot_phone: &str,
        bot_lid: Option<&str>,
    ) -> bool {
        // 1. Structured: check if any mentioned_jid's digits match the bot's phone or LID digits
        for jid in mentioned_jids {
            if Self::jid_matches_bot(jid, bot_phone, bot_lid) {
                return true;
            }
        }

        // 2. Text fallback: word-boundary-aware match for @<bot_digits>.
        //    Scan all occurrences -- an earlier prefix false-match must not mask a later real mention.
        fn has_text_mention(text: &str, digits: &str) -> bool {
            if digits.is_empty() {
                return false;
            }

            let pattern = format!("@{digits}");
            let mut search_from = 0;
            while let Some(rel_pos) = text[search_from..].find(&pattern) {
                let pos = search_from + rel_pos;
                let after_idx = pos + pattern.len();
                let leading_ok = pos == 0
                    || text[..pos]
                        .chars()
                        .next_back()
                        .is_none_or(|ch| !ch.is_ascii_alphanumeric());
                let trailing_ok = text[after_idx..]
                    .chars()
                    .next()
                    .is_none_or(|ch| !ch.is_ascii_digit());
                if leading_ok && trailing_ok {
                    return true;
                }
                search_from = after_idx;
            }
            false
        }

        has_text_mention(text, bot_phone) || bot_lid.is_some_and(|lid| has_text_mention(text, lid))
    }

    /// Extract the author JID of the message quoted by a reply.
    #[cfg(feature = "whatsapp-web")]
    fn extract_reply_participant(msg: &waproto::whatsapp::Message) -> Option<&str> {
        Self::extract_context_info(msg).and_then(|ctx| ctx.participant.as_deref())
    }

    #[cfg(feature = "whatsapp-web")]
    fn is_reply_to_bot(
        msg: &waproto::whatsapp::Message,
        bot_phone: &str,
        bot_lid: Option<&str>,
    ) -> bool {
        Self::extract_reply_participant(msg)
            .is_some_and(|participant| Self::jid_matches_bot(participant, bot_phone, bot_lid))
    }

    #[cfg(feature = "whatsapp-web")]
    fn is_message_addressed_to_bot(
        msg: &waproto::whatsapp::Message,
        text: &str,
        bot_phone: &str,
        bot_lid: Option<&str>,
    ) -> bool {
        let mentioned_jids = Self::extract_mentioned_jids(msg);
        Self::contains_bot_mention(text, &mentioned_jids, bot_phone, bot_lid)
            || Self::is_reply_to_bot(msg, bot_phone, bot_lid)
    }

    /// `is_message_addressed_to_bot` against the live bot identity, or `None`
    /// while that identity is still unknown and the question has no answer.
    #[cfg(feature = "whatsapp-web")]
    fn addressed_to_bot(
        msg: &waproto::whatsapp::Message,
        text: &str,
        bot_phone: &Mutex<Option<String>>,
        bot_lid: &Mutex<Option<String>>,
    ) -> Option<bool> {
        let bot_phone = bot_phone.lock();
        let bot_lid = bot_lid.lock();
        (bot_phone.is_some() || bot_lid.is_some()).then(|| {
            Self::is_message_addressed_to_bot(
                msg,
                text,
                bot_phone.as_deref().unwrap_or(""),
                bot_lid.as_deref(),
            )
        })
    }
}

#[cfg(feature = "whatsapp-web")]
fn fromme_outside_self_chat_is_operator_trigger(
    is_group: bool,
    dm_mention_patterns: &[regex::Regex],
    group_mention_patterns: &[regex::Regex],
    text: &str,
) -> bool {
    let applicable = if is_group {
        group_mention_patterns
    } else {
        dm_mention_patterns
    };
    if applicable.is_empty() {
        return false;
    }
    super::whatsapp::WhatsAppChannel::text_matches_patterns(applicable, text)
}

#[cfg(feature = "whatsapp-web")]
fn is_group_chat_allowed(chat_jid: &str, allowed_groups: &[String]) -> bool {
    if allowed_groups.is_empty() {
        return true;
    }
    let chat_user = chat_jid
        .split_once('@')
        .map(|(user, _)| user)
        .unwrap_or(chat_jid);
    allowed_groups.iter().any(|entry| {
        let entry = entry.trim();
        !entry.is_empty() && (entry == chat_jid || entry == chat_user)
    })
}

#[cfg(feature = "whatsapp-web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhatsAppMediaKind {
    Image,
    Document,
    Video,
    Audio,
    Voice,
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppMediaKind {
    fn from_marker(kind: &str) -> Option<Self> {
        match kind {
            "IMAGE" | "PHOTO" => Some(Self::Image),
            "DOCUMENT" | "FILE" => Some(Self::Document),
            "VIDEO" => Some(Self::Video),
            "AUDIO" => Some(Self::Audio),
            "VOICE" => Some(Self::Voice),
            _ => None,
        }
    }

    fn media_type(self) -> wacore::download::MediaType {
        match self {
            Self::Image => wacore::download::MediaType::Image,
            Self::Document => wacore::download::MediaType::Document,
            Self::Video => wacore::download::MediaType::Video,
            Self::Audio | Self::Voice => wacore::download::MediaType::Audio,
        }
    }

    fn mime_for_path(self, path: &Path) -> String {
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase);
        if matches!(self, Self::Voice) && matches!(ext.as_deref(), Some("ogg" | "oga" | "opus")) {
            return "audio/ogg; codecs=opus".to_string();
        }
        match ext.as_deref() {
            Some("png") => "image/png",
            Some("jpg" | "jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("webp") => "image/webp",
            Some("bmp") => "image/bmp",
            Some("mp4") => "video/mp4",
            Some("mov") => "video/quicktime",
            Some("mkv") => "video/x-matroska",
            Some("avi") => "video/x-msvideo",
            Some("webm") => "video/webm",
            Some("mp3") => "audio/mpeg",
            Some("m4a") => "audio/mp4",
            Some("wav") => "audio/wav",
            Some("flac") => "audio/flac",
            Some("ogg" | "oga") => "audio/ogg",
            Some("opus") => "audio/opus",
            Some("pdf") => "application/pdf",
            Some("doc") => "application/msword",
            Some("docx") => {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
            Some("xls") => "application/vnd.ms-excel",
            Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Some("csv") => "text/csv",
            Some("txt") => "text/plain",
            _ => "application/octet-stream",
        }
        .to_string()
    }
}

#[cfg(feature = "whatsapp-web")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WhatsAppMediaMarker {
    kind: WhatsAppMediaKind,
    /// Path-like target resolved against the workspace before upload.
    target: String,
}

#[cfg(feature = "whatsapp-web")]
use crate::util::WhatsAppLocation;

#[cfg(feature = "whatsapp-web")]
#[derive(Debug, Clone, PartialEq)]
enum WhatsAppMarker {
    Media(WhatsAppMediaMarker),
    Location(String),
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppMarker {
    fn from_shared_marker(kind: String, target: String) -> Option<Self> {
        if kind.eq_ignore_ascii_case("LOCATION") {
            return Some(Self::Location(target));
        }
        let kind = WhatsAppMediaKind::from_marker(&kind)?;
        Some(Self::Media(WhatsAppMediaMarker { kind, target }))
    }

    /// Short label for structured logs.
    fn kind_label(&self) -> String {
        match self {
            Self::Media(m) => format!("{:?}", m.kind),
            Self::Location(_) => "Location".to_string(),
        }
    }
}

#[cfg(feature = "whatsapp-web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhatsAppMarkerFailure {
    Refused,
    Failed,
}

#[cfg(feature = "whatsapp-web")]
#[derive(Debug)]
enum WhatsAppMarkerError {
    Refused(anyhow::Error),
    Failed(anyhow::Error),
}

#[cfg(feature = "whatsapp-web")]
impl std::fmt::Display for WhatsAppMarkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(err) | Self::Failed(err) => write!(f, "{err}"),
        }
    }
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppMarkerError {
    fn kind(&self) -> WhatsAppMarkerFailure {
        match self {
            Self::Refused(_) => WhatsAppMarkerFailure::Refused,
            Self::Failed(_) => WhatsAppMarkerFailure::Failed,
        }
    }
}

#[cfg(feature = "whatsapp-web")]
fn validate_whatsapp_marker_target(
    target: &str,
    workspace_dir: Option<&Path>,
) -> std::result::Result<PathBuf, WhatsAppMarkerError> {
    if target.starts_with("http://") || target.starts_with("https://") {
        return Err(WhatsAppMarkerError::Refused(anyhow::Error::msg(
            "WhatsApp Web media markers currently accept local workspace files only",
        )));
    }
    let disallowed_scheme = if target.starts_with("data:") {
        Some("data")
    } else if target.starts_with("file:") {
        Some("file")
    } else if target.contains("://") {
        Some(target.split("://").next().unwrap_or("?"))
    } else {
        None
    };
    if let Some(scheme) = disallowed_scheme {
        ::zeroclaw_log::record!(
            WARN,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                .with_attrs(::serde_json::json!({"scheme": scheme})),
            "whatsapp-web: marker target uses disallowed scheme"
        );
        return Err(WhatsAppMarkerError::Refused(anyhow::Error::msg(
            "WhatsApp Web marker target uses a disallowed scheme",
        )));
    }

    let workspace = workspace_dir.ok_or_else(|| {
        ::zeroclaw_log::record!(
            WARN,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                .with_attrs(::serde_json::json!({"reason": "no_workspace_dir"})),
            "whatsapp-web: local marker target has no workspace_dir"
        );
        WhatsAppMarkerError::Refused(anyhow::Error::msg(
            "WhatsApp Web channel was started without a workspace_dir",
        ))
    })?;
    let workspace_canon = std::fs::canonicalize(workspace)
        .with_context(|| format!("canonicalize workspace {}", workspace.display()))
        .map_err(WhatsAppMarkerError::Refused)?;
    let target_path = Path::new(target);
    let absolute = if target_path.is_absolute() {
        target_path.to_path_buf()
    } else {
        workspace_canon.join(target_path)
    };
    let target_canon = match std::fs::canonicalize(&absolute) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            ::zeroclaw_log::record!(
                WARN,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"reason": "not_found"})),
                "whatsapp-web: marker target not found on disk"
            );
            return Err(WhatsAppMarkerError::Failed(anyhow::Error::msg(
                "WhatsApp Web marker target not found on disk",
            )));
        }
        Err(err) => {
            return Err(WhatsAppMarkerError::Refused(
                anyhow::Error::from(err).context("canonicalize WhatsApp marker target"),
            ));
        }
    };

    if !target_canon.starts_with(&workspace_canon) {
        ::zeroclaw_log::record!(
            WARN,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                .with_attrs(::serde_json::json!({"reason": "outside_workspace"})),
            "whatsapp-web: marker target escapes workspace_dir"
        );
        return Err(WhatsAppMarkerError::Refused(anyhow::Error::msg(
            "WhatsApp Web marker target resolves outside workspace_dir",
        )));
    }
    Ok(target_canon)
}

#[cfg(feature = "whatsapp-web")]
fn validate_whatsapp_location_target(
    target: &str,
) -> std::result::Result<WhatsAppLocation, WhatsAppMarkerError> {
    WhatsAppLocation::parse(target).ok_or_else(|| {
        ::zeroclaw_log::record!(
            WARN,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                .with_attrs(::serde_json::json!({"reason": "invalid_location"})),
            "whatsapp-web: location marker target is malformed or outside WGS84 range"
        );
        WhatsAppMarkerError::Refused(anyhow::Error::msg(
            "WhatsApp Web location marker must be `lat,lng[,name[,address]]` with in-range WGS84 coordinates",
        ))
    })
}

#[cfg(feature = "whatsapp-web")]
fn whatsapp_delivery_failure_note(failure_count: usize) -> Option<String> {
    if failure_count == 0 {
        return None;
    }
    let count = failure_count.to_string();
    let key = if failure_count == 1 {
        "channel-whatsapp-web-delivery-failure-note-one"
    } else {
        "channel-whatsapp-web-delivery-failure-note-many"
    };
    Some(i18n::get_required_cli_string_with_args(
        key,
        &[("count", count.as_str())],
    ))
}

#[cfg(feature = "whatsapp-web")]
impl ::zeroclaw_api::attribution::Attributable for WhatsAppWebChannel {
    fn role(&self) -> ::zeroclaw_api::attribution::Role {
        ::zeroclaw_api::attribution::Role::Channel(
            ::zeroclaw_api::attribution::ChannelKind::WhatsappWeb,
        )
    }
    fn alias(&self) -> &str {
        &self.alias
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl Channel for WhatsAppWebChannel {
    fn name(&self) -> &str {
        "whatsapp"
    }

    async fn fetch_media_bytes(&self, descriptor: &str) -> Result<FetchedMedia> {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let client = self.client.lock().clone();
        let Some(client) = client else {
            anyhow::bail!("WhatsApp Web client not connected");
        };
        let raw = STANDARD
            .decode(descriptor.trim())
            .map_err(|_| anyhow::anyhow!("malformed media descriptor"))?;
        let doc: serde_json::Value = serde_json::from_slice(&raw)
            .map_err(|_| anyhow::anyhow!("malformed media descriptor"))?;
        let field = |name: &str| {
            doc.get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        let kind = field("k").unwrap_or_default();
        let media_type = match kind.as_str() {
            "audio" => wacore::download::MediaType::Audio,
            "image" => wacore::download::MediaType::Image,
            "document" => wacore::download::MediaType::Document,
            other => anyhow::bail!("unsupported media kind '{other}' in descriptor"),
        };
        let dp = field("dp").ok_or_else(|| anyhow::anyhow!("descriptor missing direct path"))?;
        let mk = STANDARD
            .decode(field("mk").unwrap_or_default())
            .map_err(|_| anyhow::anyhow!("descriptor missing media key"))?;
        let es = STANDARD
            .decode(field("es").unwrap_or_default())
            .map_err(|_| anyhow::anyhow!("descriptor missing file hash"))?;
        let fs = field("fs")
            .and_then(|v| STANDARD.decode(v).ok())
            .unwrap_or_default();
        let len = doc
            .get("len")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let mime = field("mime");

        let data = client
            .download_from_params(&dp, &mk, &fs, &es, len, media_type)
            .await
            .map_err(|e| anyhow::anyhow!("media download failed: {e}"))?;

        Ok(FetchedMedia { kind, mime, data })
    }

    async fn fetch_media(&self, descriptor: &str) -> Result<String> {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let FetchedMedia { kind, mime, data } = self.fetch_media_bytes(descriptor).await?;

        if kind == "audio" {
            let Some(manager) = self.transcription_manager.as_deref() else {
                anyhow::bail!("transcription is not configured on this instance");
            };
            let file_name = match mime.as_deref() {
                Some(m) if m.contains("mp4") || m.contains("m4a") => "voice.m4a",
                Some(m) if m.contains("mpeg") || m.contains("mp3") => "voice.mp3",
                Some(m) if m.contains("webm") => "voice.webm",
                _ => "voice.ogg",
            };
            let text = manager
                .transcribe(&data, file_name)
                .await
                .map_err(|e| anyhow::anyhow!("transcription failed: {e}"))?;
            Ok(format!("[Audio transcription: {}]", text.trim()))
        } else {
            const MAX_IMAGE_BYTES: usize = 8 * 1024 * 1024;
            if data.len() > MAX_IMAGE_BYTES {
                anyhow::bail!("image too large to inline ({} bytes)", data.len());
            }
            let mime = mime.unwrap_or_else(|| "image/jpeg".to_string());
            Ok(format!(
                "[IMAGE:data:{mime};base64,{}]",
                STANDARD.encode(&data)
            ))
        }
    }

    async fn send(&self, message: &SendMessage) -> Result<()> {
        let client = self.client.lock().clone();
        let Some(client) = client else {
            anyhow::bail!("WhatsApp Web client not connected. Initialize the bot first.");
        };

        // Validate recipient allowlist only for direct phone-number targets.
        if !Self::is_jid(&message.recipient) {
            let normalized = self.normalize_phone(&message.recipient);
            if !self.is_number_allowed(&normalized) {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                    &format!("recipient {} not in allowed list", message.recipient)
                );
                return Ok(());
            }
        }

        let deliverable_recipient = Self::resolve_outbound_recipient(&message.recipient);
        let to = self.recipient_to_jid(&deliverable_recipient)?;
        let raw_content = if message.content.contains("<function_calls")
            || message.content.contains("</function_calls")
            || message.content.contains("<tool_call")
            || message.content.contains("</tool_call")
            || message.content.contains("<tool_calls")
            || message.content.contains("</tool_calls")
        {
            crate::util::strip_tool_call_tags(&message.content)
        } else {
            message.content.clone()
        };
        let (mut text_content, raw_markers) = if raw_content.contains('[')
            && raw_content.contains(':')
            && raw_content.contains(']')
        {
            let (cleaned, raw_markers) = super::util::parse_attachment_markers(&raw_content);
            if raw_markers.is_empty() {
                (raw_content, raw_markers)
            } else {
                (cleaned, raw_markers)
            }
        } else {
            (raw_content, Vec::new())
        };
        let markers = raw_markers
            .into_iter()
            .filter_map(|(kind, target)| WhatsAppMarker::from_shared_marker(kind, target))
            .collect::<Vec<_>>();

        // Voice chat mode: send text normally AND queue a voice note of the
        // final answer. Only substantive messages (not tool outputs) are queued.
        // A debounce task waits 10s after the last substantive message, then
        // sends ONE voice note. Text in → text out. Voice in → text + voice out.
        let is_voice_chat = self
            .voice_chats
            .lock()
            .map(|vs| vs.contains(&message.recipient))
            .unwrap_or(false);

        if is_voice_chat && self.tts_manager.is_some() {
            let content = &text_content;
            // Only queue substantive natural-language replies for voice.
            // Skip tool outputs: URLs, JSON, code blocks, errors, short status.
            let is_substantive = content.len() > 40
                && !content.starts_with("http")
                && !content.starts_with('{')
                && !content.starts_with('[')
                && !content.starts_with("Error")
                && !content.contains("```")
                && !content.contains("tool_call")
                && !content.contains("wttr.in");

            if is_substantive {
                if let Ok(mut pv) = self.pending_voice.lock() {
                    pv.insert(
                        message.recipient.clone(),
                        (content.clone(), std::time::Instant::now()),
                    );
                }

                let pending = self.pending_voice.clone();
                let voice_chats = self.voice_chats.clone();
                let client_clone = client.clone();
                let to_clone = to.clone();
                let recipient = message.recipient.clone();
                let tts_manager = self.tts_manager.clone().unwrap();
                zeroclaw_spawn::spawn!(async move {
                    // Wait 10 seconds — long enough for the agent to finish its
                    // full tool chain and send the final answer.
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

                    // Atomic check-and-remove: only one task gets the value
                    let to_voice = pending.lock().ok().and_then(|mut pv| {
                        if let Some((_, ts)) = pv.get(&recipient)
                            && ts.elapsed().as_secs() >= 8
                        {
                            return pv.remove(&recipient).map(|(text, _)| text);
                        }
                        None
                    });

                    if let Some(text) = to_voice {
                        if let Ok(mut vc) = voice_chats.lock() {
                            vc.remove(&recipient);
                        }
                        match Box::pin(WhatsAppWebChannel::synthesize_voice_static(
                            &client_clone,
                            &to_clone,
                            &text,
                            &tts_manager,
                        ))
                        .await
                        {
                            Ok(()) => {
                                ::zeroclaw_log::record!(
                                    INFO,
                                    ::zeroclaw_log::Event::new(
                                        module_path!(),
                                        ::zeroclaw_log::Action::Note
                                    ),
                                    &format!("voice reply sent ({} chars)", text.len())
                                );
                            }
                            Err(e) => {
                                ::zeroclaw_log::record!(
                                    WARN,
                                    ::zeroclaw_log::Event::new(
                                        module_path!(),
                                        ::zeroclaw_log::Action::Note
                                    )
                                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                                    .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                                    "TTS voice reply failed"
                                );
                            }
                        }
                    }
                });
            }
            // Fall through to send text normally (voice chat gets BOTH)
        }

        let mut delivered_markers = 0usize;
        let mut failed_marker_count = 0usize;
        for marker in &markers {
            // Location markers carry inline data (lat,lng,...) and skip the
            // workspace file validator, but the coordinates must still parse
            // and land in WGS84 range; media markers must resolve to a real
            // file inside the workspace before upload.
            let result = match marker {
                WhatsAppMarker::Location(target) => {
                    match validate_whatsapp_location_target(target) {
                        Ok(loc) => Self::send_location(&client, &to, &loc).await,
                        Err(err) => {
                            ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(
                                    module_path!(),
                                    ::zeroclaw_log::Action::Note
                                )
                                .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                                .with_attrs(::serde_json::json!({
                                    "kind": "Location",
                                    "reason": "invalid coordinates",
                                    "error": err.to_string(),
                                })),
                                "whatsapp-web: dropping unresolved outbound attachment marker"
                            );
                            failed_marker_count += 1;
                            continue;
                        }
                    }
                }
                WhatsAppMarker::Media(media) => {
                    let target = match validate_whatsapp_marker_target(
                        &media.target,
                        self.workspace_dir.as_deref(),
                    ) {
                        Ok(path) => path,
                        Err(err) => {
                            let reason = match err.kind() {
                                WhatsAppMarkerFailure::Refused => "trust boundary",
                                WhatsAppMarkerFailure::Failed => "not found",
                            };
                            ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(
                                    module_path!(),
                                    ::zeroclaw_log::Action::Note
                                )
                                .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                                .with_attrs(::serde_json::json!({
                                    "kind": format!("{:?}", media.kind),
                                    "reason": reason,
                                    "error": err.to_string(),
                                })),
                                "whatsapp-web: dropping unresolved outbound attachment marker"
                            );
                            failed_marker_count += 1;
                            continue;
                        }
                    };
                    Self::send_media_marker(&client, &to, media, &target).await
                }
            };
            match result {
                Ok(()) => delivered_markers += 1,
                Err(err) => {
                    ::zeroclaw_log::record!(
                        WARN,
                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                            .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                            .with_attrs(::serde_json::json!({
                                "kind": marker.kind_label(),
                                "error": err.to_string(),
                            })),
                        "whatsapp-web: marker delivery failed"
                    );
                    failed_marker_count += 1;
                }
            }
        }

        if let Some(note) = whatsapp_delivery_failure_note(failed_marker_count) {
            if text_content.is_empty() {
                text_content = note;
            } else {
                text_content.push_str("\n\n");
                text_content.push_str(&note);
            }
        }

        if !markers.is_empty() && text_content.is_empty() && delivered_markers > 0 {
            return Ok(());
        }

        // Send text message
        let outgoing = waproto::whatsapp::Message {
            conversation: Some(text_content),
            ..Default::default()
        };

        // Box::pin the large future (~34KB) so it doesn't inflate the
        // enclosing Send future's stack slot — clippy::large_futures.
        // whatsapp-rust 0.6: send_message returns `SendResult { message_id, to }`
        // instead of a bare `String` (oxidezap/whatsapp-rust
        let send_result = Box::pin(client.send_message(to, outgoing)).await?;
        ::zeroclaw_log::record!(
            DEBUG,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!(
                "sent text to {} (id: {})",
                message.recipient, send_result.message_id
            )
        );
        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        // Store the sender channel for incoming messages
        *self.tx.lock() = Some(tx.clone());

        // Capture alias as an Arc so the long-running event closure (inside
        // the reconnect loop) can clone cheaply per spawned message without
        // borrowing `self` for its 'static lifetime.
        let alias = std::sync::Arc::new(self.alias.clone());

        use wacore::proto_helpers::MessageExt;
        use wacore::store::DevicePropsOverride;
        use wacore::types::events::Event;
        use wacore_binary::jid::JidExt as _;
        use whatsapp_rust::TokioRuntime;
        use whatsapp_rust::bot::Bot;
        use whatsapp_rust::pair_code::PairCodeOptions;
        use whatsapp_rust::store::{Device, DeviceStore};
        use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
        use whatsapp_rust_ureq_http_client::UreqHttpClient;

        let retry_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        loop {
            let expanded_session_path = Self::expand_session_path(&self.session_path);

            ::zeroclaw_log::record!(
                INFO,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                &format!("channel starting (session: {})", expanded_session_path)
            );

            // Initialize storage backend
            let storage = RusqliteStore::new(&expanded_session_path)?;
            let backend = Arc::new(storage);

            // Check if we have a saved device to load
            let mut device = Device::new(backend.clone());
            if backend.exists().await? {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "found existing session, loading device"
                );
                if let Some(core_device) = backend.load().await? {
                    device.load_from_serializable(core_device);
                } else {
                    anyhow::bail!("Device exists but failed to load");
                }
                if let Some(ref pn) = device.pn
                    && let Some(digits) = Self::store_jid_digits(&self.bot_phone, pn.user())
                {
                    ::zeroclaw_log::record!(
                        INFO,
                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                        &format!("pre-resolved bot phone from saved session: +{}", digits)
                    );
                }
                if let Some(ref lid) = device.lid
                    && let Some(digits) = Self::store_jid_digits(&self.bot_lid, lid.user())
                {
                    ::zeroclaw_log::record!(
                        INFO,
                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                        &format!("pre-resolved bot LID from saved session: {}", digits)
                    );
                }
            } else {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "no existing session, new device will be created during pairing"
                );
            };

            // Create transport factory. WebSocket URL override comes from
            // `[whatsapp.ws_url]`; legacy `WHATSAPP_WS_URL` env var is gone.
            let mut transport_factory = TokioWebSocketTransportFactory::new();
            if let Some(ref ws_url) = self.ws_url {
                transport_factory = transport_factory.with_url(ws_url.clone());
            }

            // Create HTTP client for media operations
            let http_client = UreqHttpClient::new();

            // Channel to signal logout from the event handler back to the listen loop.
            let (logout_tx, mut logout_rx) = tokio::sync::broadcast::channel::<()>(1);

            // Tracks whether Event::LoggedOut actually fired (vs task crash).
            let session_revoked = Arc::new(std::sync::atomic::AtomicBool::new(false));

            // Build the bot
            let tx_clone = tx.clone();
            let peer_resolver = Arc::clone(&self.peer_resolver);
            let logout_tx_clone = logout_tx.clone();
            let retry_count_clone = retry_count.clone();
            let session_revoked_clone = session_revoked.clone();
            let transcription_config = self.transcription.clone();
            let transcription_mgr = self.transcription_manager.clone();
            let voice_chats = self.voice_chats.clone();
            let wa_mode = self.mode.clone();
            let wa_dm_policy = self.dm_policy.clone();
            let wa_group_policy = self.group_policy.clone();
            let wa_self_chat_mode = self.self_chat_mode;
            let mention_only = self.mention_only;
            let passive_group_context = self.passive_group_context;
            let bot_phone_clone = self.bot_phone.clone();
            let bot_lid_clone = self.bot_lid.clone();
            let wa_dm_mention_patterns = self.dm_mention_patterns.clone();
            let wa_group_mention_patterns = self.group_mention_patterns.clone();
            let allowed_groups_resolver = Arc::clone(&self.allowed_groups_resolver);
            let persist_clone = self.persist.clone();
            let configured_push_name = self.push_name.clone();

            let mut builder = Bot::builder()
                .with_backend(backend)
                .with_transport_factory(transport_factory)
                .with_http_client(http_client)
                .with_runtime(TokioRuntime)
                .with_device_props(
                    DevicePropsOverride::new()
                        .with_os("ZeroClaw")
                        .with_platform_type(PlatformType::Desktop),
                )
                .on_event({
                    let alias = Arc::clone(&alias);
                    move |event, client| {
                    let tx_inner = tx_clone.clone();
                    let peer_resolver = Arc::clone(&peer_resolver);
                    let logout_tx = logout_tx_clone.clone();
                    let retry_count = retry_count_clone.clone();
                    let session_revoked = session_revoked_clone.clone();
                    let alias = Arc::clone(&alias);
                    let transcription_config = transcription_config.clone();
                    let transcription_mgr = transcription_mgr.clone();
                    let voice_chats = voice_chats.clone();
                    let wa_mode = wa_mode.clone();
                    let wa_dm_policy = wa_dm_policy.clone();
                    let wa_group_policy = wa_group_policy.clone();
                    let passive_group_context = passive_group_context;
                    let bot_phone_inner = bot_phone_clone.clone();
                    let bot_lid_inner = bot_lid_clone.clone();
                    let wa_dm_mention_patterns = wa_dm_mention_patterns.clone();
                    let wa_group_mention_patterns = wa_group_mention_patterns.clone();
                    let allowed_groups_resolver = Arc::clone(&allowed_groups_resolver);
                    let persist_inner = persist_clone.clone();
                    let configured_push_name = configured_push_name.clone();
                    async move {
                        // whatsapp-rust 0.6: event handlers receive `Arc<Event>`
                        // per so we match against `&*event` to get a
                        // `&Event` reference and bind variant fields by ref.
                        match &*event {
                            Event::Message(msg, info) => {
                                let sender_jid = info.source.sender.clone();
                                let sender_alt = info.source.sender_alt.clone();
                                let sender = sender_jid.user().to_string();
                                let sender_push_name = Some(info.push_name.clone())
                                    .filter(|name| !name.trim().is_empty());
                                let chat = info.source.chat.to_string();

                                let mapped_phone = if sender_jid.is_lid() {
                                    match client.get_lid_pn_entry(&sender_jid).await {
                                        Ok(Some(entry)) => Some(entry.phone_number),
                                        _ => None,
                                    }
                                } else {
                                    None
                                };
                                let sender_candidates = Self::sender_phone_candidates(
                                    &sender_jid,
                                    sender_alt.as_ref(),
                                    mapped_phone.as_deref(),
                                );

                                let allowed_peers = peer_resolver();
                                let normalized = sender_candidates
                                    .iter()
                                    .find(|candidate| {
                                        Self::is_number_allowed_for_list(&allowed_peers, candidate)
                                    })
                                    .cloned();

                                let is_group = info.source.is_group;
                                let reply_target = Self::compute_reply_target(&chat);

                                let allowed_groups = allowed_groups_resolver();
                                if is_group && !is_group_chat_allowed(&chat, &allowed_groups) {
                                    ::zeroclaw_log::record!(
                                        DEBUG,
                                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                                            .with_attrs(::serde_json::json!({ "chat": chat })),
                                        "dropping group message: chat not in allowed_groups"
                                    );
                                    return;
                                }

                                // ── Personal-mode chat-type policy filtering ──
                                if wa_mode == zeroclaw_config::schema::WhatsAppWebMode::Personal {
                                    // Self-chat: the chat JID user part matches
                                    // the sender's user part (message to "Notes
                                    // to Self").
                                    let sender_user = sender_jid.user();
                                    let chat_user = chat
                                        .split_once('@')
                                        .map(|(u, _)| u)
                                        .unwrap_or(&chat);
                                    let is_self_chat = !is_group && sender_user == chat_user && info.source.is_from_me;

                                    if is_self_chat {
                                        if !wa_self_chat_mode {
                                            ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "ignoring self-chat message (self_chat_mode=false)");
                                            return;
                                        }
                                        // self_chat_mode=true: always process, skip further policy checks.
                                    } else if info.source.is_from_me
                                        && !fromme_outside_self_chat_is_operator_trigger(
                                            is_group,
                                            &wa_dm_mention_patterns,
                                            &wa_group_mention_patterns,
                                            msg.text_content().unwrap_or(""),
                                        )
                                    {
                                        ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_attrs(::serde_json::json!({"chat": chat, "sender": sender})), "ignoring fromMe message outside self-chat thread (chat=, sender=)");
                                        return;
                                    } else if is_group {
                                        match wa_group_policy {
                                            zeroclaw_config::schema::WhatsAppChatPolicy::Ignore => {
                                                ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "ignoring group message (group_policy=ignore)");
                                                return;
                                            }
                                            zeroclaw_config::schema::WhatsAppChatPolicy::All => {
                                                // allow unconditionally
                                            }
                                            zeroclaw_config::schema::WhatsAppChatPolicy::Allowlist => {
                                                if normalized.is_none() {
                                                    let lid_diag = Self::lid_rejection_diagnostic(
                                                        &sender_jid,
                                                        mapped_phone.as_deref(),
                                                    );
                                                    ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown), &format!("message from unrecognized sender not in allowed list (candidates_count={}){}", sender_candidates.len(), lid_diag));
                                                    return;
                                                }
                                            }
                                        }
                                    } else {
                                        // DM (non-self)
                                        match wa_dm_policy {
                                            zeroclaw_config::schema::WhatsAppChatPolicy::Ignore => {
                                                ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "ignoring DM (dm_policy=ignore)");
                                                return;
                                            }
                                            zeroclaw_config::schema::WhatsAppChatPolicy::All => {
                                                // allow unconditionally
                                            }
                                            zeroclaw_config::schema::WhatsAppChatPolicy::Allowlist => {
                                                if normalized.is_none() {
                                                    let lid_diag = Self::lid_rejection_diagnostic(
                                                        &sender_jid,
                                                        mapped_phone.as_deref(),
                                                    );
                                                    ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown), &format!("message from unrecognized sender not in allowed list (candidates_count={}){}", sender_candidates.len(), lid_diag));
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }

                                let normalized = normalized.unwrap_or_else(|| sender.clone());
                                let conversation_scope =
                                    Self::group_context_scope(passive_group_context, is_group);
                                let mut passive_context = false;
                                let text_content = msg.text_content().unwrap_or("").trim().to_string();
                                let mut content = Self::media_fallback_content(text_content, msg);

                                ::zeroclaw_log::record!(INFO, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("WhatsApp Web message received (sender_len={}, chat_len={}, content_len={})", sender.len(), chat.len(), content.len()));
                                ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("WhatsApp Web message content: {}", content));

                                // mention_only: group messages without a bot mention can become
                                // passive context when explicitly enabled; otherwise they keep
                                // the existing drop behavior. This runs before STT/media
                                // downloads so passive messages have no provider/tool side effects.
                                // Computed unconditionally: the mention_only
                                // gate below consumes it, and the orchestrator's
                                // reply-intent precheck needs the same answer
                                // even when that gate is off.
                                let addressed = Self::addressed_to_bot(
                                    msg,
                                    &content,
                                    &bot_phone_inner,
                                    &bot_lid_inner,
                                );
                                if mention_only && is_group {
                                    if let Some(addressed) = addressed {
                                        if Self::should_record_passive_group_context(
                                            passive_group_context,
                                            is_group,
                                            addressed,
                                        ) {
                                            passive_context = true;
                                        } else if !addressed {
                                            ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "ignoring group message not addressed to bot");
                                            return;
                                        }
                                    } else {
                                        ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "mention_only active but bot identity unknown, skipping group msg");
                                        return;
                                    }
                                }
                                let explicitly_addressed = addressed.unwrap_or(false);

                                let passive_from_mention_gating_possible =
                                    Self::should_record_passive_group_context(
                                        passive_group_context,
                                        is_group,
                                        false,
                                    );
                                if !passive_context && passive_from_mention_gating_possible {
                                    match super::whatsapp::WhatsAppChannel::apply_mention_gating(
                                        &wa_dm_mention_patterns,
                                        &wa_group_mention_patterns,
                                        &content,
                                        is_group,
                                    ) {
                                        Some(c) => content = c,
                                        None => {
                                            passive_context = true;
                                        }
                                    }
                                }

                                if passive_context {
                                    if content.is_empty() {
                                        ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("ignoring empty passive group context from {}", normalized));
                                        return;
                                    }
                                    // Media stays undownloaded on this lane;
                                    // the claim tickets let a later
                                    // fetch_media call redeem it.
                                    content = Self::append_media_refs(content, msg);
                                    Self::send_inbound_channel_message(
                                        &tx_inner,
                                        alias.as_ref(),
                                        &normalized,
                                        sender_push_name.as_deref(),
                                        reply_target,
                                        Self::with_turn_context(
                                            content,
                                            msg,
                                            &bot_phone_inner,
                                            &bot_lid_inner,
                                        ),
                                        Vec::new(),
                                        true,
                                        explicitly_addressed,
                                        conversation_scope,
                                    )
                                    .await;
                                    return;
                                }

                                // Attempt voice note transcription (ptt = push-to-talk = voice note).
                                // When `transcribe_non_ptt_audio` is enabled in the transcription
                                // config, also transcribe forwarded / regular audio messages.
                                let voice_text = if let Some(ref audio) = msg.audio_message {
                                    let is_ptt = audio.ptt == Some(true);
                                    let non_ptt_enabled = transcription_config
                                        .as_ref()
                                        .is_some_and(|c| c.transcribe_non_ptt_audio);
                                    if is_ptt || non_ptt_enabled {
                                        Self::try_transcribe_voice_note(
                                            &client,
                                            audio,
                                            transcription_config.as_ref(),
                                            transcription_mgr.as_deref(),
                                        )
                                        .await
                                    } else {
                                        ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("ignoring non-PTT audio message from {}", normalized));
                                        None
                                    }
                                } else {
                                    None
                                };

                                // Use transcribed voice text, or fall back to text content.
                                // Track whether this chat used a voice note so we reply in kind.
                                if let Some(ref vt) = voice_text {
                                    if let Ok(mut vs) = voice_chats.lock() {
                                        vs.insert(reply_target.clone());
                                    }
                                    content = format!("[Voice] {vt}");
                                } else if let Ok(mut vs) = voice_chats.lock() {
                                    vs.remove(&reply_target);
                                }
                                content = Self::media_fallback_content(content, msg);

                                if content.is_empty() {
                                    ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("ignoring empty or non-text message from {}", normalized));
                                    return;
                                }

                                if !passive_from_mention_gating_possible {
                                    content = match super::whatsapp::WhatsAppChannel::apply_mention_gating(
                                        &wa_dm_mention_patterns,
                                        &wa_group_mention_patterns,
                                        &content,
                                        is_group,
                                    ) {
                                        Some(c) => c,
                                        None => {
                                            ::zeroclaw_log::record!(DEBUG, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_attrs(::serde_json::json!({"normalized": normalized})), "message from did not match mention patterns, dropping");
                                            return;
                                        }
                                    };
                                }

                                let mut attachments = Vec::new();
                                // Audio included: a media pipeline (or a
                                // multimodal model) is the only thing that can
                                // still use a voice note whose STT failed.
                                Self::collect_media_attachments(
                                    &client,
                                    msg,
                                    "",
                                    true,
                                    &mut attachments,
                                )
                                .await;
                                if let Some(quoted) = Self::extract_quoted_message(msg) {
                                    Self::collect_media_attachments(
                                        &client,
                                        quoted,
                                        "quoted-",
                                        true,
                                        &mut attachments,
                                    )
                                    .await;
                                }

                                // Tickets ride under the addressed turn too:
                                // the attachment bytes above serve THIS turn,
                                // the ticket keeps the media redeemable later
                                // (save to a file, re-view after the landed
                                // copy ages out).
                                content = Self::append_media_refs(content, msg);

                                Self::send_inbound_channel_message(
                                    &tx_inner,
                                    alias.as_ref(),
                                    &normalized,
                                    sender_push_name.as_deref(),
                                    reply_target,
                                    Self::with_turn_context(
                                        content,
                                        msg,
                                        &bot_phone_inner,
                                        &bot_lid_inner,
                                    ),
                                    attachments,
                                    false,
                                    explicitly_addressed,
                                    conversation_scope,
                                )
                                .await;
                            }
                            Event::Connected(_) => {
                                crate::login_events::LoginEvent::Connected.emit(
                                    "whatsapp",
                                    alias.as_ref(),
                                    "WhatsApp Web connected successfully",
                                );
                                WhatsAppWebChannel::reset_retry(&retry_count);
                                let device = client
                                    .persistence_manager()
                                    .get_device_snapshot()
                                    .await;
                                // Resolve bot identity from the device store
                                if mention_only {
                                    if let Some(ref pn) = device.pn
                                        && let Some(digits) =
                                            Self::store_jid_digits(&bot_phone_inner, pn.user())
                                    {
                                        ::zeroclaw_log::record!(INFO, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("resolved bot identity from device: +{}", digits));
                                    }
                                    if let Some(ref lid) = device.lid
                                        && let Some(digits) =
                                            Self::store_jid_digits(&bot_lid_inner, lid.user())
                                    {
                                        ::zeroclaw_log::record!(INFO, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("resolved bot LID from device: {}", digits));
                                    }
                                }
                                // `Bot::builder().with_push_name()` is not used: it writes
                                // the local store pre-connect and the server's
                                // `setting_pushName` sync overwrites it. Connected fires
                                // after that sync, so the device snapshot is authoritative
                                // and comparing against it keeps reconnects from emitting a
                                // redundant app-state patch.
                                if let Some(desired) = WhatsAppWebChannel::push_name_to_apply(
                                    configured_push_name.as_deref(),
                                    &device.push_name,
                                ) {
                                    match client.profile().set_push_name(desired).await {
                                        Ok(()) => {
                                            ::zeroclaw_log::record!(INFO, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), &format!("set WhatsApp push name (configured len={}, previous len={})", desired.len(), device.push_name.len()));
                                        }
                                        Err(e) => {
                                            ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown), &format!("failed to set WhatsApp push name (configured len={}); keeping the account's current name: {e}", desired.len()));
                                        }
                                    }
                                }
                                // Persist the linked account as an authorized
                                // peer in canonical peer_groups (same shape
                                // WeChat pairing writes). Idempotent, so the
                                // reconnect-after-resume case is a no-op.
                                if let Some(ref pn) = device.pn {
                                    let digits = Self::jid_digits(pn.user());
                                    if !digits.is_empty()
                                        && let Err(e) =
                                            crate::identity_persist::persist_external_peer(
                                                persist_inner.as_ref(),
                                                "whatsapp",
                                                alias.as_ref(),
                                                &format!("+{digits}"),
                                            )
                                            .await
                                    {
                                        ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown), &format!("failed to persist linked WhatsApp identity: {e}"));
                                    }
                                }
                            }
                            Event::LoggedOut(_) => {
                                session_revoked.store(true, std::sync::atomic::Ordering::Relaxed);
                                crate::login_events::LoginEvent::LoggedOut.emit(
                                    "whatsapp",
                                    alias.as_ref(),
                                    "WhatsApp Web was logged out — will clear session and reconnect",
                                );
                                let _ = logout_tx.send(());
                            }
                            Event::StreamError(stream_error) => {
                                ::zeroclaw_log::record!(ERROR, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail).with_outcome(::zeroclaw_log::EventOutcome::Failure), &format!("stream error: {:?}", stream_error));
                            }
                            Event::PairingCode { code, .. } => {
                                crate::login_events::LoginEvent::PairCode { code: code.as_str() }
                                    .emit(
                                    "whatsapp",
                                    alias.as_ref(),
                                    "WhatsApp Web pair code received — enter it in WhatsApp > Linked Devices",
                                );
                                eprintln!();
                                eprintln!("pair code: {code}");
                                eprintln!();
                            }
                            Event::PairingQrCode { code, .. } => {
                                crate::login_events::LoginEvent::Qr {
                                    payload: code.as_str(),
                                    image_url: None,
                                    attempt: None,
                                    max_attempts: None,
                                }
                                .emit(
                                    "whatsapp",
                                    alias.as_ref(),
                                    "WhatsApp Web QR code received (scan with WhatsApp > Linked Devices)",
                                );
                                match Self::render_pairing_qr(code) {
                                    Ok(rendered) => {
                                        eprintln!();
                                        eprintln!(
                                            "WhatsApp Web QR code (scan in WhatsApp > Linked Devices):"
                                        );
                                        eprintln!("{rendered}");
                                        eprintln!();
                                    }
                                    Err(err) => {
                                        ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown), &format!("failed to render pairing QR in terminal: {}", err));
                                        eprintln!();
                                        eprintln!("WhatsApp Web QR payload: {code}");
                                        eprintln!();
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }});

            // Configure pair-code flow when a phone number is provided.
            if let Some(ref phone) = self.pair_phone {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "pair-code flow enabled for configured phone number"
                );
                builder = builder.with_pair_code(PairCodeOptions {
                    phone_number: phone.clone(),
                    custom_code: self.pair_code.clone(),
                    ..Default::default()
                });
            } else if self.pair_code.is_some() {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                    "pair_code is set but pair_phone is missing; pair code config is ignored"
                );
            }

            let mut bot = builder.build().await?;
            let client = bot.client();
            // A bot never announces presence, which would otherwise downgrade every
            // delivery receipt to `type="inactive"` and drop the delivered ticks.
            client.set_force_active_delivery_receipts(true);
            *self.client.lock() = Some(client);

            // Run the bot
            let bot_handle = bot.run().await?;

            // Store the bot handle for later shutdown
            *self.bot_handle.lock() = Some(bot_handle);

            // Drop the outer sender so logout_rx.recv() returns Err when the
            // bot task ends without emitting LoggedOut (e.g. crash/panic).
            drop(logout_tx);

            // Wait for a logout signal or process shutdown.
            let should_reconnect = select! {
                res = logout_rx.recv() => {
                    // Both Ok(()) and Err (sender dropped) mean the session ended.
                    let _ = res;
                    true
                }
                _ = tokio::signal::ctrl_c() => {
                    ::zeroclaw_log::record!(INFO, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note), "channel received Ctrl+C");
                    false
                }
            };

            *self.client.lock() = None;
            let handle = self.bot_handle.lock().take();
            if let Some(handle) = handle {
                handle.abort();
                // Await the aborted task so background I/O finishes before
                // we delete session files.
                let _ = handle.await;
            }

            // Drop bot/device so the SQLite connection is closed
            // before we remove session files (releases WAL/SHM locks).
            // `backend` was moved into the builder, so dropping `bot`
            // releases the last Arc reference to the storage backend.
            drop(bot);
            drop(device);

            if should_reconnect {
                let (attempts, exceeded) = Self::record_retry(&retry_count);
                if exceeded {
                    anyhow::bail!(
                        "exceeded {} reconnect attempts, giving up",
                        Self::MAX_RETRIES
                    );
                }

                // Only purge session files when LoggedOut was explicitly observed.
                // A transient task crash (Err from recv) should not wipe a valid session.
                if Self::should_purge_session(&session_revoked) {
                    for path in Self::session_file_paths(&expanded_session_path) {
                        match tokio::fs::remove_file(&path).await {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(
                                    module_path!(),
                                    ::zeroclaw_log::Action::Note
                                )
                                .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                                &format!("failed to remove session file {}: {e}", path)
                            ),
                        }
                    }
                    ::zeroclaw_log::record!(
                        INFO,
                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                        "session files removed, restarting for QR pairing"
                    );
                } else {
                    ::zeroclaw_log::record!(
                        WARN,
                        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                            .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                        "bot stopped without LoggedOut; reconnecting with existing session"
                    );
                }

                let delay = Self::compute_retry_delay(attempts);
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    &format!(
                        "reconnecting in {}s (attempt {}/{})",
                        delay,
                        attempts,
                        Self::MAX_RETRIES
                    )
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                continue;
            }

            break;
        }

        Ok(())
    }

    async fn health_check(&self) -> bool {
        let bot_handle_guard = self.bot_handle.lock();
        bot_handle_guard.is_some()
    }

    async fn start_typing(&self, recipient: &str) -> Result<()> {
        let client = self.client.lock().clone();
        let Some(client) = client else {
            anyhow::bail!("WhatsApp Web client not connected. Initialize the bot first.");
        };

        if !Self::is_jid(recipient) {
            let normalized = self.normalize_phone(recipient);
            if !self.is_number_allowed(&normalized) {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                    &format!("typing target {} not in allowed list", recipient)
                );
                return Ok(());
            }
        }

        let deliverable_recipient = Self::resolve_outbound_recipient(recipient);
        let to = self.recipient_to_jid(&deliverable_recipient)?;
        client.chatstate().send_composing(&to).await.map_err(|e| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                "Failed to send typing state (composing)"
            );
            anyhow::Error::msg(format!("Failed to send typing state (composing): {e}"))
        })?;

        ::zeroclaw_log::record!(
            DEBUG,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!("start typing for {}", recipient)
        );
        Ok(())
    }

    async fn stop_typing(&self, recipient: &str) -> Result<()> {
        let client = self.client.lock().clone();
        let Some(client) = client else {
            anyhow::bail!("WhatsApp Web client not connected. Initialize the bot first.");
        };

        if !Self::is_jid(recipient) {
            let normalized = self.normalize_phone(recipient);
            if !self.is_number_allowed(&normalized) {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                    &format!("typing target {} not in allowed list", recipient)
                );
                return Ok(());
            }
        }

        let deliverable_recipient = Self::resolve_outbound_recipient(recipient);
        let to = self.recipient_to_jid(&deliverable_recipient)?;
        client.chatstate().send_paused(&to).await.map_err(|e| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"error": format!("{}", e)})),
                "Failed to send typing state (paused)"
            );
            anyhow::Error::msg(format!("Failed to send typing state (paused): {e}"))
        })?;

        ::zeroclaw_log::record!(
            DEBUG,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!("stop typing for {}", recipient)
        );
        Ok(())
    }
}

// Stub implementation when feature is not enabled
#[cfg(not(feature = "whatsapp-web"))]
pub struct WhatsAppWebChannel {
    _private: (),
}

#[cfg(not(feature = "whatsapp-web"))]
impl WhatsAppWebChannel {
    pub fn new(
        _session_path: String,
        _pair_phone: Option<String>,
        _pair_code: Option<String>,
        _ws_url: Option<String>,
        _alias: impl Into<String>,
        _peer_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
        _mention_only: bool,
        _mode: zeroclaw_config::schema::WhatsAppWebMode,
        _dm_policy: zeroclaw_config::schema::WhatsAppChatPolicy,
        _group_policy: zeroclaw_config::schema::WhatsAppChatPolicy,
        _self_chat_mode: bool,
    ) -> Self {
        Self { _private: () }
    }

    pub fn with_transcription(self, _config: zeroclaw_config::schema::TranscriptionConfig) -> Self {
        self
    }

    pub fn with_tts(self, _config: zeroclaw_config::schema::TtsConfig) -> Self {
        self
    }
}

#[cfg(not(feature = "whatsapp-web"))]
impl ::zeroclaw_api::attribution::Attributable for WhatsAppWebChannel {
    fn role(&self) -> ::zeroclaw_api::attribution::Role {
        ::zeroclaw_api::attribution::Role::Channel(
            ::zeroclaw_api::attribution::ChannelKind::WhatsappWeb,
        )
    }
    fn alias(&self) -> &str {
        "whatsapp"
    }
}

#[cfg(not(feature = "whatsapp-web"))]
#[async_trait]
impl Channel for WhatsAppWebChannel {
    fn name(&self) -> &str {
        "whatsapp"
    }

    async fn send(&self, _message: &SendMessage) -> Result<()> {
        anyhow::bail!(i18n::get_required_cli_string(
            "channel-whatsapp-web-feature-missing-error"
        ));
    }

    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        anyhow::bail!(i18n::get_required_cli_string(
            "channel-whatsapp-web-feature-missing-error"
        ));
    }

    async fn health_check(&self) -> bool {
        false
    }

    async fn start_typing(&self, _recipient: &str) -> Result<()> {
        anyhow::bail!(i18n::get_required_cli_string(
            "channel-whatsapp-web-feature-missing-error"
        ));
    }

    async fn stop_typing(&self, _recipient: &str) -> Result<()> {
        anyhow::bail!(i18n::get_required_cli_string(
            "channel-whatsapp-web-feature-missing-error"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "whatsapp-web")]
    use wacore_binary::jid::Jid;

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_ref_descriptors_cover_primary_quoted_document_and_skip_view_once() {
        fn image_msg() -> waproto::whatsapp::ImageMessage {
            waproto::whatsapp::ImageMessage {
                direct_path: Some("/v/x".into()),
                media_key: Some(vec![1]),
                file_enc_sha256: Some(vec![2]),
                mimetype: Some("image/jpeg".into()),
                ..Default::default()
            }
        }

        // Primary image only.
        let msg = waproto::whatsapp::Message {
            image_message: Some(Box::new(image_msg())),
            ..Default::default()
        };
        assert_eq!(WhatsAppWebChannel::media_ref_descriptors(&msg).len(), 1);

        // A reply whose quoted message carries the image: quoted ticket minted.
        let quoting = waproto::whatsapp::Message {
            extended_text_message: Some(Box::new(waproto::whatsapp::ExtendedTextMessage {
                text: Some("save it".into()),
                context_info: Some(Box::new(waproto::whatsapp::ContextInfo {
                    quoted_message: Some(Box::new(msg.clone())),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(WhatsAppWebChannel::media_ref_descriptors(&quoting).len(), 1);

        // Documents are redeemable too.
        let doc = waproto::whatsapp::Message {
            document_message: Some(Box::new(waproto::whatsapp::DocumentMessage {
                direct_path: Some("/v/d".into()),
                media_key: Some(vec![1]),
                file_enc_sha256: Some(vec![2]),
                mimetype: Some("application/pdf".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let descs = WhatsAppWebChannel::media_ref_descriptors(&doc);
        assert_eq!(descs.len(), 1);
        assert_eq!(
            zeroclaw_api::media_ref::summarize_media_ref(&descs[0])
                .expect("valid ticket")
                .kind,
            "document"
        );

        // View-once media never becomes a durable, replayable claim.
        let view_once = waproto::whatsapp::Message {
            image_message: Some(Box::new(waproto::whatsapp::ImageMessage {
                view_once: Some(true),
                ..image_msg()
            })),
            ..Default::default()
        };
        assert!(WhatsAppWebChannel::media_ref_descriptors(&view_once).is_empty());
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn clear_persisted_session_removes_db_triple_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("session.db");
        let db_str = db.to_string_lossy().into_owned();
        std::fs::write(&db, b"db").unwrap();
        std::fs::write(format!("{db_str}-wal"), b"wal").unwrap();
        std::fs::write(format!("{db_str}-shm"), b"shm").unwrap();

        let removed = WhatsAppWebChannel::clear_persisted_session(&db_str).unwrap();
        assert_eq!(removed.len(), 3);
        for path in WhatsAppWebChannel::session_file_paths(&db_str) {
            assert!(
                !std::path::Path::new(&path).exists(),
                "{path} must be removed"
            );
        }

        // Relinking an already unpaired channel is a safe no-op that
        // must not create the database.
        let removed = WhatsAppWebChannel::clear_persisted_session(&db_str).unwrap();
        assert!(removed.is_empty());
        assert!(!db.exists());

        // Empty session_path (channel saved without one) clears nothing.
        assert!(
            WhatsAppWebChannel::clear_persisted_session("")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_markers_reuse_shared_parser_kinds() {
        let (cleaned, raw) = super::super::util::parse_attachment_markers(
            "send [IMAGE:photo.png] [DOCUMENT:report.pdf] [VOICE:voice.ogg]",
        );
        let markers = raw
            .into_iter()
            .filter_map(|(kind, target)| WhatsAppMarker::from_shared_marker(kind, target))
            .collect::<Vec<_>>();

        assert_eq!(cleaned, "send");
        assert_eq!(
            markers
                .iter()
                .map(|marker| match marker {
                    WhatsAppMarker::Media(m) => m.kind,
                    WhatsAppMarker::Location(_) => panic!("expected media markers"),
                })
                .collect::<Vec<_>>(),
            vec![
                WhatsAppMediaKind::Image,
                WhatsAppMediaKind::Document,
                WhatsAppMediaKind::Voice
            ]
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn from_shared_marker_routes_location_kind() {
        let marker = WhatsAppMarker::from_shared_marker(
            "LOCATION".to_string(),
            "40.7128,-74.0060".to_string(),
        );
        assert!(matches!(marker, Some(WhatsAppMarker::Location(_))));
        // An invalid target still routes to the Location variant: validation
        // is deferred to the send path so the marker counts as a failed
        // delivery instead of being stripped silently.
        assert_eq!(
            WhatsAppMarker::from_shared_marker("LOCATION".to_string(), "999,999".to_string()),
            Some(WhatsAppMarker::Location("999,999".to_string()))
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn validate_location_target_refuses_invalid_coordinates() {
        // Out-of-range and malformed targets are refused — the send loop
        // counts these as failed deliveries rather than sending a bogus pin.
        for target in ["999,999", "not-a-number,0.0", "40.7128", ""] {
            let err = validate_whatsapp_location_target(target)
                .expect_err("out-of-range or malformed target must be refused");
            assert_eq!(err.kind(), WhatsAppMarkerFailure::Refused, "{target}");
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn allowed_groups_empty_permits_all() {
        // Empty list is the default: every group passes (no behavior change).
        assert!(super::is_group_chat_allowed("123456789012345@g.us", &[]));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn validate_marker_target_accepts_workspace_relative_file() {
        let workspace = tempfile::tempdir().expect("tempdir");
        let file = workspace.path().join("photo.png");
        std::fs::write(&file, b"png").expect("write fixture");

        let resolved =
            validate_whatsapp_marker_target("photo.png", Some(workspace.path())).expect("inside");

        assert_eq!(resolved, file.canonicalize().expect("canonical fixture"));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn allowed_groups_full_jid_match() {
        let groups = vec!["123456789012345@g.us".to_string()];
        assert!(super::is_group_chat_allowed(
            "123456789012345@g.us",
            &groups
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn validate_marker_target_rejects_workspace_escape() {
        let workspace = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");

        let err = validate_whatsapp_marker_target(
            outside.path().to_str().expect("utf8 path"),
            Some(workspace.path()),
        )
        .expect_err("outside workspace must be refused");

        assert_eq!(err.kind(), WhatsAppMarkerFailure::Refused);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn allowed_groups_jid_prefix_match() {
        let groups = vec!["123456789012345".to_string()];
        assert!(super::is_group_chat_allowed(
            "123456789012345@g.us",
            &groups
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn validate_marker_target_rejects_without_workspace() {
        let err = validate_whatsapp_marker_target("photo.png", None)
            .expect_err("workspace is required for local marker reads");

        assert_eq!(err.kind(), WhatsAppMarkerFailure::Refused);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn allowed_groups_no_match_drops() {
        let groups = vec!["123456789012345".to_string()];
        assert!(!super::is_group_chat_allowed(
            "999999999999999@g.us",
            &groups
        ));
        // Blank / whitespace-only entries never match.
        assert!(!super::is_group_chat_allowed(
            "123@g.us",
            &["   ".to_string()]
        ));
        // Prefix entries match the user part EXACTLY, not as a string prefix:
        // "123" must admit "123@g.us" but never "123999@g.us".
        assert!(super::is_group_chat_allowed(
            "123@g.us",
            &["123".to_string()]
        ));
        assert!(!super::is_group_chat_allowed(
            "123999@g.us",
            &["123".to_string()]
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn validate_marker_target_marks_missing_as_failed() {
        let workspace = tempfile::tempdir().expect("tempdir");

        let err = validate_whatsapp_marker_target("missing.png", Some(workspace.path()))
            .expect_err("missing file should fail delivery");

        assert_eq!(err.kind(), WhatsAppMarkerFailure::Failed);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn delivery_failure_note_is_count_only() {
        let count: usize = 2;
        let note = whatsapp_delivery_failure_note(count).expect("note");

        // Locale-independent: count is always rendered as Arabic digits in
        // every shipped locale's FTL template (`{$count}`). The literal
        // English phrase used to live here but the assertion would break
        // on any CI runner with a non-English `$LANG`.
        assert!(!note.is_empty(), "note must be non-empty");
        assert!(
            note.contains(count.to_string().as_str()),
            "note must contain the failure count"
        );
        assert!(!note.contains("/"), "note must not contain path separators");
        assert!(
            !note.contains("workspace"),
            "note must not echo local marker targets"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn voice_marker_uses_opus_mime_for_ogg_family() {
        assert_eq!(
            WhatsAppMediaKind::Voice.mime_for_path(Path::new("voice.ogg")),
            "audio/ogg; codecs=opus"
        );
        assert_eq!(
            WhatsAppMediaKind::Audio.mime_for_path(Path::new("voice.ogg")),
            "audio/ogg"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn allowed_groups_dm_bypasses_filter() {
        // DMs bypass: the call site gates on `is_group`, so a direct message
        // is admitted even when a non-empty allowed_groups would not match it.
        let groups = vec!["123456789012345".to_string()];
        let is_group = false;
        let dm_jid = "987654321098765@s.whatsapp.net";
        let admitted = !is_group || super::is_group_chat_allowed(dm_jid, &groups);
        assert!(admitted);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn passive_group_context_is_default_off_and_group_only() {
        assert!(!WhatsAppWebChannel::should_record_passive_group_context(
            false, true, false
        ));
        assert!(!WhatsAppWebChannel::should_record_passive_group_context(
            true, false, false
        ));
        assert!(!WhatsAppWebChannel::should_record_passive_group_context(
            true, true, true
        ));
        assert!(WhatsAppWebChannel::should_record_passive_group_context(
            true, true, false
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn passive_group_context_uses_reply_target_scope_for_groups() {
        assert_eq!(
            WhatsAppWebChannel::group_context_scope(false, true),
            ChannelConversationScope::Sender
        );
        assert_eq!(
            WhatsAppWebChannel::group_context_scope(true, false),
            ChannelConversationScope::Sender
        );
        assert_eq!(
            WhatsAppWebChannel::group_context_scope(true, true),
            ChannelConversationScope::ReplyTarget
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_channel_name() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(ch.name(), "whatsapp");
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_number_allowed_exact() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert!(ch.is_number_allowed("+1234567890"));
        assert!(!ch.is_number_allowed("+9876543210"));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_number_allowed_wildcard() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["*".into()]),
            Arc::new(Vec::new),
        );
        assert!(ch.is_number_allowed("+1234567890"));
        assert!(ch.is_number_allowed("+9999999999"));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_number_denied_empty() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(Vec::new),
            Arc::new(Vec::new),
        );
        // Empty allowlist means "deny all" (matches channel-wide allowlist policy).
        assert!(!ch.is_number_allowed("+1234567890"));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_normalize_phone_adds_plus() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(ch.normalize_phone("1234567890"), "+1234567890");
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_normalize_phone_preserves_plus() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(ch.normalize_phone("+1234567890"), "+1234567890");
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_normalize_phone_from_jid() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(
            ch.normalize_phone("1234567890@s.whatsapp.net"),
            "+1234567890"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_normalize_phone_token_accepts_formatted_phone() {
        assert_eq!(
            WhatsAppWebChannel::normalize_phone_token("+1 (555) 123-4567"),
            Some("+15551234567".to_string())
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_allowlist_matches_normalized_format() {
        let allowed = vec!["+15551234567".to_string()];
        assert!(WhatsAppWebChannel::is_number_allowed_for_list(
            &allowed,
            "+1 (555) 123-4567"
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_sender_candidates_include_sender_alt_phone() {
        let sender = Jid::lid("76188559093817");
        let sender_alt = Jid::pn("15551234567");
        let candidates =
            WhatsAppWebChannel::sender_phone_candidates(&sender, Some(&sender_alt), None);
        assert!(candidates.contains(&"+15551234567".to_string()));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn whatsapp_web_sender_candidates_include_lid_mapping_phone() {
        let sender = Jid::lid("76188559093817");
        let candidates =
            WhatsAppWebChannel::sender_phone_candidates(&sender, None, Some("15551234567"));
        assert!(candidates.contains(&"+15551234567".to_string()));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn compute_reply_target_preserves_lid_dm() {
        // LID DM → preserved as-is (library handles LID resolution internally)
        let chat_jid = "76188559093817@lid";
        let result = WhatsAppWebChannel::compute_reply_target(chat_jid);
        assert_eq!(
            result, chat_jid,
            "LID DM must be preserved - library handles LID addressing natively"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn compute_reply_target_preserves_pn_dm() {
        // PN DM → preserved as-is
        let chat_jid = "15551234567@s.whatsapp.net";
        let result = WhatsAppWebChannel::compute_reply_target(chat_jid);
        assert_eq!(result, chat_jid, "PN DM must preserve original chat JID");
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn compute_reply_target_preserves_group() {
        // Group chat → preserved as-is
        let chat_jid = "120363012345678901@g.us";
        let result = WhatsAppWebChannel::compute_reply_target(chat_jid);
        assert_eq!(
            result, chat_jid,
            "Group chat must preserve original chat JID"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn lid_rejection_diagnostic_empty_for_non_lid_sender() {
        let sender = Jid::pn("15551234567");
        let diag = WhatsAppWebChannel::lid_rejection_diagnostic(&sender, None);
        assert!(
            diag.is_empty(),
            "non-LID senders must not generate any LID-resolution suffix; got {diag:?}"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn lid_rejection_diagnostic_names_resolution_failure_for_lid_with_no_phone() {
        let sender = Jid::lid("76188559093817");
        let diag = WhatsAppWebChannel::lid_rejection_diagnostic(&sender, None);
        assert!(
            diag.contains("LID→phone resolution returned None"),
            "diagnostic must name the resolution failure mode #6350 describes; got {diag:?}"
        );
        assert!(
            diag.contains("76188559093817"),
            "diagnostic must surface the LID identifier so the operator can add the LID-form workaround; got {diag:?}"
        );
        assert!(
            diag.contains("allowed_numbers"),
            "diagnostic must point at the config knob to fix this; got {diag:?}"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn lid_rejection_diagnostic_distinguishes_resolved_phone_mismatch() {
        // LID resolved successfully but the resulting phone wasn't in the
        // allowlist. Different cause from the resolution failure path; the
        // operator shouldn't be steered toward the LID workaround.
        let sender = Jid::lid("76188559093817");
        let diag = WhatsAppWebChannel::lid_rejection_diagnostic(&sender, Some("15551234567"));
        assert!(
            !diag.contains("LID→phone resolution returned None"),
            "must not suggest resolution failed when mapped_phone is Some; got {diag:?}"
        );
        assert!(
            diag.contains("did not match"),
            "diagnostic must explain the resolved phone failed the allowlist; got {diag:?}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "whatsapp-web")]
    async fn whatsapp_web_health_check_disconnected() {
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        );
        assert!(!ch.health_check().await);
    }

    // ── Reconnect retry state machine tests (exercise production helpers) ──

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn compute_retry_delay_doubles_with_cap() {
        // Uses the production helper that listen() calls for backoff.
        // attempt 1 → 3s, 2 → 6s, 3 → 12s, … 7 → 192s, 8 → 300s (capped)
        let expected = [3, 6, 12, 24, 48, 96, 192, 300, 300, 300];
        for (i, &want) in expected.iter().enumerate() {
            let attempt = (i + 1) as u32;
            assert_eq!(
                WhatsAppWebChannel::compute_retry_delay(attempt),
                want,
                "attempt {attempt}"
            );
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn compute_retry_delay_zero_attempt() {
        // Edge case: attempt 0 should still produce BASE (saturating_sub clamps).
        assert_eq!(
            WhatsAppWebChannel::compute_retry_delay(0),
            WhatsAppWebChannel::BASE_DELAY_SECS
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn record_retry_increments_and_detects_exceeded() {
        use std::sync::atomic::AtomicU32;
        let counter = AtomicU32::new(0);

        // First MAX_RETRIES attempts should not exceed.
        for i in 1..=WhatsAppWebChannel::MAX_RETRIES {
            let (attempt, exceeded) = WhatsAppWebChannel::record_retry(&counter);
            assert_eq!(attempt, i);
            assert!(!exceeded, "attempt {i} should not exceed max");
        }

        // Next attempt exceeds the limit.
        let (attempt, exceeded) = WhatsAppWebChannel::record_retry(&counter);
        assert_eq!(attempt, WhatsAppWebChannel::MAX_RETRIES + 1);
        assert!(exceeded);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn reset_retry_clears_counter() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = AtomicU32::new(0);

        // Simulate several reconnect attempts via the production helper.
        for _ in 0..5 {
            WhatsAppWebChannel::record_retry(&counter);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 5);

        // Event::Connected calls reset_retry — verify it zeroes the counter.
        WhatsAppWebChannel::reset_retry(&counter);
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        // After reset, record_retry starts from 1 again.
        let (attempt, exceeded) = WhatsAppWebChannel::record_retry(&counter);
        assert_eq!(attempt, 1);
        assert!(!exceeded);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn should_purge_session_only_when_revoked() {
        use std::sync::atomic::AtomicBool;
        let flag = AtomicBool::new(false);

        // Transient crash: flag is false → should NOT purge.
        assert!(!WhatsAppWebChannel::should_purge_session(&flag));

        // Explicit LoggedOut: flag set to true → should purge.
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(WhatsAppWebChannel::should_purge_session(&flag));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn with_transcription_sets_config_when_enabled() {
        let tc = zeroclaw_config::schema::TranscriptionConfig {
            enabled: true,
            api_key: Some("test_key".to_string()),
            ..Default::default()
        };

        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        )
        .with_transcription(tc);
        assert!(ch.transcription.is_some());
        assert!(ch.transcription_manager.is_some());
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn with_transcription_ignores_when_disabled() {
        let tc = zeroclaw_config::schema::TranscriptionConfig::default(); // enabled = false
        let mention_only = false;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test-whatsapp.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["+1234567890".into()]),
            Arc::new(Vec::new),
        )
        .with_transcription(tc);
        assert!(ch.transcription.is_none());
        assert!(ch.transcription_manager.is_none());
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn with_transcription_binds_the_configured_provider() {
        let cases = [
            (
                zeroclaw_config::schema::TranscriptionConfig {
                    enabled: true,
                    api_key: Some("test_key".to_string()),
                    ..Default::default()
                },
                "groq",
            ),
            (
                zeroclaw_config::schema::TranscriptionConfig {
                    enabled: true,
                    openai: Some(zeroclaw_config::schema::OpenAiSttConfig {
                        api_key: Some("test_key".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                "openai",
            ),
        ];

        for (tc, expected) in cases {
            let cfg = zeroclaw_config::schema::WhatsAppConfig {
                enabled: true,
                session_path: Some("/tmp/test-whatsapp.db".into()),
                ..Default::default()
            };
            let ch = WhatsAppWebChannel::new(
                &cfg,
                "whatsapp_web_test_alias",
                Arc::new(|| vec!["+1234567890".into()]),
                Arc::new(Vec::new),
            )
            .with_transcription(tc);

            // An unbound manager makes `transcribe` bail for every provider.
            let manager = ch.transcription_manager.expect("manager");
            assert_eq!(manager.agent_transcription_provider(), expected);
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn session_file_paths_includes_wal_and_shm() {
        let paths = WhatsAppWebChannel::session_file_paths("/tmp/test.db");
        assert_eq!(
            paths,
            [
                "/tmp/test.db".to_string(),
                "/tmp/test.db-wal".to_string(),
                "/tmp/test.db-shm".to_string(),
            ]
        );
    }

    // ── Mention detection tests ──

    #[cfg(feature = "whatsapp-web")]
    fn extended_text_reply(
        participant: &str,
        mentioned_jids: &[&str],
    ) -> waproto::whatsapp::Message {
        waproto::whatsapp::Message {
            extended_text_message: Some(Box::new(
                waproto::whatsapp::message::ExtendedTextMessage {
                    text: Some("expand the previous response".to_string()),
                    context_info: Some(Box::new(waproto::whatsapp::ContextInfo {
                        participant: Some(participant.to_string()),
                        mentioned_jid: mentioned_jids
                            .iter()
                            .map(|jid| (*jid).to_string())
                            .collect(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn sticker_reply(
        participant: &str,
        quoted_message: Option<waproto::whatsapp::Message>,
    ) -> waproto::whatsapp::Message {
        waproto::whatsapp::Message {
            sticker_message: Some(Box::new(waproto::whatsapp::message::StickerMessage {
                mimetype: Some("image/webp".to_string()),
                context_info: Some(Box::new(waproto::whatsapp::ContextInfo {
                    participant: Some(participant.to_string()),
                    quoted_message: quoted_message.map(Box::new),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[cfg(feature = "whatsapp-web")]
    fn image_mention(mentioned_jids: &[&str]) -> waproto::whatsapp::Message {
        waproto::whatsapp::Message {
            image_message: Some(Box::new(waproto::whatsapp::message::ImageMessage {
                mimetype: Some("image/jpeg".to_string()),
                context_info: Some(Box::new(waproto::whatsapp::ContextInfo {
                    mentioned_jid: mentioned_jids
                        .iter()
                        .map(|jid| (*jid).to_string())
                        .collect(),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn quoted_reply_prefix_names_what_the_reply_targets() {
        // Nothing quoted → nothing prepended.
        let plain = waproto::whatsapp::Message {
            conversation: Some("yes".to_string()),
            ..Default::default()
        };
        assert_eq!(WhatsAppWebChannel::quoted_reply_prefix(&plain), None);
        assert_eq!(
            WhatsAppWebChannel::with_quoted_reply_prefix("yes".to_string(), &plain),
            "yes"
        );

        let quoted = waproto::whatsapp::Message {
            conversation: Some("should we ship\nthe invoice today?".to_string()),
            ..Default::default()
        };
        let reply = sticker_reply("200@lid", Some(quoted));
        assert_eq!(
            WhatsAppWebChannel::with_quoted_reply_prefix("yes".to_string(), &reply),
            "[Reply to 200: should we ship the invoice today?]\nyes"
        );

        // Quoted media reuses the media fallback, captions included.
        let quoted_image = waproto::whatsapp::Message {
            image_message: Some(Box::new(waproto::whatsapp::message::ImageMessage {
                caption: Some("the broken part".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let reply = sticker_reply("200@lid", Some(quoted_image));
        assert_eq!(
            WhatsAppWebChannel::quoted_reply_prefix(&reply).as_deref(),
            Some("[Reply to 200: [Image] the broken part]")
        );

        // Long quotes are truncated rather than dominating the turn.
        let quoted_long = waproto::whatsapp::Message {
            conversation: Some("ש".repeat(500)),
            ..Default::default()
        };
        let reply = sticker_reply("200@lid", Some(quoted_long));
        let prefix = WhatsAppWebChannel::quoted_reply_prefix(&reply).expect("prefix");
        assert!(prefix.ends_with("...]"), "{prefix}");
        assert_eq!(
            prefix.chars().count(),
            "[Reply to 200: ]".chars().count()
                + WhatsAppWebChannel::QUOTED_REPLY_PREFIX_MAX_CHARS
                + "...".len()
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn quoted_reply_prefix_states_the_quoted_author_id() {
        let quoted = || waproto::whatsapp::Message {
            conversation: Some("delete sections 2 and 3".to_string()),
            ..Default::default()
        };

        // The author id, device suffix stripped. Correlating it with the bot's
        // own number or a group sender label is the model's job.
        assert_eq!(
            WhatsAppWebChannel::quoted_reply_prefix(&sticker_reply(
                "972532445442:6@s.whatsapp.net",
                Some(quoted())
            ))
            .as_deref(),
            Some("[Reply to 972532445442: delete sections 2 and 3]")
        );

        // No usable id → the bare shape.
        let anonymous = |participant: Option<&str>| waproto::whatsapp::Message {
            sticker_message: Some(Box::new(waproto::whatsapp::message::StickerMessage {
                mimetype: Some("image/webp".to_string()),
                context_info: Some(Box::new(waproto::whatsapp::ContextInfo {
                    participant: participant.map(ToOwned::to_owned),
                    quoted_message: Some(Box::new(quoted())),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        for participant in [None, Some("@lid")] {
            assert_eq!(
                WhatsAppWebChannel::quoted_reply_prefix(&anonymous(participant)).as_deref(),
                Some("[Reply to: delete sections 2 and 3]")
            );
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn self_id_disclosure_states_the_live_session_ids() {
        let disclosure = |phone: Option<&str>, lid: Option<&str>| {
            WhatsAppWebChannel::self_id_disclosure(
                &Mutex::new(phone.map(ToOwned::to_owned)),
                &Mutex::new(lid.map(ToOwned::to_owned)),
            )
        };

        // Both halves of the identity when the session knows both.
        assert_eq!(
            disclosure(Some("972533333333"), Some("84342633502")).as_deref(),
            Some("[Your id: 972533333333, 84342633502]")
        );
        // Whichever half resolved first still gets disclosed.
        assert_eq!(
            disclosure(Some("972533333333"), None).as_deref(),
            Some("[Your id: 972533333333]")
        );
        assert_eq!(
            disclosure(None, Some("84342633502@lid")).as_deref(),
            Some("[Your id: 84342633502]")
        );
        // Nothing invented before the session resolves an identity.
        assert_eq!(disclosure(None, None), None);
        assert_eq!(disclosure(Some(""), None), None);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn turn_context_discloses_the_bot_ids_above_the_quote() {
        let quoted = waproto::whatsapp::Message {
            conversation: Some("delete sections 2 and 3".to_string()),
            ..Default::default()
        };
        let reply = sticker_reply("972532445442@s.whatsapp.net", Some(quoted));

        assert_eq!(
            WhatsAppWebChannel::with_turn_context(
                "what am I referencing?".to_string(),
                &reply,
                &Mutex::new(Some("972533333333".to_string())),
                &Mutex::new(Some("84342633502".to_string())),
            ),
            "[Your id: 972533333333, 84342633502]\n\
             [Reply to 972532445442: delete sections 2 and 3]\n\
             what am I referencing?"
        );

        // Identity still unknown: the turn carries what it can and no more.
        assert_eq!(
            WhatsAppWebChannel::with_turn_context(
                "what am I referencing?".to_string(),
                &reply,
                &Mutex::new(None),
                &Mutex::new(None),
            ),
            "[Reply to 972532445442: delete sections 2 and 3]\n\
             what am I referencing?"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn document_file_name_is_bare_bounded_and_always_present() {
        assert_eq!(
            WhatsAppWebChannel::document_file_name(Some("invoice.pdf"), "application/pdf"),
            "invoice.pdf"
        );
        // The sender picks this string: keep the basename only.
        assert_eq!(
            WhatsAppWebChannel::document_file_name(
                Some("../../etc/passwd"),
                "application/octet-stream"
            ),
            "passwd"
        );
        assert_eq!(
            WhatsAppWebChannel::document_file_name(Some("a\\b\\c.docx"), "application/msword"),
            "c.docx"
        );
        // Missing, blank, or dot-only names fall back to the MIME subtype.
        assert_eq!(
            WhatsAppWebChannel::document_file_name(None, "application/pdf"),
            "whatsapp-document.pdf"
        );
        assert_eq!(
            WhatsAppWebChannel::document_file_name(Some("  "), "application/octet-stream"),
            "whatsapp-document.octet-stream"
        );
        assert_eq!(
            WhatsAppWebChannel::document_file_name(Some("..."), ""),
            "whatsapp-document.bin"
        );

        let long = WhatsAppWebChannel::document_file_name(Some(&"n".repeat(500)), "text/plain");
        assert_eq!(
            long.chars().count(),
            WhatsAppWebChannel::DOCUMENT_NAME_MAX_CHARS
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn jid_digits_extracts_phone_from_jid() {
        assert_eq!(
            WhatsAppWebChannel::jid_digits("919211916069@s.whatsapp.net"),
            "919211916069"
        );
        assert_eq!(
            WhatsAppWebChannel::jid_digits("76188559093817@lid"),
            "76188559093817"
        );
        assert_eq!(WhatsAppWebChannel::jid_digits("15551234567"), "15551234567");
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_structured() {
        let jids = vec!["919211916069@s.whatsapp.net".to_string()];
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey @919211916069 check this",
            &jids,
            "919211916069",
            None
        ));
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey check this",
            &jids,
            "919211916069",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_text_fallback() {
        let no_jids: Vec<String> = vec![];
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey @919211916069 check this",
            &no_jids,
            "919211916069",
            None
        ));
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey @919211916069",
            &no_jids,
            "919211916069",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_prefix_false_positive() {
        let no_jids: Vec<String> = vec![];
        assert!(!WhatsAppWebChannel::contains_bot_mention(
            "hey @919211916069 check this",
            &no_jids,
            "91921191606",
            None
        ));
        assert!(!WhatsAppWebChannel::contains_bot_mention(
            "hey @155512345678",
            &no_jids,
            "15551234567",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_no_match() {
        let no_jids: Vec<String> = vec![];
        assert!(!WhatsAppWebChannel::contains_bot_mention(
            "just a regular message",
            &no_jids,
            "919211916069",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_scans_past_prefix_false_match() {
        let no_jids: Vec<String> = vec![];
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "@9192119160691 real @919211916069",
            &no_jids,
            "919211916069",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_rejects_embedded_at() {
        let no_jids: Vec<String> = vec![];
        assert!(!WhatsAppWebChannel::contains_bot_mention(
            "foo@919211916069 bar",
            &no_jids,
            "919211916069",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn jid_digits_strips_device_suffix() {
        assert_eq!(
            WhatsAppWebChannel::jid_digits("919211916069:16@s.whatsapp.net"),
            "919211916069"
        );
        assert_eq!(
            WhatsAppWebChannel::jid_digits("227728477442093:3@lid"),
            "227728477442093"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_matches_lid() {
        let jids = vec!["227728477442093@lid".to_string()];
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey @DisplayName check this",
            &jids,
            "6287778315246",
            Some("227728477442093")
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn contains_bot_mention_matches_lid_when_phone_unknown() {
        let jids = vec!["227728477442093@lid".to_string()];
        assert!(WhatsAppWebChannel::contains_bot_mention(
            "hey @DisplayName check this",
            &jids,
            "",
            Some("227728477442093")
        ));
        assert!(!WhatsAppWebChannel::contains_bot_mention(
            "plain @ mention",
            &[],
            "",
            None
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn extract_mentioned_jids_reads_media_context_info() {
        let msg = image_mention(&["100@s.whatsapp.net"]);
        assert_eq!(
            WhatsAppWebChannel::extract_mentioned_jids(&msg),
            vec!["100@s.whatsapp.net".to_string()]
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn addressed_to_bot_is_none_until_the_bot_identity_is_known() {
        let msg = extended_text_reply("200@lid", &[]);
        let text = "expand the previous response";

        let (no_phone, no_lid) = (Mutex::new(None), Mutex::new(None));
        assert_eq!(
            WhatsAppWebChannel::addressed_to_bot(&msg, text, &no_phone, &no_lid),
            None
        );

        let phone = Mutex::new(Some("100".to_string()));
        let lid = Mutex::new(Some("200".to_string()));
        assert_eq!(
            WhatsAppWebChannel::addressed_to_bot(&msg, text, &phone, &lid),
            Some(true)
        );

        let other_lid = Mutex::new(Some("999".to_string()));
        assert_eq!(
            WhatsAppWebChannel::addressed_to_bot(&msg, text, &phone, &other_lid),
            Some(false)
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn message_addressed_to_bot_accepts_reply_to_phone_jid() {
        let msg = extended_text_reply("100@s.whatsapp.net", &[]);
        assert!(WhatsAppWebChannel::is_message_addressed_to_bot(
            &msg,
            "expand the previous response",
            "100",
            None,
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn message_addressed_to_bot_accepts_reply_to_lid_jid() {
        let msg = extended_text_reply("200@lid", &[]);
        assert!(WhatsAppWebChannel::is_message_addressed_to_bot(
            &msg,
            "expand the previous response",
            "100",
            Some("200"),
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn message_addressed_to_bot_accepts_media_reply_to_lid_jid() {
        let msg = sticker_reply("200@lid", None);
        assert!(WhatsAppWebChannel::is_message_addressed_to_bot(
            &msg,
            "[Sticker]",
            "100",
            Some("200"),
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn extract_quoted_message_reads_media_context_info() {
        let quoted = waproto::whatsapp::Message {
            image_message: Some(Box::new(waproto::whatsapp::message::ImageMessage {
                mimetype: Some("image/png".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let msg = sticker_reply("200@lid", Some(quoted));
        let quoted = WhatsAppWebChannel::extract_quoted_message(&msg)
            .expect("sticker reply should expose the quoted message");
        assert!(quoted.image_message.is_some());
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_keeps_sticker_messages_addressable() {
        let msg = sticker_reply("200@lid", None);
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            "[Sticker]"
        );
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content("hello".to_string(), &msg),
            "hello"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_keeps_the_caption() {
        let msg = waproto::whatsapp::Message {
            image_message: Some(Box::new(waproto::whatsapp::message::ImageMessage {
                caption: Some("  what is wrong with this?  ".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            "[Image]\n\nwhat is wrong with this?"
        );

        // A blank caption stays at the bare marker.
        let msg = waproto::whatsapp::Message {
            video_message: Some(Box::new(waproto::whatsapp::message::VideoMessage {
                caption: Some("   ".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            "[Video]"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_marks_audio_so_a_failed_transcript_still_answers() {
        let voice = waproto::whatsapp::Message {
            audio_message: Some(Box::new(waproto::whatsapp::message::AudioMessage {
                ptt: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &voice),
            "[Voice]"
        );
        // A successful transcript is already in `content` and wins.
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content("[Voice] hello".to_string(), &voice),
            "[Voice] hello"
        );

        let audio = waproto::whatsapp::Message {
            audio_message: Some(Box::new(waproto::whatsapp::message::AudioMessage {
                ptt: None,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &audio),
            "[Audio]"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_leaves_non_media_messages_empty() {
        let msg = waproto::whatsapp::Message::default();
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            ""
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_parses_static_location() {
        let msg = waproto::whatsapp::Message {
            location_message: Some(Box::new(waproto::whatsapp::message::LocationMessage {
                degrees_latitude: Some(40.7128),
                degrees_longitude: Some(-74.0060),
                name: Some("NYC".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            "[Location: 40.712800, -74.006000 — NYC]"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_skips_live_location() {
        let msg = waproto::whatsapp::Message {
            location_message: Some(Box::new(waproto::whatsapp::message::LocationMessage {
                degrees_latitude: Some(40.7128),
                degrees_longitude: Some(-74.0060),
                is_live: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            ""
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn media_fallback_content_skips_missing_coordinates() {
        // Missing longitude — should silently drop, not fabricate 0,0
        let msg = waproto::whatsapp::Message {
            location_message: Some(Box::new(waproto::whatsapp::message::LocationMessage {
                degrees_latitude: Some(40.7128),
                degrees_longitude: None,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            ""
        );
        // Missing latitude
        let msg = waproto::whatsapp::Message {
            location_message: Some(Box::new(waproto::whatsapp::message::LocationMessage {
                degrees_latitude: None,
                degrees_longitude: Some(-74.0060),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            WhatsAppWebChannel::media_fallback_content(String::new(), &msg),
            ""
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn mime_extension_uses_safe_subtype() {
        assert_eq!(
            WhatsAppWebChannel::mime_extension("image/jpeg; name=photo", "jpg"),
            "jpg"
        );
        assert_eq!(
            WhatsAppWebChannel::mime_extension("application/vnd.ms-excel", "bin"),
            "vnd.ms-excel"
        );
        assert_eq!(
            WhatsAppWebChannel::mime_extension("image/../../png", "bin"),
            "bin"
        );
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn message_addressed_to_bot_rejects_reply_to_other_participant() {
        let msg = extended_text_reply("300@s.whatsapp.net", &[]);
        assert!(!WhatsAppWebChannel::is_message_addressed_to_bot(
            &msg,
            "expand the previous response",
            "100",
            Some("200"),
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn message_addressed_to_bot_accepts_explicit_mention_in_other_reply() {
        let msg = extended_text_reply("300@s.whatsapp.net", &["100@s.whatsapp.net"]);
        assert!(WhatsAppWebChannel::is_message_addressed_to_bot(
            &msg,
            "expand the previous response",
            "100",
            Some("200"),
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn constructor_seeds_bot_phone_from_pair_phone() {
        let mention_only = true;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test.db".into()),
            pair_phone: Some("919211916069".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["*".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(*ch.bot_phone.lock(), Some("919211916069".to_string()));
        assert_eq!(*ch.bot_lid.lock(), None);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn constructor_carries_push_name_from_config() {
        let mk = |push_name: Option<&str>| {
            let cfg = zeroclaw_config::schema::WhatsAppConfig {
                enabled: true,
                session_path: Some("/tmp/test.db".into()),
                push_name: push_name.map(Into::into),
                ..Default::default()
            };
            WhatsAppWebChannel::new(
                &cfg,
                "whatsapp_web_test_alias",
                Arc::new(|| vec!["*".into()]),
                Arc::new(Vec::new),
            )
        };
        assert_eq!(mk(Some("סוכן")).push_name.as_deref(), Some("סוכן"));
        assert_eq!(mk(None).push_name, None);
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn push_name_to_apply_sends_only_on_a_real_difference() {
        // Multi-byte throughout, so any byte-slicing of the value shows up here.
        let non_ascii = "סוכן – שירות";
        assert!(non_ascii.len() > non_ascii.chars().count());

        // (configured, name the device announces, name to send)
        let cases: [(Option<&str>, &str, Option<&str>); 11] = [
            (None, "Galaxy S21", None),
            (None, "", None),
            (Some(""), "Old", None),
            (Some("   \t "), "Old", None),
            (Some("ZeroClawAgent"), "Galaxy S21", Some("ZeroClawAgent")),
            (Some("ZeroClawAgent"), "", Some("ZeroClawAgent")),
            (Some("ZeroClawAgent"), "ZeroClawAgent", None),
            (Some("  ZeroClawAgent  "), "ZeroClawAgent", None),
            (Some(non_ascii), "Galaxy S21", Some(non_ascii)),
            (Some(non_ascii), non_ascii, None),
            // A value that is a strict byte-prefix of the device's name is
            // still a difference.
            (Some("סוכן"), "סוכנת", Some("סוכן")),
        ];
        for (configured, current, want) in cases {
            assert_eq!(
                WhatsAppWebChannel::push_name_to_apply(configured, current),
                want,
                "configured {configured:?} against device {current:?}"
            );
        }
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn constructor_no_pair_phone_leaves_bot_phone_none() {
        let mention_only = true;
        let self_chat_mode = false;
        let cfg = zeroclaw_config::schema::WhatsAppConfig {
            enabled: true,
            session_path: Some("/tmp/test.db".into()),
            mention_only,
            self_chat_mode,
            ..Default::default()
        };
        let ch = WhatsAppWebChannel::new(
            &cfg,
            "whatsapp_web_test_alias",
            Arc::new(|| vec!["*".into()]),
            Arc::new(Vec::new),
        );
        assert_eq!(*ch.bot_phone.lock(), None);
    }

    // ── fromme_outside_self_chat_is_operator_trigger ───────────

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn fromme_trigger_drops_when_no_mention_patterns_configured() {
        let dm: Vec<regex::Regex> = vec![];
        let group: Vec<regex::Regex> = vec![];
        // Without configured patterns, a fromMe message in a third-party
        // DM or group must drop — there is no opt-in signal that says the
        // operator wants outbound mirrors to be treated as triggers.
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            false,
            &dm,
            &group,
            "TinyBot foo"
        ));
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            true,
            &dm,
            &group,
            "TinyBot foo"
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn fromme_trigger_falls_through_when_dm_pattern_matches() {
        // @ilteoood's configured workflow: dm_mention_patterns = ["TinyBot"].
        // Operator types "TinyBot translate this" in a friend's DM →
        // intentional invocation, must fall through.
        let dm = vec![
            regex::RegexBuilder::new("TinyBot")
                .case_insensitive(true)
                .build()
                .unwrap(),
        ];
        let group: Vec<regex::Regex> = vec![];
        assert!(fromme_outside_self_chat_is_operator_trigger(
            false,
            &dm,
            &group,
            "TinyBot translate this"
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn fromme_trigger_drops_when_dm_pattern_does_not_match() {
        // Operator types a normal message in a friend's DM — even with
        // patterns configured, no match means it stays an outbound mirror
        // and must be dropped to prevent impersonation.
        let dm = vec![
            regex::RegexBuilder::new("TinyBot")
                .case_insensitive(true)
                .build()
                .unwrap(),
        ];
        let group: Vec<regex::Regex> = vec![];
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            false,
            &dm,
            &group,
            "see you at 7"
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn fromme_trigger_uses_group_patterns_for_group_threads() {
        // group_mention_patterns gates the group case; dm patterns must
        // not be consulted for group messages and vice versa. This pins
        // the predicate's branch selection.
        let dm: Vec<regex::Regex> = vec![
            regex::RegexBuilder::new("DmTrigger")
                .case_insensitive(true)
                .build()
                .unwrap(),
        ];
        let group = vec![
            regex::RegexBuilder::new("GroupTrigger")
                .case_insensitive(true)
                .build()
                .unwrap(),
        ];
        // In a group, only group_patterns matter.
        assert!(fromme_outside_self_chat_is_operator_trigger(
            true,
            &dm,
            &group,
            "GroupTrigger hi"
        ));
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            true,
            &dm,
            &group,
            "DmTrigger hi"
        ));
        // In a DM, only dm_patterns matter.
        assert!(fromme_outside_self_chat_is_operator_trigger(
            false,
            &dm,
            &group,
            "DmTrigger hi"
        ));
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            false,
            &dm,
            &group,
            "GroupTrigger hi"
        ));
    }

    #[test]
    #[cfg(feature = "whatsapp-web")]
    fn fromme_trigger_drops_when_text_is_empty() {
        // Voice notes and media-only messages return empty text. With no
        // text to match against, the operator-trigger path must drop —
        // never transcribe a fromMe voice note just to check whether it
        // is a bot trigger (cost + impersonation risk).
        let dm = vec![
            regex::RegexBuilder::new("TinyBot")
                .case_insensitive(true)
                .build()
                .unwrap(),
        ];
        let group: Vec<regex::Regex> = vec![];
        assert!(!fromme_outside_self_chat_is_operator_trigger(
            false, &dm, &group, ""
        ));
    }
}
