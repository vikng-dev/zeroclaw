//! Redeem media claim tickets from the current chat's history.
//!
//! Channels that skip media downloads (passive group context) leave a
//! `[WA-MEDIA:...]` descriptor in the history row. This tool finds those
//! descriptors in the persisted session, picks one by short id or recency,
//! and asks the owning channel to download and render it.

use crate::reaction::ChannelMapHandle;
use serde_json::json;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use zeroclaw_api::media_ref::{extract_media_refs, summarize_media_ref};
use zeroclaw_api::tool::{Tool, ToolOutput, ToolResult};
use zeroclaw_config::policy::{SecurityPolicy, ToolOperation};
use zeroclaw_infra::session_backend::SessionBackend;

/// How far back in the persisted session the marker scan reaches. Matches the
/// order of magnitude of the channel history window; tickets older than this
/// have scrolled out of the conversation anyway.
const SCAN_TAIL_ROWS: usize = 80;

pub struct FetchMediaTool {
    security: Arc<SecurityPolicy>,
    channels: ChannelMapHandle,
    backend: Arc<dyn SessionBackend>,
    /// Where redeemed media lands as files (`media_pipeline.files_dir`,
    /// falling back to `<workspace>/media_files`). `view` spools images here
    /// so the result is a short re-loadable `[IMAGE:<path>]` marker instead
    /// of a data URI that outgrows tool-result truncation; `save` writes the
    /// durable file it exists to produce.
    files_root: PathBuf,
}

impl FetchMediaTool {
    pub fn new(
        security: Arc<SecurityPolicy>,
        channels: ChannelMapHandle,
        backend: Arc<dyn SessionBackend>,
        files_root: PathBuf,
    ) -> Self {
        Self {
            security,
            channels,
            backend,
            files_root,
        }
    }

    /// Land redeemed bytes as a file, named by the ticket's short id so
    /// repeated redemptions of one ticket overwrite rather than accumulate.
    async fn land_bytes(
        &self,
        id: &str,
        mime: Option<&str>,
        kind: &str,
        data: &[u8],
    ) -> anyhow::Result<PathBuf> {
        tokio::fs::create_dir_all(&self.files_root).await?;
        let path = self
            .files_root
            .join(format!("{id}.{}", extension_for(mime, kind)));
        tokio::fs::write(&path, data).await?;
        Ok(path)
    }
}

/// File extension for a redeemed ticket, from its MIME type with the
/// descriptor kind as fallback.
fn extension_for(mime: Option<&str>, kind: &str) -> &'static str {
    match mime.unwrap_or_default() {
        m if m.contains("jpeg") || m.contains("jpg") => "jpg",
        m if m.contains("png") => "png",
        m if m.contains("webp") => "webp",
        m if m.contains("gif") => "gif",
        m if m.contains("mp4") || m.contains("m4a") => "m4a",
        m if m.contains("mpeg") || m.contains("mp3") => "mp3",
        m if m.contains("ogg") || m.contains("opus") => "ogg",
        m if m.contains("webm") => "webm",
        m if m.contains("pdf") => "pdf",
        _ if kind == "image" => "jpg",
        _ if kind == "audio" => "ogg",
        _ => "bin",
    }
}

/// Select descriptors from history rows (oldest-first input), newest first.
/// `id` narrows to tickets whose short id starts with it; otherwise the most
/// recent `last` tickets are taken.
fn select_media_refs(rows: &[String], id: Option<&str>, last: usize) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    for row in rows.iter().rev() {
        for payload in extract_media_refs(row).into_iter().rev() {
            let Some(summary) = summarize_media_ref(&payload) else {
                continue;
            };
            if let Some(id) = id {
                if !summary.id.starts_with(id) {
                    continue;
                }
            }
            found.push(payload);
            if found.len() >= last {
                return found;
            }
        }
    }
    found
}

#[async_trait::async_trait]
impl Tool for FetchMediaTool {
    fn name(&self) -> &str {
        "fetch_media"
    }

    fn description(&self) -> &str {
        "Retrieve media from this conversation's recent history that was not \
         downloaded at receive time (voice notes, images from group messages). \
         History shows such items as '[undownloaded audio — id XXXXXXXX ...]'. \
         Pass that id, or omit it to redeem the most recent item(s). \
         mode=\"view\" (default) returns a transcription for audio and the \
         image itself for images; mode=\"save\" writes the raw bytes to a \
         file and returns its path, for uploading or storing the media \
         somewhere."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "description": "Channel to fetch through (defaults to the conversation's own channel)"
                },
                "id": {
                    "type": "string",
                    "description": "Short id of one media item, as shown in history"
                },
                "last": {
                    "type": "integer",
                    "description": "Redeem the N most recent undownloaded items (default 1)"
                },
                "mode": {
                    "type": "string",
                    "enum": ["view", "save"],
                    "description": "view (default): render for reading — transcription or inline image. save: write the raw bytes to a file and return the path."
                }
            },
            "required": []
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let mode = args
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("view");
        let save = match mode {
            "save" => true,
            "view" => false,
            other => {
                return Ok(ToolResult {
                    success: false,
                    output: ToolOutput::default(),
                    error: Some(format!("unknown mode '{other}': use \"view\" or \"save\"")),
                });
            }
        };
        // Viewing is a read; saving creates a file on disk, so it is gated as
        // a state-changing operation.
        let operation = if save {
            ToolOperation::Act
        } else {
            ToolOperation::Read
        };
        if let Err(error) = self
            .security
            .enforce_tool_operation(operation, "fetch_media")
        {
            return Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some(error),
            });
        }

        let session_key = zeroclaw_api::TOOL_LOOP_SESSION_KEY
            .try_with(Clone::clone)
            .ok()
            .flatten();
        let Some(session_key) = session_key else {
            return Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some("fetch_media only works inside a conversation turn".to_string()),
            });
        };

        let channel_name = args
            .get("channel")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let channel = {
            let map = self.channels.read();
            map.get(&channel_name)
                .or_else(|| {
                    channel_name
                        .split_once('.')
                        .and_then(|(bare, _)| map.get(bare))
                })
                .cloned()
        };
        let Some(channel) = channel else {
            let known: Vec<String> = self.channels.read().keys().cloned().collect();
            return Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some(format!(
                    "channel '{channel_name}' not found; active channels: {}",
                    known.join(", ")
                )),
            });
        };

        let id = args
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        #[allow(clippy::cast_possible_truncation)]
        let last = args
            .get("last")
            .and_then(serde_json::Value::as_u64)
            .map_or(1, |v| (v as usize).clamp(1, 5));

        let messages = self.backend.load(&session_key);
        let start = messages.len().saturating_sub(SCAN_TAIL_ROWS);
        let rows: Vec<String> = messages[start..]
            .iter()
            .map(|m| m.content.clone())
            .collect();
        let selected = select_media_refs(&rows, id, last);
        if selected.is_empty() {
            return Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some(match id {
                    Some(id) => format!("no undownloaded media with id '{id}' in recent history"),
                    None => "no undownloaded media in recent history".to_string(),
                }),
            });
        }

        let mut output = String::new();
        let mut saved = Vec::new();
        let mut failures = 0usize;
        for payload in &selected {
            let summary = summarize_media_ref(payload);
            let (label, short_id, kind) = summary
                .as_ref()
                .map(|s| (format!("{} {}", s.kind, s.id), s.id.clone(), s.kind.clone()))
                .unwrap_or_else(|| ("media".to_string(), "media".to_string(), String::new()));

            if save {
                match channel.fetch_media_bytes(payload).await {
                    Ok(media) => {
                        match self
                            .land_bytes(&short_id, media.mime.as_deref(), &media.kind, &media.data)
                            .await
                        {
                            Ok(path) => {
                                let _ = writeln!(
                                    output,
                                    "[{label}] saved: {} ({}, {} bytes)\n",
                                    path.display(),
                                    media.mime.as_deref().unwrap_or("unknown type"),
                                    media.data.len()
                                );
                                saved.push(json!({
                                    "id": short_id,
                                    "kind": media.kind,
                                    "mime": media.mime,
                                    "bytes": media.data.len(),
                                    "path": path.display().to_string(),
                                }));
                            }
                            Err(e) => {
                                failures += 1;
                                let _ = writeln!(output, "[{label}] save failed: {e}\n");
                            }
                        }
                    }
                    Err(e) => {
                        failures += 1;
                        let _ = writeln!(output, "[{label}] retrieval failed: {e}\n");
                    }
                }
                continue;
            }

            // View. Images spool to disk and return a short re-loadable
            // [IMAGE:<path>] marker — an inline data URI would be dropped
            // whole by tool-result truncation. Audio (transcription) and
            // anything else keeps the channel's own rendering.
            if kind == "image" {
                if let Ok(media) = channel.fetch_media_bytes(payload).await {
                    if let Ok(path) = self
                        .land_bytes(&short_id, media.mime.as_deref(), &media.kind, &media.data)
                        .await
                    {
                        let _ = writeln!(
                            output,
                            "[{label}]\n[IMAGE:{}]\n(also on disk at that path for file tools)\n",
                            path.display()
                        );
                        continue;
                    }
                }
                // fall through to the channel renderer on any failure
            }
            match channel.fetch_media(payload).await {
                Ok(text) => {
                    let _ = writeln!(output, "[{label}]\n{text}\n");
                }
                Err(e) => {
                    failures += 1;
                    let _ = writeln!(output, "[{label}] retrieval failed: {e}\n");
                }
            }
        }
        let text = output.trim_end().to_string();
        let output = if saved.is_empty() {
            text.into()
        } else {
            ToolOutput::json_with_text(json!({ "saved": saved }), text)
        };
        Ok(ToolResult {
            success: failures < selected.len(),
            output,
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    fn ticket(kind: &str, es: &str) -> String {
        let payload =
            STANDARD.encode(format!(r#"{{"v":1,"k":"{kind}","es":"{es}","dp":"/x"}}"#));
        format!("[WA-MEDIA:{payload}]")
    }

    #[test]
    fn selects_newest_first_and_respects_count() {
        let rows = vec![
            format!("[Voice]\n{}", ticket("audio", "AAAAAAAAAA")),
            format!("[Voice]\n{}", ticket("audio", "BBBBBBBBBB")),
            "plain text".to_string(),
        ];
        let picked = select_media_refs(&rows, None, 2);
        assert_eq!(picked.len(), 2);
        assert_eq!(summarize_media_ref(&picked[0]).unwrap().id, "BBBBBBBB");
        assert_eq!(summarize_media_ref(&picked[1]).unwrap().id, "AAAAAAAA");
    }

    #[test]
    fn selects_by_id_prefix() {
        let rows = vec![
            format!("x {}", ticket("audio", "AbCdEfGhIj")),
            format!("y {}", ticket("image", "ZyXwVuTsRq")),
        ];
        let picked = select_media_refs(&rows, Some("AbCd"), 1);
        assert_eq!(picked.len(), 1);
        assert_eq!(summarize_media_ref(&picked[0]).unwrap().kind, "audio");
        assert!(select_media_refs(&rows, Some("nope"), 1).is_empty());
    }

    #[test]
    fn ignores_user_typed_lookalikes() {
        let rows = vec!["fake [WA-MEDIA:not-a-ticket] here".to_string()];
        assert!(select_media_refs(&rows, None, 3).is_empty());
    }

    #[test]
    fn extension_prefers_mime_and_falls_back_to_kind() {
        assert_eq!(extension_for(Some("image/jpeg"), "image"), "jpg");
        assert_eq!(extension_for(Some("audio/ogg; codecs=opus"), "audio"), "ogg");
        assert_eq!(extension_for(None, "image"), "jpg");
        assert_eq!(extension_for(None, "audio"), "ogg");
        assert_eq!(extension_for(Some("application/x-thing"), ""), "bin");
    }
}
