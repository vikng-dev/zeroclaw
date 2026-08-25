//! Inline media claim tickets.
//!
//! A channel that receives media it chooses not to download (e.g. passive
//! WhatsApp group context) can keep a compact re-download descriptor in the
//! history turn instead of a dead placeholder. The descriptor is a
//! base64-encoded JSON object inside a `[WA-MEDIA:...]` marker — base64 so the
//! payload can never contain `]` and every bracket scanner in the tree treats
//! the marker as one opaque token. The `fetch_media` tool finds markers in the
//! persisted history and hands the payload back to the owning channel, which
//! is the only party that knows how to redeem it.

use base64::{Engine as _, engine::general_purpose::STANDARD};

/// Marker prefix. The kind token is deliberately absent from every
/// attachment/media kind list (channel egress, provider multimodal, memory
/// consolidation), so existing parsers pass the marker through verbatim.
pub const MEDIA_REF_PREFIX: &str = "[WA-MEDIA:";

/// What a descriptor says about itself without giving the crypto fields to
/// the model: a stable short id (from the encrypted-file hash) and the kind.
pub struct MediaRefSummary {
    pub id: String,
    pub kind: String,
}

/// Extract every marker payload from a text, in order of appearance.
pub fn extract_media_refs(content: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel) = content[cursor..].find(MEDIA_REF_PREFIX) {
        let start = cursor + rel + MEDIA_REF_PREFIX.len();
        match content[start..].find(']') {
            Some(rel_end) => {
                refs.push(content[start..start + rel_end].to_string());
                cursor = start + rel_end + 1;
            }
            None => break,
        }
    }
    refs
}

/// Replace every marker in a text using the supplied renderer. A payload the
/// renderer declines (None) is left in place verbatim.
pub fn replace_media_refs(content: &str, mut render: impl FnMut(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(content.len());
    let mut cursor = 0usize;
    while let Some(rel) = content[cursor..].find(MEDIA_REF_PREFIX) {
        let start = cursor + rel;
        let payload_start = start + MEDIA_REF_PREFIX.len();
        let Some(rel_end) = content[payload_start..].find(']') else {
            break;
        };
        let payload = &content[payload_start..payload_start + rel_end];
        out.push_str(&content[cursor..start]);
        match render(payload) {
            Some(replacement) => out.push_str(&replacement),
            None => out.push_str(&content[start..payload_start + rel_end + 1]),
        }
        cursor = payload_start + rel_end + 1;
    }
    out.push_str(&content[cursor..]);
    out
}

/// Decode a payload far enough to identify it. Returns None on anything that
/// is not a well-formed descriptor (including user-typed lookalikes).
pub fn summarize_media_ref(payload: &str) -> Option<MediaRefSummary> {
    let raw = STANDARD.decode(payload.trim()).ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    let kind = doc.get("k")?.as_str()?.to_string();
    let hash = doc.get("es")?.as_str()?;
    if hash.len() < 8 {
        return None;
    }
    Some(MediaRefSummary {
        id: hash[..8].to_string(),
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(kind: &str, es: &str) -> String {
        STANDARD.encode(format!(r#"{{"v":1,"k":"{kind}","es":"{es}","dp":"/x"}}"#))
    }

    #[test]
    fn extracts_and_replaces_markers() {
        let p = payload("audio", "AbCdEfGhIj");
        let text = format!("[Voice]\n[WA-MEDIA:{p}] trailing");
        assert_eq!(extract_media_refs(&text), vec![p.clone()]);
        let replaced = replace_media_refs(&text, |x| {
            assert_eq!(x, p);
            Some("[gone]".to_string())
        });
        assert_eq!(replaced, "[Voice]\n[gone] trailing");
    }

    #[test]
    fn summary_ids_from_hash_and_rejects_garbage() {
        let s = summarize_media_ref(&payload("audio", "AbCdEfGhIj")).unwrap();
        assert_eq!(s.id, "AbCdEfGh");
        assert_eq!(s.kind, "audio");
        assert!(summarize_media_ref("not-base64!").is_none());
        assert!(summarize_media_ref(&STANDARD.encode("{\"v\":1}")).is_none());
    }

    #[test]
    fn unrenderable_payloads_survive_verbatim() {
        let text = "[WA-MEDIA:junk] tail";
        assert_eq!(replace_media_refs(text, |_| None), text);
    }
}
