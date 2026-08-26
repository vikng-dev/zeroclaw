//! Automatic media understanding pipeline for inbound channel messages.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::Duration;
use zeroclaw_config::schema::MediaPipelineConfig;

use super::super::transcription::TranscriptionManager;

// Re-export media types from zeroclaw-types for backwards compatibility.
pub use zeroclaw_api::media::{MediaAttachment, MediaKind};

/// The media understanding pipeline.
/// Consumes a message's text and attachments, returning enriched text with
/// media annotations prepended.
pub struct MediaPipeline<'a> {
    config: &'a MediaPipelineConfig,
    transcription_manager: Option<&'a TranscriptionManager>,
    vision_available: bool,
    files_root: Option<PathBuf>,
}

impl<'a> MediaPipeline<'a> {
    /// Create a new pipeline. `vision_available` indicates whether the current
    /// model provider supports vision (image description). `transcription_manager`
    /// is `None` when transcription is disabled at the channel level — audio
    /// attachments fall back to `[Audio: attached]` annotations.
    pub fn new(
        config: &'a MediaPipelineConfig,
        transcription_manager: Option<&'a TranscriptionManager>,
        vision_available: bool,
    ) -> Self {
        Self {
            config,
            transcription_manager,
            vision_available,
            files_root: None,
        }
    }

    /// Land attachment bytes as files under `root` so annotations can carry a
    /// path instead of megabytes of base64: the persisted turn stays small,
    /// vision re-inflates `[IMAGE:<path>]` from disk, and the agent's file
    /// tools can reuse the file (upload, move, read). Without a root the
    /// pipeline keeps its inline/announce-only behavior.
    #[must_use]
    pub fn with_files_root(mut self, root: Option<PathBuf>) -> Self {
        self.files_root = root;
        self
    }

    /// Process a message's attachments and return enriched text.
    /// If the pipeline is disabled via config, returns `original_text` unchanged.
    pub async fn process(&self, original_text: &str, attachments: &[MediaAttachment]) -> String {
        if !self.config.enabled || attachments.is_empty() {
            return original_text.to_string();
        }

        let mut annotations = Vec::new();

        for attachment in attachments {
            match attachment.kind() {
                MediaKind::Audio if self.config.transcribe_audio => {
                    let annotation = self.process_audio(attachment).await;
                    annotations.push(annotation);
                }
                MediaKind::Image if self.config.describe_images => {
                    let annotation = self.process_image(attachment).await;
                    annotations.push(annotation);
                }
                MediaKind::Video if self.config.summarize_video => {
                    let annotation = self.process_video(attachment).await;
                    annotations.push(annotation);
                }
                MediaKind::Document if self.config.announce_documents => {
                    annotations.push(self.process_document(attachment).await);
                }
                _ => {}
            }
        }

        if annotations.is_empty() {
            return original_text.to_string();
        }

        let mut enriched = String::with_capacity(
            annotations.iter().map(|a| a.len() + 1).sum::<usize>() + original_text.len() + 2,
        );

        for annotation in &annotations {
            enriched.push_str(annotation);
            enriched.push('\n');
        }

        if !original_text.is_empty() {
            enriched.push('\n');
            enriched.push_str(original_text);
        }

        enriched.trim().to_string()
    }

    /// Transcribe an audio attachment using the existing transcription infra.
    /// The raw audio also lands as a file when a files root is set, so "save
    /// that voice note" stays possible after the transcription is delivered.
    async fn process_audio(&self, attachment: &MediaAttachment) -> String {
        let saved = self.save_attachment(&attachment.file_name, &attachment.data).await;
        let file_note = saved
            .as_deref()
            .map(|p| format!("\n[Audio file: {} saved at {}]", attachment.file_name, p.display()))
            .unwrap_or_default();

        let Some(manager) = self.transcription_manager else {
            return format!("[Audio: attached]{file_note}");
        };

        match manager
            .transcribe(&attachment.data, &attachment.file_name)
            .await
        {
            Ok(text) => {
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    format!("[Audio transcription: (empty)]{file_note}")
                } else {
                    format!("[Audio transcription: {trimmed}]{file_note}")
                }
            }
            Err(err) => {
                ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown).with_attrs(::serde_json::json!({"file": attachment.file_name, "error": format!("{}", err)})), "Media pipeline: audio transcription failed");
                format!("[Audio: transcription failed]{file_note}")
            }
        }
    }

    /// Annotate an image. With a files root the (vision-normalized) bytes land
    /// on disk and the annotation carries `[IMAGE:<path>]` — the provider
    /// layer re-inflates path markers from disk, the persisted turn stays a
    /// couple of hundred bytes instead of megabytes of base64, and the path
    /// is real input for file tools ("upload this to Drive"). Inline base64
    /// remains the fallback when no root is set or the write fails.
    async fn process_image(&self, attachment: &MediaAttachment) -> String {
        let (mime, data) = image_payload_for_vision(attachment);
        let file_name = image_file_name(&attachment.file_name, &mime);
        if let Some(path) = self.save_attachment(&file_name, data.as_ref()).await {
            let shown = path.display();
            return if self.vision_available {
                format!(
                    "[Image: {} attached, saved at {shown} — the path works with file tools]\n[IMAGE:{shown}]",
                    attachment.file_name
                )
            } else {
                format!("[Image: {} attached, saved at {shown}]", attachment.file_name)
            };
        }
        if self.vision_available {
            let b64 = STANDARD.encode(data.as_ref());
            format!(
                "[Image: {} attached, will be processed by vision model]\n[IMAGE:data:{};base64,{}]",
                attachment.file_name, mime, b64
            )
        } else {
            format!("[Image: {} attached]", attachment.file_name)
        }
    }

    /// Summarize a video attachment.
    /// Video analysis requires external APIs not currently integrated,
    /// but the bytes still land as a reusable file when a root is set.
    async fn process_video(&self, attachment: &MediaAttachment) -> String {
        match self.save_attachment(&attachment.file_name, &attachment.data).await {
            Some(path) => format!(
                "[Video: {} attached, saved at {}]",
                attachment.file_name,
                path.display()
            ),
            None => format!("[Video: {} attached]", attachment.file_name),
        }
    }

    /// Announce a document. There is no extraction step: the point is that the
    /// agent learns a file arrived, what it is, and (with a files root) where
    /// its bytes are, instead of reading a bare `[Document]` marker.
    async fn process_document(&self, attachment: &MediaAttachment) -> String {
        let mut annotation = format!("[Document: {} attached", attachment.file_name);
        if let Some(ref mime) = attachment.mime_type {
            annotation.push_str(&format!(", type {mime}"));
        }
        annotation.push_str(&format!(", {} bytes", attachment.data.len()));
        if let Some(path) = self
            .save_attachment(&attachment.file_name, &attachment.data)
            .await
        {
            annotation.push_str(&format!(", saved at {}", path.display()));
        }
        annotation.push(']');
        annotation
    }

    /// Write attachment bytes under the files root, returning the landed
    /// path. `None` when no root is configured or the write fails — callers
    /// fall back to their inline/announce-only annotation. Old files are
    /// swept opportunistically on the way past.
    async fn save_attachment(&self, file_name: &str, data: &[u8]) -> Option<PathBuf> {
        let root = self.files_root.as_ref()?;
        if let Err(err) = tokio::fs::create_dir_all(root).await {
            ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown).with_attrs(::serde_json::json!({"dir": root.display().to_string(), "error": format!("{}", err)})), "Media pipeline: cannot create files dir");
            return None;
        }
        sweep_old_files(root, Duration::from_secs(self.config.retention_hours.saturating_mul(3600))).await;

        let path = unique_media_path(root, file_name).await;
        match tokio::fs::write(&path, data).await {
            Ok(()) => Some(path),
            Err(err) => {
                ::zeroclaw_log::record!(WARN, ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_outcome(::zeroclaw_log::EventOutcome::Unknown).with_attrs(::serde_json::json!({"path": path.display().to_string(), "error": format!("{}", err)})), "Media pipeline: failed to save attachment");
                None
            }
        }
    }
}

/// A collision-free landing path: `<millis>_<sanitized name>`, with a counter
/// suffix in the (already unlikely) same-millisecond same-name case.
async fn unique_media_path(root: &Path, file_name: &str) -> PathBuf {
    let safe = sanitize_file_name(file_name);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let mut candidate = root.join(format!("{millis}_{safe}"));
    let mut counter = 1u32;
    while tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
        candidate = root.join(format!("{millis}-{counter}_{safe}"));
        counter += 1;
    }
    candidate
}

/// Keep only the final path component and replace anything outside
/// `[A-Za-z0-9._-]` so a platform-supplied name can never traverse or need
/// shell quoting.
fn sanitize_file_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(['.', '_']).to_string();
    if trimmed.is_empty() {
        "attachment".to_string()
    } else {
        trimmed
    }
}

/// The saved image file's name must match the bytes actually written:
/// vision normalization can turn a `.webp` into PNG bytes.
fn image_file_name(original: &str, mime: &str) -> String {
    if mime.eq_ignore_ascii_case("image/png") && !original.to_ascii_lowercase().ends_with(".png") {
        let stem = original.rsplit_once('.').map_or(original, |(stem, _)| stem);
        format!("{stem}.png")
    } else {
        original.to_string()
    }
}

/// Best-effort removal of landed files older than `max_age`. Errors are
/// ignored: retention is hygiene, not correctness, and the next write sweeps
/// again.
async fn sweep_old_files(root: &Path, max_age: Duration) {
    if max_age.is_zero() {
        return;
    }
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return;
    };
    let now = std::time::SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if now.duration_since(modified).unwrap_or_default() > max_age {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

fn image_payload_for_vision(attachment: &MediaAttachment) -> (String, Cow<'_, [u8]>) {
    let mime = attachment.mime_type.as_deref().unwrap_or("image/jpeg");

    #[cfg(feature = "image-normalization")]
    if is_webp_attachment(attachment, mime) {
        match webp_to_png(&attachment.data) {
            Ok(png) => return ("image/png".to_string(), Cow::Owned(png)),
            Err(err) => {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                        .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                        .with_attrs(::serde_json::json!({
                            "file": attachment.file_name,
                            "error": format!("{}", err),
                            "error_key": "media_pipeline_webp_to_png_failed",
                        })),
                    "Media pipeline: failed to normalize WebP image for vision"
                );
            }
        }
    }

    (mime.to_string(), Cow::Borrowed(&attachment.data))
}

#[cfg(feature = "image-normalization")]
fn is_webp_attachment(attachment: &MediaAttachment, mime: &str) -> bool {
    mime.eq_ignore_ascii_case("image/webp")
        || attachment
            .file_name
            .rsplit_once('.')
            .is_some_and(|(_, ext)| ext.eq_ignore_ascii_case("webp"))
}

#[cfg(feature = "image-normalization")]
fn webp_to_png(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let image = image::load_from_memory_with_format(data, image::ImageFormat::WebP)?;
    let mut cursor = std::io::Cursor::new(Vec::new());
    image.write_to(&mut cursor, image::ImageFormat::Png)?;
    Ok(cursor.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_pipeline_config(enabled: bool) -> MediaPipelineConfig {
        MediaPipelineConfig {
            enabled,
            ..MediaPipelineConfig::default()
        }
    }

    fn sample_audio() -> MediaAttachment {
        MediaAttachment {
            file_name: "voice.ogg".to_string(),
            data: vec![0u8; 100],
            mime_type: Some("audio/ogg".to_string()),
        }
    }

    fn sample_image() -> MediaAttachment {
        MediaAttachment {
            file_name: "photo.jpg".to_string(),
            data: vec![0u8; 50],
            mime_type: Some("image/jpeg".to_string()),
        }
    }

    fn sample_document() -> MediaAttachment {
        MediaAttachment {
            file_name: "invoice.pdf".to_string(),
            data: vec![0u8; 4096],
            mime_type: Some("application/pdf".to_string()),
        }
    }

    fn sample_video() -> MediaAttachment {
        MediaAttachment {
            file_name: "clip.mp4".to_string(),
            data: vec![0u8; 200],
            mime_type: Some("video/mp4".to_string()),
        }
    }

    #[test]
    fn media_kind_from_mime() {
        let audio = MediaAttachment {
            file_name: "file".to_string(),
            data: vec![],
            mime_type: Some("audio/ogg".to_string()),
        };
        assert_eq!(audio.kind(), MediaKind::Audio);

        let image = MediaAttachment {
            file_name: "file".to_string(),
            data: vec![],
            mime_type: Some("image/png".to_string()),
        };
        assert_eq!(image.kind(), MediaKind::Image);

        let video = MediaAttachment {
            file_name: "file".to_string(),
            data: vec![],
            mime_type: Some("video/mp4".to_string()),
        };
        assert_eq!(video.kind(), MediaKind::Video);
    }

    #[test]
    fn media_kind_from_extension() {
        let audio = MediaAttachment {
            file_name: "voice.ogg".to_string(),
            data: vec![],
            mime_type: None,
        };
        assert_eq!(audio.kind(), MediaKind::Audio);

        let image = MediaAttachment {
            file_name: "photo.png".to_string(),
            data: vec![],
            mime_type: None,
        };
        assert_eq!(image.kind(), MediaKind::Image);

        let video = MediaAttachment {
            file_name: "clip.mp4".to_string(),
            data: vec![],
            mime_type: None,
        };
        assert_eq!(video.kind(), MediaKind::Video);

        let unknown = MediaAttachment {
            file_name: "data.bin".to_string(),
            data: vec![],
            mime_type: None,
        };
        assert_eq!(unknown.kind(), MediaKind::Unknown);
    }

    #[tokio::test]
    async fn disabled_pipeline_returns_original_text() {
        let config = default_pipeline_config(false);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline.process("hello", &[sample_audio()]).await;
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn empty_attachments_returns_original_text() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline.process("hello", &[]).await;
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn image_annotation_with_vision() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, true);

        let result = pipeline.process("check this", &[sample_image()]).await;
        assert!(
            result.contains("[Image: photo.jpg attached, will be processed by vision model]"),
            "expected vision annotation, got: {result}"
        );
        assert!(
            result.contains("[IMAGE:data:image/jpeg;base64,"),
            "expected image data marker, got: {result}"
        );
        assert!(result.contains("check this"));
    }

    #[cfg(feature = "image-normalization")]
    #[tokio::test]
    async fn webp_image_is_normalized_to_png_for_vision() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, true);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let webp = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            1,
            1,
            image::Rgba([255, 0, 0, 255]),
        ));
        webp.write_to(&mut cursor, image::ImageFormat::WebP)
            .expect("test WebP should encode");

        let sticker = MediaAttachment {
            file_name: "sticker.webp".to_string(),
            data: cursor.into_inner(),
            mime_type: Some("image/webp".to_string()),
        };

        let result = pipeline.process("what is this?", &[sticker]).await;

        assert!(result.contains("[IMAGE:data:image/png;base64,"));
        assert!(!result.contains("[IMAGE:data:image/webp;base64,"));
        assert!(result.contains("what is this?"));
    }

    #[tokio::test]
    async fn image_annotation_without_vision() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline.process("check this", &[sample_image()]).await;
        assert!(
            result.contains("[Image: photo.jpg attached]"),
            "expected basic image annotation, got: {result}"
        );
        assert!(
            !result.contains("[IMAGE:data:"),
            "non-vision path must not inline image data, got: {result}"
        );
    }

    #[tokio::test]
    async fn video_annotation() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline.process("watch", &[sample_video()]).await;
        assert!(
            result.contains("[Video: clip.mp4 attached]"),
            "expected video annotation, got: {result}"
        );
    }

    #[tokio::test]
    async fn audio_without_transcription_enabled() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline.process("", &[sample_audio()]).await;
        assert_eq!(result, "[Audio: attached]");
    }

    #[tokio::test]
    async fn multiple_attachments_produce_multiple_annotations() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let attachments = vec![sample_audio(), sample_image(), sample_video()];
        let result = pipeline.process("context", &attachments).await;

        assert!(
            result.contains("[Audio: attached]"),
            "missing audio annotation"
        );
        assert!(
            result.contains("[Image: photo.jpg attached]"),
            "missing image annotation"
        );
        assert!(
            result.contains("[Video: clip.mp4 attached]"),
            "missing video annotation"
        );
        assert!(result.contains("context"), "missing original text");
    }

    #[tokio::test]
    async fn disabled_sub_features_skip_processing() {
        let config = MediaPipelineConfig {
            enabled: true,
            transcribe_audio: false,
            describe_images: false,
            summarize_video: false,
            announce_documents: false,
            ..MediaPipelineConfig::default()
        };
        let pipeline = MediaPipeline::new(&config, None, false);

        let attachments = vec![
            sample_audio(),
            sample_image(),
            sample_video(),
            sample_document(),
        ];
        let result = pipeline.process("hello", &attachments).await;
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn documents_are_announced_with_name_type_and_size() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false);

        let result = pipeline
            .process("please read it", &[sample_document()])
            .await;
        assert_eq!(
            result,
            "[Document: invoice.pdf attached, type application/pdf, 4096 bytes]\n\nplease read it"
        );
    }

    #[tokio::test]
    async fn image_lands_as_file_with_path_marker_for_vision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, true)
            .with_files_root(Some(dir.path().to_path_buf()));

        let result = pipeline.process("check this", &[sample_image()]).await;

        assert!(
            !result.contains("[IMAGE:data:"),
            "with a files root the annotation must not inline base64, got: {result}"
        );
        let marker_path = result
            .split("[IMAGE:")
            .nth(1)
            .and_then(|rest| rest.split(']').next())
            .expect("path marker present");
        assert!(
            std::path::Path::new(marker_path).is_file(),
            "marker must point at a real file: {marker_path}"
        );
        assert_eq!(std::fs::read(marker_path).unwrap(), sample_image().data);
        assert!(result.contains("saved at"), "got: {result}");
        assert!(result.contains("check this"));
    }

    #[tokio::test]
    async fn document_lands_as_file_and_annotation_names_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false)
            .with_files_root(Some(dir.path().to_path_buf()));

        let result = pipeline.process("", &[sample_document()]).await;

        assert!(result.contains("saved at"), "got: {result}");
        let saved: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(saved.len(), 1);
        let name = saved[0].as_ref().unwrap().file_name();
        assert!(
            name.to_string_lossy().ends_with("_invoice.pdf"),
            "unexpected landed name: {name:?}"
        );
    }

    #[tokio::test]
    async fn unwritable_files_root_falls_back_to_inline() {
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, true)
            .with_files_root(Some(PathBuf::from("/dev/null/not-a-dir")));

        let result = pipeline.process("check this", &[sample_image()]).await;
        assert!(
            result.contains("[IMAGE:data:image/jpeg;base64,"),
            "fallback must keep the vision flow alive, got: {result}"
        );
    }

    #[tokio::test]
    async fn same_name_attachments_do_not_collide() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = default_pipeline_config(true);
        let pipeline = MediaPipeline::new(&config, None, false)
            .with_files_root(Some(dir.path().to_path_buf()));

        let result = pipeline
            .process("", &[sample_document(), sample_document()])
            .await;

        assert_eq!(result.matches("saved at").count(), 2);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn sweep_removes_only_stale_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stale = dir.path().join("old.bin");
        std::fs::write(&stale, b"x").unwrap();
        let ago = std::time::SystemTime::now() - Duration::from_secs(10 * 3600);
        let file = std::fs::File::options().write(true).open(&stale).unwrap();
        file.set_modified(ago).unwrap();
        drop(file);
        let fresh = dir.path().join("new.bin");
        std::fs::write(&fresh, b"y").unwrap();

        sweep_old_files(dir.path(), Duration::from_secs(3600)).await;

        assert!(!stale.exists(), "stale file must be swept");
        assert!(fresh.exists(), "fresh file must survive");
    }

    #[test]
    fn sanitize_strips_traversal_and_junk() {
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("my photo (1).jpg"), "my_photo__1_.jpg");
        assert_eq!(sanitize_file_name("..."), "attachment");
        assert_eq!(sanitize_file_name("c:\\x\\évil né.pdf"), "vil_n_.pdf");
    }

    #[test]
    fn image_file_name_tracks_png_normalization() {
        assert_eq!(image_file_name("sticker.webp", "image/png"), "sticker.png");
        assert_eq!(image_file_name("photo.jpg", "image/jpeg"), "photo.jpg");
        assert_eq!(image_file_name("shot.png", "image/png"), "shot.png");
    }
}
