#[cfg(debug_assertions)]
use std::path::Path;
use std::path::PathBuf;

use rust_embed::RustEmbed;
use tracing::error;
#[cfg(debug_assertions)]
use tracing::{debug, warn};

const EMBEDDED_TEMPLATE_PREFIX: &str = "templates/";

#[cfg(debug_assertions)]
const TEMPLATE_DIR_ENV_VAR: &str = "QUEBEC_TEMPLATE_DIR";
#[cfg(debug_assertions)]
const TEMPLATE_DIR_RELATIVE: &str = "src/control_plane/templates";
#[cfg(debug_assertions)]
const STATIC_DIR_RELATIVE: &str = "src/control_plane/static";

// Define embedded template resources
// Note: Templates is only used in release builds (embedded assets),
// in debug builds we read directly from filesystem for hot-reload
#[derive(RustEmbed)]
#[folder = "src/control_plane/templates/"]
#[prefix = "templates/"]
#[include = "*.html"]
#[allow(dead_code)]
pub struct Templates;

// Static JS/CSS assets vendored from CDN (see scripts/download_assets.py)
#[derive(RustEmbed)]
#[folder = "src/control_plane/static/"]
#[allow(dead_code)]
pub struct StaticAssets;

fn sort_templates(templates: &mut [String]) {
    templates.sort_by(|a, b| {
        if a == "base.html" {
            std::cmp::Ordering::Less
        } else if b == "base.html" {
            std::cmp::Ordering::Greater
        } else {
            a.cmp(b)
        }
    });
}

fn embedded_template_content(template_name: &str) -> Option<String> {
    let asset_path = format!("{EMBEDDED_TEMPLATE_PREFIX}{template_name}");
    match Templates::get(&asset_path) {
        Some(content) => match std::str::from_utf8(content.data.as_ref()) {
            Ok(s) => Some(s.to_string()),
            Err(e) => {
                error!("Failed to decode template {}: {}", template_name, e);
                None
            }
        },
        None => {
            error!("Template {} not found in embedded assets", template_name);
            None
        }
    }
}

fn embedded_template_list() -> Vec<String> {
    let mut templates: Vec<String> = Templates::iter()
        .filter_map(|path| {
            let path_str = path.as_ref();
            if path_str.starts_with(EMBEDDED_TEMPLATE_PREFIX) && path_str.ends_with(".html") {
                path_str
                    .strip_prefix(EMBEDDED_TEMPLATE_PREFIX)
                    .map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();

    sort_templates(&mut templates);
    templates
}

#[cfg(debug_assertions)]
fn manifest_dir(relative_path: &str) -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path);
    if path.is_dir() {
        Some(path)
    } else {
        warn!(
            "Expected control plane resource directory {} does not exist",
            path.display()
        );
        None
    }
}

#[cfg(debug_assertions)]
fn template_dir_from_env() -> Option<PathBuf> {
    std::env::var_os(TEMPLATE_DIR_ENV_VAR)
        .map(PathBuf::from)
        .and_then(|path| {
            if path.is_dir() {
                Some(path)
            } else {
                warn!(
                    "{} is set to {}, but that directory does not exist",
                    TEMPLATE_DIR_ENV_VAR,
                    path.display()
                );
                None
            }
        })
}

#[cfg(debug_assertions)]
fn template_dir() -> Option<PathBuf> {
    template_dir_from_env().or_else(|| manifest_dir(TEMPLATE_DIR_RELATIVE))
}

pub fn template_file_path(template_name: &str) -> Option<PathBuf> {
    #[cfg(debug_assertions)]
    {
        return template_dir().map(|dir| dir.join(template_name));
    }

    #[cfg(not(debug_assertions))]
    {
        let _ = template_name;
        None
    }
}

#[cfg(debug_assertions)]
pub fn static_asset_path(filename: &str) -> Option<PathBuf> {
    manifest_dir(STATIC_DIR_RELATIVE).map(|dir| dir.join(filename))
}

pub fn get_template_content(template_name: &str) -> Option<String> {
    #[cfg(debug_assertions)]
    {
        if let Some(path) = template_file_path(template_name) {
            match std::fs::read_to_string(&path) {
                Ok(content) => return Some(content),
                Err(e) => {
                    error!(
                        "Failed to read template {} from {}: {}",
                        template_name,
                        path.display(),
                        e
                    );
                }
            }
        }

        debug!(
            "No filesystem template directory available, falling back to embedded template {}",
            template_name
        );
    }

    embedded_template_content(template_name)
}

// Get list of all available templates
pub fn list_templates() -> Vec<String> {
    #[cfg(debug_assertions)]
    {
        if let Some(dir) = template_dir() {
            match std::fs::read_dir(&dir) {
                Ok(entries) => {
                    let mut templates: Vec<String> = entries
                        .filter_map(|entry| {
                            let entry = entry.ok()?;
                            let path = entry.path();
                            if path.is_file() && path.extension().map_or(false, |ext| ext == "html")
                            {
                                path.file_name()
                                    .and_then(|name| name.to_str())
                                    .map(|name| name.to_string())
                            } else {
                                None
                            }
                        })
                        .collect();

                    sort_templates(&mut templates);
                    return templates;
                }
                Err(e) => {
                    error!(
                        "Failed to read templates directory {}: {}",
                        dir.display(),
                        e
                    );
                }
            }
        }

        debug!("No filesystem template directory available, falling back to embedded templates");
    }

    embedded_template_list()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_templates_include_base_template() {
        let templates = embedded_template_list();
        assert_eq!(templates.first().map(String::as_str), Some("base.html"));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn manifest_template_path_points_to_control_plane_templates() {
        let path = template_file_path("base.html").expect("template path should resolve in debug");
        assert!(path.ends_with("src/control_plane/templates/base.html"));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn manifest_static_path_points_to_control_plane_static_dir() {
        let path =
            static_asset_path("tailwindcss.js").expect("static asset path should resolve in debug");
        assert!(path.ends_with("src/control_plane/static/tailwindcss.js"));
    }
}
