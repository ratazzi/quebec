use rust_embed::RustEmbed;
use tracing::error;

// Define embedded template resources
#[derive(RustEmbed)]
#[folder = "src/control_plane/templates/"]
#[prefix = "templates/"]
#[include = "*.html"]
pub struct Templates;

// Read templates from filesystem in development mode, use embedded resources in production mode
pub fn get_template_content(template_name: &str) -> Option<String> {
    #[cfg(debug_assertions)]
    {
        // In development mode, read directly from filesystem
        let path = format!("src/control_plane/templates/{}", template_name);
        match std::fs::read_to_string(&path) {
            Ok(content) => Some(content),
            Err(e) => {
                error!("Failed to read template {} from disk: {}", template_name, e);
                None
            }
        }
    }

    #[cfg(not(debug_assertions))]
    {
        // In production mode, read from embedded resources
        let asset_path = format!("templates/{}", template_name);
        match Templates::get(&asset_path) {
            Some(content) => {
                match std::str::from_utf8(content.data.as_ref()) {
                    Ok(s) => Some(s.to_string()),
                    Err(e) => {
                        error!("Failed to decode template {}: {}", template_name, e);
                        None
                    }
                }
            },
            None => {
                error!("Template {} not found in embedded assets", template_name);
                None
            }
        }
    }
}

// Get list of all available templates
pub fn list_templates() -> Vec<String> {
    #[cfg(debug_assertions)]
    {
        // In development mode, read from filesystem
        match std::fs::read_dir("src/control_plane/templates") {
            Ok(entries) => {
                let mut templates: Vec<String> = entries
                    .filter_map(|entry| {
                        let entry = entry.ok()?;
                        let path = entry.path();
                        if path.is_file() && path.extension().map_or(false, |ext| ext == "html") {
                            path.file_name()
                                .and_then(|name| name.to_str())
                                .map(|name| name.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Ensure base templates are loaded first
                templates.sort_by(|a, b| {
                    // base.html is always loaded first
                    if a == "base.html" {
                        std::cmp::Ordering::Less
                    } else if b == "base.html" {
                        std::cmp::Ordering::Greater
                    } else {
                        a.cmp(b)
                    }
                });

                templates
            },
            Err(e) => {
                error!("Failed to read templates directory: {}", e);
                Vec::new()
            }
        }
    }

    #[cfg(not(debug_assertions))]
    {
        // In production mode, read from embedded resources
        let mut templates: Vec<String> = Templates::iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if path_str.starts_with("templates/") && path_str.ends_with(".html") {
                    path_str.strip_prefix("templates/").map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();

        // Ensure base templates are loaded first
        templates.sort_by(|a, b| {
            // base.html is always loaded first
            if a == "base.html" {
                std::cmp::Ordering::Less
            } else if b == "base.html" {
                std::cmp::Ordering::Greater
            } else {
                a.cmp(b)
            }
        });

        templates
    }
}
