use rust_embed::RustEmbed;
use tracing::error;

// 定义嵌入式模板资源
#[derive(RustEmbed)]
#[folder = "src/web/templates/"]
#[prefix = "templates/"]
#[include = "*.html"]
pub struct Templates;

// 在开发模式下从文件系统读取模板，在生产模式下使用嵌入式资源
pub fn get_template_content(template_name: &str) -> Option<String> {
    #[cfg(debug_assertions)]
    {
        // 在开发模式下，直接从文件系统读取
        let path = format!("src/web/templates/{}", template_name);
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
        // 在生产模式下，从嵌入式资源读取
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

// 获取所有可用模板的列表
pub fn list_templates() -> Vec<String> {
    #[cfg(debug_assertions)]
    {
        // 在开发模式下，从文件系统读取
        match std::fs::read_dir("src/web/templates") {
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

                // 确保基础模板先加载
                templates.sort_by(|a, b| {
                    // base.html 总是最先加载
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
        // 在生产模式下，从嵌入式资源读取
        let mut templates: Vec<String> = Templates::iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if path_str.starts_with("templates/") && path_str.ends_with(".html") {
                    Some(path_str.strip_prefix("templates/").unwrap().to_string())
                } else {
                    None
                }
            })
            .collect();

        // 确保基础模板先加载
        templates.sort_by(|a, b| {
            // base.html 总是最先加载
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
