// Allow dead code temporarily until client command integration is complete
#![allow(dead_code)]

use dialoguer::{theme::ColorfulTheme, Confirm, Input, Select};

use crate::quilt::quilt_service_client::QuiltServiceClient;

/// Check if we're in an interactive terminal
pub fn is_interactive() -> bool {
    use std::env;

    // Not interactive if:
    // - stdin is not a TTY
    // - stdout is not a TTY
    // - CI environment variable is set
    // - QUILT_NO_INTERACT is set

    if env::var("CI").is_ok() {
        return false;
    }

    if env::var("QUILT_NO_INTERACT").is_ok() {
        return false;
    }

    atty::is(atty::Stream::Stdin) && atty::is(atty::Stream::Stdout)
}

/// Prompt user to select a container image from available .tar.gz files
pub fn prompt_for_image() -> Result<String, String> {
    if !is_interactive() {
        return Err("Cannot prompt for image in non-interactive mode".to_string());
    }

    // Find all .tar.gz files in current directory
    let mut images: Vec<String> = glob::glob("*.tar.gz")
        .map_err(|e| format!("Failed to search for images: {}", e))?
        .filter_map(|entry| entry.ok())
        .filter_map(|path| path.to_str().map(String::from))
        .collect();

    if images.is_empty() {
        return Err("No .tar.gz container images found in current directory".to_string());
    }

    images.sort();

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select container image")
        .items(&images)
        .default(0)
        .interact()
        .map_err(|e| format!("Failed to get selection: {}", e))?;

    Ok(images[selection].clone())
}

/// Prompt user to select a container from running containers
pub async fn prompt_for_container(
    _client: &mut QuiltServiceClient<tonic::transport::Channel>,
) -> Result<String, String> {
    if !is_interactive() {
        return Err("Cannot prompt for container in non-interactive mode".to_string());
    }

    // This would need to list containers - for now, ask for manual input
    // In a full implementation, we'd call a ListContainers RPC
    let container = Input::<String>::with_theme(&ColorfulTheme::default())
        .with_prompt("Container ID or name")
        .interact_text()
        .map_err(|e| format!("Failed to get input: {}", e))?;

    Ok(container)
}

/// Confirm an action with the user
pub fn confirm_action(prompt: &str, default: bool) -> Result<bool, String> {
    if !is_interactive() {
        // In non-interactive mode, use the default
        return Ok(default);
    }

    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .default(default)
        .interact()
        .map_err(|e| format!("Failed to get confirmation: {}", e))
}

/// Get string input from user with a prompt
pub fn get_input(prompt: &str, default: Option<&str>) -> Result<String, String> {
    if !is_interactive() {
        if let Some(def) = default {
            return Ok(def.to_string());
        }
        return Err("Cannot get input in non-interactive mode".to_string());
    }

    let theme = ColorfulTheme::default();
    let mut input = Input::<String>::with_theme(&theme)
        .with_prompt(prompt);

    if let Some(def) = default {
        input = input.default(def.to_string());
    }

    input
        .interact_text()
        .map_err(|e| format!("Failed to get input: {}", e))
}

/// Select from a list of options
pub fn select_from_list(prompt: &str, items: &[String]) -> Result<usize, String> {
    if !is_interactive() {
        return Err("Cannot select from list in non-interactive mode".to_string());
    }

    if items.is_empty() {
        return Err("No items to select from".to_string());
    }

    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(items)
        .default(0)
        .interact()
        .map_err(|e| format!("Failed to get selection: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_interactive_checks_env() {
        std::env::set_var("CI", "true");
        assert!(!is_interactive());
        std::env::remove_var("CI");

        std::env::set_var("QUILT_NO_INTERACT", "1");
        assert!(!is_interactive());
        std::env::remove_var("QUILT_NO_INTERACT");
    }
}
