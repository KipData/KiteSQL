// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

pub const STATISTICS_META_SUBDIR: &str = "kite_sql_statistics_metas";

#[cfg(not(target_arch = "wasm32"))]
fn default_statistics_base_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|path| path.join(STATISTICS_META_SUBDIR))
}

#[cfg(target_arch = "wasm32")]
fn default_statistics_base_dir() -> Option<PathBuf> {
    Some(PathBuf::from(STATISTICS_META_SUBDIR))
}

/// Returns the statistics base directory, using a platform default.
pub fn statistics_base_dir() -> Option<PathBuf> {
    default_statistics_base_dir()
}

/// Retrieves the statistics base directory, panicking if it cannot be determined.
pub fn require_statistics_base_dir() -> PathBuf {
    statistics_base_dir()
        .unwrap_or_else(|| panic!("statistics_base_dir is empty and no default is available"))
}

#[cfg(target_arch = "wasm32")]
mod wasm_storage {
    use crate::errors::DatabaseError;
    use once_cell::sync::Lazy;
    use std::collections::HashMap;
    use std::sync::Mutex;

    static MEMORY_STORE: Lazy<Mutex<HashMap<String, String>>> =
        Lazy::new(|| Mutex::new(HashMap::new()));

    pub fn storage_keys_with_prefix(prefix: &str) -> Result<Vec<String>, DatabaseError> {
        let store = MEMORY_STORE.lock().unwrap();
        Ok(store
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    pub fn remove_storage_key(key: &str) -> Result<(), DatabaseError> {
        MEMORY_STORE.lock().unwrap().remove(key);
        Ok(())
    }

    pub fn set_storage_item(key: &str, value: &str) -> Result<(), DatabaseError> {
        MEMORY_STORE
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    pub fn get_storage_item(key: &str) -> Result<Option<String>, DatabaseError> {
        Ok(MEMORY_STORE.lock().unwrap().get(key).cloned())
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm_storage::{
    get_storage_item as wasm_get_storage_item, remove_storage_key as wasm_remove_storage_key,
    set_storage_item as wasm_set_storage_item,
    storage_keys_with_prefix as wasm_storage_keys_with_prefix,
};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn uses_default_statistics_base_dir() {
        let expected = dirs::home_dir()
            .expect("home dir")
            .join(STATISTICS_META_SUBDIR);
        assert_eq!(statistics_base_dir(), Some(expected));
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;
    use std::path::PathBuf;
    use wasm_bindgen_test::*;

    fn clear_prefix(prefix: &str) {
        let keys = wasm_storage_keys_with_prefix(prefix).expect("list keys");

        for key in keys {
            wasm_remove_storage_key(&key).expect("remove key");
        }
    }

    #[wasm_bindgen_test]
    fn base_dir_and_storage_roundtrip() {
        let base_prefix = STATISTICS_META_SUBDIR;
        let prefix_with_sep = format!("{base_prefix}/");

        clear_prefix(&prefix_with_sep);

        let configured = require_statistics_base_dir();
        assert_eq!(configured, PathBuf::from(base_prefix));

        let key = format!("{prefix_with_sep}sample");
        let value = "value";

        wasm_set_storage_item(&key, value).expect("set item");
        assert_eq!(
            wasm_get_storage_item(&key).expect("get item"),
            Some(value.to_string())
        );

        let keys = wasm_storage_keys_with_prefix(&prefix_with_sep).expect("keys");
        assert!(keys.iter().any(|existing| existing == &key));

        wasm_remove_storage_key(&key).expect("remove key");
        let keys = wasm_storage_keys_with_prefix(&prefix_with_sep).expect("keys after remove");
        assert!(!keys.iter().any(|existing| existing == &key));
    }
}
