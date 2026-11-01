//! per-peer `context_token` 状态机存储：内存 + `state/weixin.context-tokens.json` 原子写。
//!
//! 语义（Hermes-CN-Core weixin.py，MIT）：入站消息携带的 context_token 更新该 peer 的
//! 有效 token；发送 -14/stale 时 evict 并无 token 重试一次。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{json, Value};

use super::write_secret_file;

#[derive(Debug)]
pub struct TokenStore {
    path: PathBuf,
    map: Mutex<HashMap<String, String>>,
}

impl TokenStore {
    /// 从磁盘装载（文件缺失/损坏按空表起，不报错）。
    pub fn load(path: PathBuf) -> Self {
        let mut map = HashMap::new();
        if let Ok(raw) = std::fs::read_to_string(&path) {
            if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                if let Some(tokens) = v.get("tokens").and_then(|x| x.as_object()) {
                    for (peer, tok) in tokens {
                        if let Some(t) = tok.as_str() {
                            if !t.is_empty() {
                                map.insert(peer.clone(), t.to_string());
                            }
                        }
                    }
                }
            }
        }
        Self {
            path,
            map: Mutex::new(map),
        }
    }

    /// 空表（测试用）。
    pub fn empty(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, peer: &str) -> Option<String> {
        self.map.lock().unwrap().get(peer).cloned()
    }

    /// 写入/更新 token 并落盘（失败仅告警，不影响内存态）。
    pub fn set(&self, peer: &str, token: &str) {
        self.map
            .lock()
            .unwrap()
            .insert(peer.to_string(), token.to_string());
        self.persist();
    }

    /// 失效某 peer 的 token（-14/stale 后调用）。
    pub fn evict(&self, peer: &str) {
        self.map.lock().unwrap().remove(peer);
        self.persist();
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn persist(&self) {
        let snapshot: HashMap<String, String> = self.map.lock().unwrap().clone();
        let doc = json!({"version": 1, "tokens": snapshot});
        let text = serde_json::to_string_pretty(&doc).unwrap_or_default();
        if let Err(e) = write_secret_file(Path::new(&self.path), text.as_bytes()) {
            tracing::warn!("context-token 落盘失败（内存态不受影响）: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_evict_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("weixin.context-tokens.json");
        let store = TokenStore::load(path.clone());
        assert!(store.is_empty());

        store.set("user-a", "tok-1");
        store.set("user-b", "tok-2");
        assert_eq!(store.get("user-a").as_deref(), Some("tok-1"));
        assert_eq!(store.len(), 2);

        store.evict("user-a");
        assert!(store.get("user-a").is_none());
        assert_eq!(store.get("user-b").as_deref(), Some("tok-2"));

        // 重载持久化结果
        let reloaded = TokenStore::load(path);
        assert_eq!(reloaded.get("user-b").as_deref(), Some("tok-2"));
        assert!(reloaded.get("user-a").is_none());
    }

    #[test]
    fn load_tolerates_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not json at all").unwrap();
        let store = TokenStore::load(path.clone());
        assert!(store.is_empty());
        // 文件不存在也一样
        let store2 = TokenStore::load(dir.path().join("missing.json"));
        assert!(store2.is_empty());
    }

    #[test]
    fn empty_skips_disk() {
        let store = TokenStore::empty("/nonexistent-dir/never-written.json");
        store.set("p", "t");
        assert_eq!(store.get("p").as_deref(), Some("t"));
    }
}
