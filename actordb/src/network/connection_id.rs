//! Opaque identity of one physical peer-transport binding.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Identity of one physical connection, independent of peer name and session.
/// Clones preserve identity; replacement connections always receive a new one.
/// This process-local value is not authentication evidence or a wire field.
#[derive(Clone)]
pub struct PeerConnectionId(Arc<()>);

impl PeerConnectionId {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for PeerConnectionId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for PeerConnectionId {}
impl Hash for PeerConnectionId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}
impl fmt::Debug for PeerConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerConnectionId(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_survives_cloning_and_is_not_reused_while_old_work_retains_it() {
        let original = PeerConnectionId::new();
        let retained = original.clone();
        assert_eq!(original, retained);
        drop(original);
        for _ in 0..1024 {
            assert_ne!(PeerConnectionId::new(), retained);
        }
        let mut identities = std::collections::HashSet::new();
        assert!(identities.insert(retained.clone()));
        assert!(!identities.insert(retained));
        assert!(identities.insert(PeerConnectionId::new()));
    }
}
