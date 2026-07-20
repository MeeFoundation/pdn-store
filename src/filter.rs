//! Session-scoped egress access control for reconciliation.
//!
//! The reconciliation engine reads entries only through the ranger store
//! trait: the crate-internal `SessionStore` wraps a replica's store with an optional per-session
//! [`EntryFilter`], so every read the engine performs — the first key, range
//! iterations, fingerprints, counts — sees only admitted entries. Filtering
//! the iterators filters the fingerprints by construction (fingerprints are
//! computed by iterating ranges), so no unadmitted entry can leak through a
//! fingerprint, a split boundary, or an item transmission. Write-side
//! methods (`entry_put`, `prefixes_of`, `remove_prefix_filtered`) pass
//! through unfiltered — ingest is the `validate_entry` hook's concern.
//!
//! The domain meaning of a filter (grants, identities) stays outside this
//! crate: an embedder hands in opaque predicates over [`SignedEntry`]
//! through a [`SessionAccessProvider`], consulted per session on both
//! session roles — accepting an incoming sync request and dialing out —
//! because both ends of a reconciliation serve entries.

use std::{future::Future, pin::Pin, sync::Arc};

use iroh::PublicKey;

use crate::{
    keys::NamespaceId,
    ranger::{Fingerprint, Range, RangeEntry, Store},
    store::PublicKeyStore,
    sync::{RecordIdentifier, SignedEntry},
};

/// Per-session egress predicate: `true` admits the entry into the peer's
/// view. Must be cheap — it runs on every entry a range scan touches.
pub type EntryFilter = Arc<dyn Fn(&SignedEntry) -> bool + Send + Sync + 'static>;

/// What one session may see of a replica, decided per (namespace, peer) at
/// session setup and frozen for the session.
#[derive(Clone)]
pub enum SessionAccess {
    /// The peer sees the replica whole.
    Full,
    /// The peer sees only the entries the filter admits.
    Filtered(EntryFilter),
    /// No session: reject as if the replica were not hosted here.
    Deny,
}

impl std::fmt::Debug for SessionAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionAccess::Full => write!(f, "Full"),
            SessionAccess::Filtered(_) => write!(f, "Filtered(..)"),
            SessionAccess::Deny => write!(f, "Deny"),
        }
    }
}

/// Which end of the session this node is on when the provider is asked.
///
/// Both ends serve entries (reconciliation is bidirectional), but the
/// policies differ: a node may refuse to *accept* a caller it cannot judge
/// while still being allowed to *dial* out with a closed egress (serving
/// nothing, receiving whatever the callee's own filter admits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRole {
    /// This node is accepting an incoming sync request.
    Accept,
    /// This node is dialing out to sync.
    Dial,
}

/// Future returned by a [`SessionAccessProvider`].
pub type SessionAccessFuture = Pin<Box<dyn Future<Output = SessionAccess> + Send + 'static>>;

/// Decides, per session, what a peer may see of a namespace. Consulted on
/// both session roles. `None` (provider unset) keeps every session
/// [`SessionAccess::Full`] — vanilla iroh-docs behaviour.
pub type SessionAccessProvider =
    Arc<dyn Fn(NamespaceId, PublicKey, SessionRole) -> SessionAccessFuture + Send + Sync + 'static>;

/// A replica store narrowed to one session's view.
///
/// With `filter: None` every method delegates unchanged. With a filter, all
/// reading methods yield only admitted entries; `get_first` returns the
/// first *admitted* key (the first physical key would leak an unadmitted
/// entry's existence through the initial range boundary), and
/// `get_fingerprint` recomputes over the filtered range iterator.
pub(crate) struct SessionStore<S> {
    inner: S,
    filter: Option<EntryFilter>,
}

impl<S> SessionStore<S> {
    pub(crate) fn new(inner: S, filter: Option<EntryFilter>) -> Self {
        Self { inner, filter }
    }
}

/// Whether `entry` passes the session's filter (no filter admits all).
fn admitted(filter: &Option<EntryFilter>, entry: &SignedEntry) -> bool {
    match filter {
        None => true,
        Some(f) => f(entry),
    }
}

impl<S: Store<SignedEntry>> Store<SignedEntry> for SessionStore<S> {
    type Error = S::Error;
    type RangeIterator<'a>
        = FilteredIter<S::RangeIterator<'a>>
    where
        S: 'a;
    type ParentIterator<'a>
        = S::ParentIterator<'a>
    where
        S: 'a;

    fn get_first(&mut self) -> Result<RecordIdentifier, Self::Error> {
        let Some(filter) = self.filter.clone() else {
            return self.inner.get_first();
        };
        // The full range (x == y wraps around); the first admitted key, or
        // the default when nothing is admitted — indistinguishable from an
        // empty replica, exactly as intended.
        let all = Range::new(RecordIdentifier::default(), RecordIdentifier::default());
        for entry in self.inner.get_range(all)? {
            let entry = entry?;
            if filter(&entry) {
                return Ok(entry.id().clone());
            }
        }
        Ok(RecordIdentifier::default())
    }

    #[cfg(test)]
    fn get(&mut self, key: &RecordIdentifier) -> Result<Option<SignedEntry>, Self::Error> {
        let found = self.inner.get(key)?;
        Ok(found.filter(|e| admitted(&self.filter, e)))
    }

    #[cfg(test)]
    fn len(&mut self) -> Result<usize, Self::Error> {
        let all = Range::new(RecordIdentifier::default(), RecordIdentifier::default());
        self.get_range_len(all)
    }

    #[cfg(test)]
    fn is_empty(&mut self) -> Result<bool, Self::Error> {
        Ok(self.len()? == 0)
    }

    fn get_fingerprint(
        &mut self,
        range: &Range<RecordIdentifier>,
    ) -> Result<Fingerprint, Self::Error> {
        if self.filter.is_none() {
            return self.inner.get_fingerprint(range);
        }
        // Recomputed over the session's own (filtered) range iterator, the
        // same way the store computes it over its full one.
        let mut fp = Fingerprint::empty();
        for entry in self.get_range(range.clone())? {
            fp ^= entry?.as_fingerprint();
        }
        Ok(fp)
    }

    fn entry_put(&mut self, entry: SignedEntry) -> Result<(), Self::Error> {
        self.inner.entry_put(entry)
    }

    fn get_range(
        &mut self,
        range: Range<RecordIdentifier>,
    ) -> Result<Self::RangeIterator<'_>, Self::Error> {
        Ok(FilteredIter {
            inner: self.inner.get_range(range)?,
            filter: self.filter.clone(),
        })
    }

    #[cfg(test)]
    fn prefixed_by(
        &mut self,
        prefix: &RecordIdentifier,
    ) -> Result<Self::RangeIterator<'_>, Self::Error> {
        Ok(FilteredIter {
            inner: self.inner.prefixed_by(prefix)?,
            filter: self.filter.clone(),
        })
    }

    fn prefixes_of(
        &mut self,
        key: &RecordIdentifier,
    ) -> Result<Self::ParentIterator<'_>, Self::Error> {
        self.inner.prefixes_of(key)
    }

    #[cfg(test)]
    fn all(&mut self) -> Result<Self::RangeIterator<'_>, Self::Error> {
        Ok(FilteredIter {
            inner: self.inner.all()?,
            filter: self.filter.clone(),
        })
    }

    #[cfg(test)]
    fn entry_remove(&mut self, key: &RecordIdentifier) -> Result<Option<SignedEntry>, Self::Error> {
        self.inner.entry_remove(key)
    }

    fn remove_prefix_filtered(
        &mut self,
        prefix: &RecordIdentifier,
        predicate: impl Fn(&crate::sync::Record) -> bool,
    ) -> Result<usize, Self::Error> {
        self.inner.remove_prefix_filtered(prefix, predicate)
    }
}

impl<S: PublicKeyStore> PublicKeyStore for SessionStore<S> {
    fn public_key(&self, id: &[u8; 32]) -> Result<PublicKey, iroh::KeyParsingError> {
        self.inner.public_key(id)
    }
}

/// A range iterator narrowed to admitted entries; errors pass through.
pub(crate) struct FilteredIter<I> {
    inner: I,
    filter: Option<EntryFilter>,
}

impl<I, E> Iterator for FilteredIter<I>
where
    I: Iterator<Item = Result<SignedEntry, E>>,
{
    type Item = Result<SignedEntry, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Err(e) => return Some(Err(e)),
                Ok(entry) => {
                    if admitted(&self.filter, &entry) {
                        return Some(Ok(entry));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_access_debug_is_opaque() {
        let access = SessionAccess::Filtered(Arc::new(|_| true));
        assert_eq!(format!("{access:?}"), "Filtered(..)");
    }
}
