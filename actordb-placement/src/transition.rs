use crate::{engine::validate_map, FragmentMap, PlannerError};
use std::collections::{BTreeMap, BTreeSet};

/// Bounded controller-owned overlap sequencing. Readiness must come from all
/// affected nodes' current Serving reports, collected by an authoritative
/// controller. A local callback alone is insufficient evidence.
///
/// This object neither allocates hosts nor establishes application writer
/// authority. Persist it with your control-plane state and serialize publishers
/// when running multiple controller processes.
#[derive(Debug, Clone)]
pub struct PlacementTransition {
    current: FragmentMap,
    target: FragmentMap,
    pending: BTreeSet<u32>,
    max_in_progress: usize,
}

impl PlacementTransition {
    pub fn new(
        current: FragmentMap,
        target: FragmentMap,
        max_in_progress: usize,
    ) -> Result<Self, PlannerError> {
        let count =
            u32::try_from(current.len()).map_err(|_| PlannerError("too many fragments".into()))?;
        validate_map(&current, count)?;
        validate_map(&target, count)?;
        if max_in_progress == 0 {
            return Err(PlannerError(
                "transition concurrency must be nonzero".into(),
            ));
        }
        let mut transition = Self {
            current,
            target,
            pending: BTreeSet::new(),
            max_in_progress,
        };
        transition.schedule();
        Ok(transition)
    }

    fn schedule(&mut self) {
        for (id, target) in &self.target {
            if self.pending.len() >= self.max_in_progress {
                break;
            }
            if self.pending.contains(id) || same(&self.current[id], target) {
                continue;
            }
            let current = self.current.get_mut(id).expect("validated map");
            for peer in target {
                if !current.contains(peer) {
                    current.push(peer.clone());
                }
            }
            self.pending.insert(*id);
        }
    }

    /// Publish this map through ManualPlacement or your PlacementController.
    pub fn placement(&self) -> &FragmentMap {
        &self.current
    }

    /// Advance only the replica sets whose complete target is currently ready.
    /// New overlaps scheduled by this call wait for a subsequent observation.
    /// Missing readiness keeps all old replicas in the published overlap.
    pub fn advance(&mut self, serving: &BTreeMap<u32, BTreeSet<String>>) -> &FragmentMap {
        let completed: Vec<u32> = self
            .pending
            .iter()
            .copied()
            .filter(|id| {
                serving
                    .get(id)
                    .is_some_and(|ready| self.target[id].iter().all(|peer| ready.contains(peer)))
            })
            .collect();
        for id in completed {
            self.current.insert(id, self.target[&id].clone());
            self.pending.remove(&id);
        }
        self.schedule();
        &self.current
    }

    pub fn is_complete(&self) -> bool {
        self.pending.is_empty()
            && self
                .current
                .iter()
                .all(|(id, replicas)| same(replicas, &self.target[id]))
    }
}

fn same(a: &[String], b: &[String]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}
