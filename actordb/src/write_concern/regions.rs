// Copyright (c) Sean Lawlor
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

//! Frozen region requirement for one receiving node's eligible replica set.

use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug)]
pub(super) struct RegionRequirement {
    receiver_region: String,
    outside_nodes: HashSet<String>,
}

impl RegionRequirement {
    /// Resolve before applying the mutation. Configuration outside the eligible
    /// replica set cannot make an otherwise impossible requirement satisfiable.
    pub(super) fn resolve(
        receiver: &str,
        eligible: &HashSet<String>,
        regions: &HashMap<String, String>,
    ) -> Result<Self, String> {
        if !eligible.contains(receiver) {
            return Err("receiving node is not an eligible replica".to_string());
        }
        let receiver_region = regions
            .get(receiver)
            .ok_or_else(|| "receiving node has no configured region".to_string())?;
        let mut outside_nodes = HashSet::new();
        for peer in eligible {
            let region = regions
                .get(peer)
                .ok_or_else(|| "eligible replica has no configured region".to_string())?;
            if peer != receiver && region != receiver_region {
                outside_nodes.insert(peer.clone());
            }
        }
        if outside_nodes.is_empty() {
            return Err("eligible replicas do not include another region".to_string());
        }
        Ok(Self {
            receiver_region: receiver_region.clone(),
            outside_nodes,
        })
    }

    pub(super) fn receiver_region(&self) -> &str {
        &self.receiver_region
    }

    pub(super) fn outside_nodes(&self) -> &HashSet<String> {
        &self.outside_nodes
    }

    /// Call only with identities from validated exact-batch acknowledgements.
    /// A set makes duplicate acknowledgements irrelevant to this condition.
    pub(super) fn satisfied_by(&self, acknowledged: &BTreeSet<String>) -> bool {
        self.outside_nodes
            .iter()
            .any(|peer| acknowledged.contains(peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eligible() -> HashSet<String> {
        ["receiver", "same", "outside"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    fn regions() -> HashMap<String, String> {
        [
            ("receiver", "west"),
            ("same", "west"),
            ("outside", "east"),
            ("ineligible", "east"),
        ]
        .into_iter()
        .map(|(node, region)| (node.to_string(), region.to_string()))
        .collect()
    }

    #[test]
    fn region_requirement_rejects_missing_and_impossible_configuration() {
        for missing in ["receiver", "same", "outside"] {
            let mut regions = regions();
            regions.remove(missing);
            assert!(RegionRequirement::resolve("receiver", &eligible(), &regions).is_err());
        }
        let mut regions = regions();
        regions.insert("outside".to_string(), "west".to_string());
        assert!(RegionRequirement::resolve("receiver", &eligible(), &regions).is_err());
        assert!(RegionRequirement::resolve("ineligible", &eligible(), &regions).is_err());
        assert!(RegionRequirement::resolve(
            "receiver",
            &HashSet::from(["receiver".to_string()]),
            &regions,
        )
        .is_err());
    }

    #[test]
    fn only_eligible_outside_region_acknowledgements_satisfy_requirement() {
        let requirement = RegionRequirement::resolve("receiver", &eligible(), &regions()).unwrap();
        assert_eq!(requirement.receiver_region(), "west");
        assert_eq!(
            requirement.outside_nodes(),
            &HashSet::from(["outside".to_string()]),
        );
        let mut acknowledgements = BTreeSet::from([
            "receiver".to_string(),
            "same".to_string(),
            "ineligible".to_string(),
        ]);
        assert!(!requirement.satisfied_by(&acknowledgements));
        acknowledgements.insert("same".to_string());
        assert!(!requirement.satisfied_by(&acknowledgements));
        acknowledgements.insert("outside".to_string());
        assert!(requirement.satisfied_by(&acknowledgements));
    }

    #[test]
    fn requirement_freezes_receiver_and_eligible_regions() {
        let mut regions = regions();
        let requirement = RegionRequirement::resolve("receiver", &eligible(), &regions).unwrap();
        regions.insert("receiver".to_string(), "east".to_string());
        regions.insert("same".to_string(), "east".to_string());
        regions.insert("outside".to_string(), "west".to_string());
        assert_eq!(requirement.receiver_region(), "west");
        assert!(!requirement.satisfied_by(&BTreeSet::from(["same".to_string()])));
        assert!(requirement.satisfied_by(&BTreeSet::from(["outside".to_string()])));
    }

    #[test]
    fn requirement_uses_the_receiving_nodes_region() {
        let requirement = RegionRequirement::resolve("outside", &eligible(), &regions()).unwrap();
        assert_eq!(requirement.receiver_region(), "east");
        assert_eq!(
            requirement.outside_nodes(),
            &HashSet::from(["receiver".to_string(), "same".to_string()]),
        );
    }
}
