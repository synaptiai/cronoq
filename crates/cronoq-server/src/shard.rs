//! Which shard serves which pool, and the answer for requests that belong to another shard.

use std::collections::HashMap;
use std::sync::Arc;

use cronoq::TaskId;
use tonic::Status;
use tonic::metadata::MetadataValue;

use crate::config::ShardPlan;

/// Metadata key naming the shard that serves a refused request.
pub const SHARD_HEADER: &str = "cronoq-shard";
/// Metadata key with that shard's gRPC endpoint.
pub const ENDPOINT_HEADER: &str = "cronoq-shard-endpoint";

/// One shard, as other servers and workers reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    /// Shard name.
    pub name: Arc<str>,
    /// Number carried in its task ids.
    pub number: u8,
    /// gRPC endpoint of its leader.
    pub endpoint: Arc<str>,
}

/// The shard layout, seen from one server.
#[derive(Debug)]
pub struct ShardMap {
    own: usize,
    shards: Vec<Shard>,
    by_pool: HashMap<String, usize>,
    by_number: HashMap<u8, usize>,
    by_resource: HashMap<String, usize>,
}

impl ShardMap {
    /// Build from a validated configuration's plan.
    pub fn new(plan: &ShardPlan) -> ShardMap {
        let mut map = ShardMap {
            own: 0,
            shards: Vec::new(),
            by_pool: HashMap::new(),
            by_number: HashMap::new(),
            by_resource: HashMap::new(),
        };
        for (i, s) in plan.all.iter().enumerate() {
            if s.name == plan.own.name {
                map.own = i;
            }
            map.shards.push(Shard {
                name: s.name.as_str().into(),
                number: s.number,
                endpoint: s.endpoint.as_str().into(),
            });
            map.by_number.insert(s.number, i);
            for p in &s.pools {
                map.by_pool.insert(p.clone(), i);
            }
        }
        for (res, shard) in &plan.resources {
            if let Some(i) = map.shards.iter().position(|s| *s.name == **shard) {
                map.by_resource.insert(res.clone(), i);
            }
        }
        map
    }

    /// This server's shard.
    pub fn own(&self) -> &Shard {
        &self.shards[self.own]
    }

    /// The other shard that serves `pool`, if another one does.
    pub fn other_for_pool(&self, pool: &str) -> Option<&Shard> {
        let i = *self.by_pool.get(pool)?;
        (i != self.own).then(|| &self.shards[i])
    }

    /// The other shard that owns resource `name`, if another one does.
    pub fn other_for_resource(&self, name: &str) -> Option<&Shard> {
        let i = *self.by_resource.get(name)?;
        (i != self.own).then(|| &self.shards[i])
    }

    /// The other shard that issued `id`, if a configured other one did.
    pub fn other_for_id(&self, id: TaskId) -> Option<&Shard> {
        let i = *self.by_number.get(&id.shard())?;
        (i != self.own).then(|| &self.shards[i])
    }
}

/// `FAILED_PRECONDITION` naming the shard that serves `what`, with its name and endpoint in
/// the response metadata so that clients and workers can go there.
pub fn elsewhere(shard: &Shard, what: &str) -> Status {
    let mut s = Status::failed_precondition(format!(
        "{what} is served by shard '{}' at {}",
        shard.name, shard.endpoint
    ));
    // Names and endpoints are validated as header-safe ASCII when the configuration loads.
    if let (Ok(name), Ok(endpoint)) = (
        MetadataValue::try_from(&*shard.name),
        MetadataValue::try_from(&*shard.endpoint),
    ) {
        s.metadata_mut().insert(SHARD_HEADER, name);
        s.metadata_mut().insert(ENDPOINT_HEADER, endpoint);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShardSection;

    fn section(name: &str, number: u8, pools: &[&str]) -> ShardSection {
        ShardSection {
            name: name.into(),
            number,
            pools: pools.iter().map(|p| p.to_string()).collect(),
            endpoint: format!("https://{name}.internal:7070"),
        }
    }

    #[test]
    fn pools_and_ids_map_to_their_shard() {
        let all = vec![
            section("cpu", 1, &["default", "batch"]),
            section("gpu", 2, &["llm"]),
        ];
        let map = ShardMap::new(&ShardPlan {
            own: all[0].clone(),
            all,
            resources: vec![("api".into(), "gpu".into())],
        });
        assert_eq!(map.other_for_resource("api").map(|s| s.number), Some(2));
        assert_eq!(map.other_for_resource("other"), None);
        assert_eq!(&*map.own().name, "cpu");
        assert_eq!(map.other_for_pool("default"), None);
        assert_eq!(map.other_for_pool("unknown"), None);
        assert_eq!(map.other_for_pool("llm").map(|s| s.number), Some(2));
        assert_eq!(map.other_for_id(TaskId::first_in_shard(1, 3)), None);
        assert_eq!(
            map.other_for_id(TaskId::first_in_shard(2, 3))
                .map(|s| &*s.name),
            Some("gpu")
        );
        // Ids of shard numbers nobody has are not routed anywhere.
        assert_eq!(map.other_for_id(TaskId::first_in_shard(9, 3)), None);
        let s = elsewhere(map.other_for_pool("llm").unwrap(), "pool 'llm'");
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
        assert_eq!(s.metadata().get(SHARD_HEADER).unwrap(), "gpu");
        assert_eq!(
            s.metadata().get(ENDPOINT_HEADER).unwrap(),
            "https://gpu.internal:7070"
        );
    }
}
