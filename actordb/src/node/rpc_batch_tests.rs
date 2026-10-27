//! Cancellation keeps page/mutation ownership until actual actor completion.
use super::*;
use crate::rpc::{NodeRpcService, RpcRequest, RpcService};
use ractor::{Actor, ActorProcessingErr};
use tokio::sync::Semaphore;

struct Gate {
    actual: ActorRef<KvMessage>,
    reads: bool,
    writes: bool,
    entered: Notify,
    permits: Semaphore,
}
struct Dispatcher;
impl Actor for Dispatcher {
    type Msg = KvMessage;
    type State = Arc<Gate>;
    type Arguments = Arc<Gate>;
    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        gate: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(gate)
    }
    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        gate: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if (gate.reads && matches!(&message, KvMessage::GetBatchRetained(_)))
            || (gate.writes && matches!(&message, KvMessage::ApplyOp { .. }))
        {
            gate.entered.notify_one();
            gate.permits.acquire().await.unwrap().forget();
        }
        gate.actual
            .cast(message)
            .map_err(|error| ActorProcessingErr::from(error.to_string()))
    }
}
async fn gated(reads: bool, writes: bool) -> (Arc<Node>, Arc<Gate>, ActorRef<KvMessage>) {
    let node = Arc::new(
        Node::start_with_options(
            "rpc-batch-owned".into(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig::default(),
            NodeRuntimeOptions::default().with_query_runtime_byte_capacity(READ_RUNTIME_BYTES),
        )
        .await
        .unwrap(),
    );
    let actual = node.router.route_for_fragment(FragmentId(0)).unwrap();
    let gate = Arc::new(Gate {
        actual,
        reads,
        writes,
        entered: Notify::new(),
        permits: Semaphore::new(0),
    });
    let (actor, _) = Dispatcher::spawn(None, Dispatcher, gate.clone())
        .await
        .unwrap();
    node.router.insert(FragmentId(0), actor.clone());
    (node, gate, actor)
}
async fn cleanup(node: &Node, gate: &Gate, actor: ActorRef<KvMessage>) {
    node.router.insert(FragmentId(0), gate.actual.clone());
    actor.stop(None);
    actor.wait(None).await.unwrap();
    node.shutdown().await.unwrap();
}
async fn settled(node: &Node) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while node.query_diagnostics().reserved_runtime_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_read_keeps_its_shared_budget_and_node_gate_while_actor_owns_the_page() {
    let (node, gate, actor) = gated(true, false).await;
    let service = Arc::new(NodeRpcService::new(node.clone(), Default::default()).unwrap());
    let request = || {
        RpcRequest::new(wire::Request {
            table: "KV".into(),
            operation: Some(wire::request::Operation::BatchGet(wire::BatchGet {
                keys: vec!["held".into()],
                priority: 0,
            })),
        })
    };
    let executing = service.clone();
    let input = request();
    let call = tokio::spawn(async move { executing.call(input).await });
    gate.entered.notified().await;
    call.abort();
    let _ = call.await;
    assert_eq!(
        node.query_diagnostics().reserved_runtime_bytes,
        READ_RUNTIME_BYTES
    );
    let rejected = service.call(request()).await.unwrap();
    assert_eq!(
        rejected.reads[0].error.as_ref().unwrap().code,
        wire::ErrorCode::Backpressure as i32
    );
    drop(rejected);
    assert!(node
        .drain_requests(
            crate::node::DrainOptions::default().with_timeout(Duration::from_millis(20))
        )
        .await
        .is_err());
    gate.permits.add_permits(1);
    settled(&node).await;
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    cleanup(&node, &gate, actor).await;
}

#[tokio::test]
async fn cancelled_write_waiter_does_not_abandon_later_ordered_waves() {
    let (node, gate, actor) = gated(false, true).await;
    let service = Arc::new(NodeRpcService::new(node.clone(), Default::default()).unwrap());
    let executing = service.clone();
    let call = tokio::spawn(async move {
        executing
            .call(RpcRequest::new(wire::Request {
                table: "KV".into(),
                operation: Some(wire::request::Operation::BatchPut(wire::BatchPut {
                    puts: vec![
                        wire::Put {
                            key: "a".into(),
                            value: vec![1],
                            ..Default::default()
                        },
                        wire::Put {
                            key: "b".into(),
                            value: vec![3],
                            ..Default::default()
                        },
                        wire::Put {
                            key: "a".into(),
                            value: vec![2],
                            ..Default::default()
                        },
                    ],
                    priority: 0,
                })),
            }))
            .await
    });
    gate.entered.notified().await;
    call.abort();
    let _ = call.await;
    assert!(service.shutdown(Duration::from_millis(20)).await.is_err());
    gate.permits.add_permits(3);
    service.shutdown(Duration::from_secs(2)).await.unwrap();
    assert_eq!(node.get("a".into()).await.unwrap().unwrap().value, vec![2]);
    assert_eq!(node.get("b".into()).await.unwrap().unwrap().value, vec![3]);
    cleanup(&node, &gate, actor).await;
}
