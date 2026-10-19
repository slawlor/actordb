//! Buffered in-process transport exercising the public transport contract.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use actordb::network::{
    AcceptFuture, Acceptor, BindFuture, BoxStream, ConnectFuture, Network, NetworkError,
};
use tokio::io::BufStream;
use tokio::sync::mpsc;

#[derive(Clone, Default)]
pub struct MemoryNetwork {
    listeners: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<BoxStream>>>>,
}

struct MemoryAcceptor {
    network: MemoryNetwork,
    addr: SocketAddr,
    incoming: mpsc::Receiver<BoxStream>,
}

impl Drop for MemoryAcceptor {
    fn drop(&mut self) {
        self.network.listeners.lock().unwrap().remove(&self.addr);
    }
}

impl Acceptor for MemoryAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(async move {
            self.incoming
                .recv()
                .await
                .ok_or_else(|| NetworkError("listener closed".into()))
        })
    }
}

impl Network for MemoryNetwork {
    fn bind(&self, mut addr: SocketAddr) -> BindFuture<'_> {
        Box::pin(async move {
            let mut listeners = self.listeners.lock().unwrap();
            if addr.port() == 0 {
                addr = (Ipv4Addr::LOCALHOST, 1).into();
                while listeners.contains_key(&addr) {
                    addr.set_port(addr.port() + 1);
                }
            }
            if listeners.contains_key(&addr) {
                return Err(NetworkError("address already bound".into()));
            }
            let (sender, incoming) = mpsc::channel(8);
            listeners.insert(addr, sender);
            Ok((
                addr,
                Box::new(MemoryAcceptor {
                    network: self.clone(),
                    addr,
                    incoming,
                }) as Box<dyn Acceptor>,
            ))
        })
    }

    fn connect(&self, addr: SocketAddr) -> ConnectFuture<'_> {
        Box::pin(async move {
            let incoming = self
                .listeners
                .lock()
                .unwrap()
                .get(&addr)
                .cloned()
                .ok_or_else(|| NetworkError("no listener".into()))?;
            // Small capacity forces partial reads/writes and backpressure.
            // BufStream requires explicit flushing even for small frames.
            let (local, remote) = tokio::io::duplex(64);
            incoming
                .send(Box::pin(BufStream::new(remote)))
                .await
                .map_err(|_| NetworkError("listener closed".into()))?;
            Ok(Box::pin(BufStream::new(local)) as BoxStream)
        })
    }
}
