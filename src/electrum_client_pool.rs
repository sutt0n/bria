use bdk::blockchain::{ElectrumBlockchain, GetHeight};
use electrum_client::{Client, ConfigBuilder};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, info, instrument};

use crate::bdk::error::BdkError;

#[derive(Clone)]
pub struct ElectrumClientPool {
    electrum_url: String,
    connections: Arc<Mutex<Vec<Arc<ElectrumBlockchain>>>>,
    semaphore: Arc<Semaphore>,
    max_connections: usize,
    min_idle: usize,
}

impl ElectrumClientPool {
    pub fn new(electrum_url: String, max_connections: usize, min_idle: usize) -> Self {
        Self {
            electrum_url,
            connections: Arc::new(Mutex::new(Vec::with_capacity(max_connections))),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_connections,
            min_idle,
        }
    }

    #[instrument(skip(self), fields(pool_size, creating_new))]
    pub async fn acquire(&self) -> Result<PooledConnection, BdkError> {
        let _permit = self.semaphore.acquire().await.expect("semaphore closed");

        let mut connections = self.connections.lock().await;

        if let Some(blockchain) = connections.pop() {
            debug!("Reusing existing connection from pool");
            tracing::Span::current().record("pool_size", connections.len());
            tracing::Span::current().record("creating_new", false);

            if Self::validate_connection(&blockchain).await {
                drop(_permit);
                return Ok(PooledConnection {
                    blockchain,
                    pool: self.clone(),
                });
            } else {
                debug!("Connection validation failed, creating new connection");
            }
        }

        drop(connections);
        drop(_permit);

        debug!("Creating new Electrum connection");
        tracing::Span::current().record("creating_new", true);
        let blockchain = self.create_connection().await?;

        Ok(PooledConnection {
            blockchain,
            pool: self.clone(),
        })
    }

    async fn create_connection(&self) -> Result<Arc<ElectrumBlockchain>, BdkError> {
        let client = Client::from_config(
            &self.electrum_url,
            ConfigBuilder::new().retry(10).timeout(Some(60)).build(),
        )?;

        Ok(Arc::new(ElectrumBlockchain::from(client)))
    }

    async fn validate_connection(blockchain: &Arc<ElectrumBlockchain>) -> bool {
        match blockchain.get_height() {
            Ok(_) => true,
            Err(e) => {
                debug!("Connection validation failed: {:?}", e);
                false
            }
        }
    }

    async fn return_connection(&self, blockchain: Arc<ElectrumBlockchain>) {
        let mut connections = self.connections.lock().await;

        if connections.len() < self.max_connections {
            debug!(
                "Returning connection to pool, pool size: {}",
                connections.len() + 1
            );
            connections.push(blockchain);
        } else {
            debug!("Pool is full, dropping connection");
        }
    }

    #[instrument(skip(self))]
    pub async fn ensure_min_idle(&self) -> Result<(), BdkError> {
        let mut connections = self.connections.lock().await;
        let current_idle = connections.len();

        if current_idle < self.min_idle {
            info!(
                "Warming up pool: current idle {}, target min idle {}",
                current_idle, self.min_idle
            );

            for _ in current_idle..self.min_idle {
                match self.create_connection().await {
                    Ok(conn) => connections.push(conn),
                    Err(e) => {
                        error!("Failed to create connection during warm-up: {:?}", e);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_current_height(&self) -> Result<u32, BdkError> {
        let conn = self.acquire().await?;
        conn.blockchain.get_height().map_err(Into::into)
    }

    pub async fn pool_stats(&self) -> PoolStats {
        let connections = self.connections.lock().await;
        PoolStats {
            idle_connections: connections.len(),
            max_connections: self.max_connections,
            min_idle: self.min_idle,
        }
    }
}

pub struct PooledConnection {
    blockchain: Arc<ElectrumBlockchain>,
    pool: ElectrumClientPool,
}

impl PooledConnection {
    pub fn blockchain(&self) -> &ElectrumBlockchain {
        &self.blockchain
    }

    pub fn into_blockchain(self) -> Arc<ElectrumBlockchain> {
        self.blockchain.clone()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let blockchain = self.blockchain.clone();
        let pool = self.pool.clone();

        tokio::spawn(async move {
            pool.return_connection(blockchain).await;
        });
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = ElectrumBlockchain;

    fn deref(&self) -> &Self::Target {
        &self.blockchain
    }
}

impl std::fmt::Debug for ElectrumClientPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElectrumClientPool")
            .field("electrum_url", &self.electrum_url)
            .field("max_connections", &self.max_connections)
            .field("min_idle", &self.min_idle)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub idle_connections: usize,
    pub max_connections: usize,
    pub min_idle: usize,
}
