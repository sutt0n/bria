use bdk::blockchain::{ElectrumBlockchain, GetHeight};
use electrum_client::{Client, ConfigBuilder};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, info, instrument, warn};

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

    #[instrument(skip(self), fields(pool_size, creating_new, available_permits))]
    pub async fn acquire(&self) -> Result<PooledConnection, BdkError> {
        debug!(
            "Acquiring connection, available permits: {}",
            self.semaphore.available_permits()
        );
        let _permit = self.semaphore.acquire().await.expect("semaphore closed");
        tracing::Span::current().record("available_permits", self.semaphore.available_permits());

        let mut connections = self.connections.lock().await;
        let initial_pool_size = connections.len();
        debug!(
            "Pool state before acquire: idle={}, max={}, min_idle={}",
            initial_pool_size, self.max_connections, self.min_idle
        );

        if let Some(blockchain) = connections.pop() {
            let remaining_pool_size = connections.len();
            debug!(
                "Attempting to reuse connection from pool (pool size: {} -> {})",
                initial_pool_size, remaining_pool_size
            );
            tracing::Span::current().record("pool_size", remaining_pool_size);
            tracing::Span::current().record("creating_new", false);

            if Self::validate_connection(&blockchain).await {
                drop(_permit);
                info!(
                    "Successfully acquired connection from pool (remaining idle: {})",
                    remaining_pool_size
                );
                return Ok(PooledConnection {
                    blockchain,
                    pool: self.clone(),
                });
            } else {
                debug!("Connection validation failed, will create new connection");
            }
        } else {
            debug!("No idle connections available in pool");
        }

        drop(connections);
        drop(_permit);

        info!("Creating new Electrum connection (no valid connections in pool)");
        tracing::Span::current().record("creating_new", true);
        let start = std::time::Instant::now();
        let blockchain = self.create_connection().await?;
        let duration = start.elapsed();
        info!(
            "New connection created successfully in {:?}ms",
            duration.as_millis()
        );

        Ok(PooledConnection {
            blockchain,
            pool: self.clone(),
        })
    }

    #[instrument(skip(self), fields(electrum_url = %self.electrum_url))]
    async fn create_connection(&self) -> Result<Arc<ElectrumBlockchain>, BdkError> {
        debug!("Creating new connection to {}", self.electrum_url);
        let start = std::time::Instant::now();
        
        let client = Client::from_config(
            &self.electrum_url,
            ConfigBuilder::new().retry(10).timeout(Some(60)).build(),
        )?;
        
        let connection_time = start.elapsed();
        debug!(
            "Connection established in {:?}ms",
            connection_time.as_millis()
        );

        Ok(Arc::new(ElectrumBlockchain::from(client)))
    }

    #[instrument(skip(blockchain))]
    async fn validate_connection(blockchain: &Arc<ElectrumBlockchain>) -> bool {
        debug!("Validating connection health");
        let start = std::time::Instant::now();
        
        match blockchain.get_height() {
            Ok(height) => {
                let validation_time = start.elapsed();
                debug!(
                    "Connection validated successfully (height: {}, time: {:?}ms)",
                    height,
                    validation_time.as_millis()
                );
                true
            }
            Err(e) => {
                let validation_time = start.elapsed();
                debug!(
                    "Connection validation failed after {:?}ms: {:?}",
                    validation_time.as_millis(),
                    e
                );
                false
            }
        }
    }

    #[instrument(skip(self, blockchain))]
    async fn return_connection(&self, blockchain: Arc<ElectrumBlockchain>) {
        let mut connections = self.connections.lock().await;
        let current_pool_size = connections.len();

        if current_pool_size < self.max_connections {
            connections.push(blockchain);
            let new_pool_size = connections.len();
            info!(
                "Connection returned to pool (pool size: {} -> {}, max: {})",
                current_pool_size, new_pool_size, self.max_connections
            );
        } else {
            info!(
                "Pool is at capacity ({}/{}), dropping connection",
                current_pool_size, self.max_connections
            );
        }
        
        debug!(
            "Pool state after return: idle={}, available_permits={}",
            connections.len(),
            self.semaphore.available_permits()
        );
    }

    #[instrument(skip(self))]
    pub async fn ensure_min_idle(&self) -> Result<(), BdkError> {
        let mut connections = self.connections.lock().await;
        let current_idle = connections.len();

        if current_idle < self.min_idle {
            let to_create = self.min_idle - current_idle;
            info!(
                "Pool warmup initiated: current_idle={}, min_idle={}, creating {} connections",
                current_idle, self.min_idle, to_create
            );

            let mut created = 0;
            let mut failed = 0;
            let start = std::time::Instant::now();

            for i in 0..to_create {
                debug!("Creating warmup connection {}/{}", i + 1, to_create);
                match self.create_connection().await {
                    Ok(conn) => {
                        connections.push(conn);
                        created += 1;
                        debug!("Warmup connection {}/{} created successfully", i + 1, to_create);
                    }
                    Err(e) => {
                        failed += 1;
                        error!(
                            "Failed to create warmup connection {}/{}: {:?}",
                            i + 1,
                            to_create,
                            e
                        );
                        break;
                    }
                }
            }

            let duration = start.elapsed();
            info!(
                "Pool warmup completed in {:?}ms: created={}, failed={}, final_idle={}",
                duration.as_millis(),
                created,
                failed,
                connections.len()
            );
        } else {
            debug!(
                "Pool already has sufficient idle connections: {}/{}",
                current_idle, self.min_idle
            );
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn get_current_height(&self) -> Result<u32, BdkError> {
        debug!("Fetching current blockchain height");
        let conn = self.acquire().await?;
        let height = conn.blockchain.get_height().map_err(BdkError::from)?;
        debug!("Current blockchain height: {}", height);
        Ok(height)
    }

    #[instrument(skip(self))]
    pub async fn pool_stats(&self) -> PoolStats {
        let connections = self.connections.lock().await;
        let stats = PoolStats {
            idle_connections: connections.len(),
            max_connections: self.max_connections,
            min_idle: self.min_idle,
        };
        info!(
            "Pool statistics: idle={}/{}, min_idle={}, available_permits={}",
            stats.idle_connections,
            stats.max_connections,
            stats.min_idle,
            self.semaphore.available_permits()
        );
        stats
    }

    /// Start a background task that periodically logs pool statistics
    pub fn start_stats_logging(&self, interval_secs: u64) {
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let stats = pool.pool_stats().await;
                let health_status = if stats.idle_connections < pool.min_idle {
                    "BELOW_MIN_IDLE"
                } else if stats.idle_connections == 0 {
                    "NO_IDLE_CONNECTIONS"
                } else if stats.idle_connections == pool.max_connections {
                    "ALL_IDLE"
                } else {
                    "HEALTHY"
                };
                
                info!(
                    "Periodic pool health check: status={}, idle={}/{}, min_idle={}, permits={}",
                    health_status,
                    stats.idle_connections,
                    stats.max_connections,
                    stats.min_idle,
                    pool.semaphore.available_permits()
                );
                
                if stats.idle_connections < pool.min_idle {
                    warn!(
                        "Pool has fewer idle connections ({}) than minimum ({})",
                        stats.idle_connections, pool.min_idle
                    );
                }
            }
        });
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
        debug!("PooledConnection being dropped, returning to pool");
        let blockchain = self.blockchain.clone();
        let pool = self.pool.clone();

        tokio::spawn(async move {
            debug!("Async return of connection to pool initiated");
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
