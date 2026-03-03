use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use super::{ChainConfig, Config, HttpConfig};

pub type SharedHttpConfig = Arc<RwLock<HttpConfig>>;

#[derive(Debug, Clone)]
pub struct NewChainEvent {
    pub chain: ChainConfig,
}

pub struct ConfigWatcher {
    config_path: PathBuf,
    http_config: SharedHttpConfig,
    chain_tx: mpsc::Sender<NewChainEvent>,
    known_chain_ids: Arc<RwLock<HashSet<u64>>>,
}

impl ConfigWatcher {
    pub fn new(
        config_path: PathBuf,
        initial_config: &Config,
        chain_tx: mpsc::Sender<NewChainEvent>,
    ) -> Self {
        let known_chain_ids: HashSet<u64> = initial_config.chains.iter().map(|c| c.chain_id).collect();

        Self {
            config_path,
            http_config: Arc::new(RwLock::new(initial_config.http.clone())),
            chain_tx,
            known_chain_ids: Arc::new(RwLock::new(known_chain_ids)),
        }
    }

    pub fn http_config(&self) -> SharedHttpConfig {
        Arc::clone(&self.http_config)
    }

    pub fn start(self) -> Result<()> {
        let config_path = self.config_path.clone();
        let http_config = self.http_config.clone();
        let chain_tx = self.chain_tx.clone();
        let known_chain_ids = self.known_chain_ids.clone();

        let (tx, mut rx) = mpsc::channel::<()>(1);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            match res {
                Ok(event) => {
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_)
                    ) {
                        let _ = tx.blocking_send(());
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Config watcher error");
                }
            }
        })?;

        let watch_path = config_path.parent().unwrap_or(&config_path);
        watcher.watch(watch_path, RecursiveMode::NonRecursive)?;

        info!(path = %config_path.display(), "Config watcher started");

        tokio::spawn(async move {
            let _watcher = watcher;
            let mut debounce = tokio::time::interval(tokio::time::Duration::from_millis(500));
            debounce.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut pending = false;

            loop {
                tokio::select! {
                    Some(()) = rx.recv() => {
                        pending = true;
                    }
                    _ = debounce.tick() => {
                        if pending {
                            pending = false;
                            if let Err(e) = reload_config(
                                &config_path,
                                &http_config,
                                &chain_tx,
                                &known_chain_ids,
                            ).await {
                                error!(error = %e, "Failed to reload config");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

async fn reload_config(
    config_path: &PathBuf,
    http_config: &SharedHttpConfig,
    chain_tx: &mpsc::Sender<NewChainEvent>,
    known_chain_ids: &Arc<RwLock<HashSet<u64>>>,
) -> Result<()> {
    let new_config = Config::load(config_path)?;

    {
        let mut http = http_config.write().await;
        *http = new_config.http.clone();
    }

    let mut known = known_chain_ids.write().await;
    for chain in &new_config.chains {
        if !known.contains(&chain.chain_id) {
            info!(
                chain = %chain.name,
                chain_id = chain.chain_id,
                "New chain detected, starting indexer"
            );
            known.insert(chain.chain_id);
            chain_tx.send(NewChainEvent { chain: chain.clone() }).await?;
        }
    }

    info!("Config reloaded");
    Ok(())
}
