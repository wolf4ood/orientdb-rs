use super::config::Config;
use super::{C3p0Result, ConnectionManger, Pool};
use std::marker::PhantomData;
use std::time::Duration;

pub struct PoolBuilder<M> {
    _p: PhantomData<M>,
    cfg: Config,
}

impl<M: ConnectionManger> PoolBuilder<M> {
    pub async fn build(self, manager: M) -> C3p0Result<Pool<M>> {
        let pool = Pool::new(manager, self.cfg);
        pool.establish_idle_connections().await?;
        Ok(pool)
    }

    pub fn max_size(mut self, max_size: u32) -> Self {
        self.cfg.max = max_size;
        self
    }
    pub fn min_size(mut self, min_size: u32) -> Self {
        self.cfg.min = min_size;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.cfg.timeout = timeout;
        self
    }
}

impl<M: ConnectionManger> Default for PoolBuilder<M> {
    fn default() -> Self {
        PoolBuilder {
            _p: PhantomData,
            cfg: Config::default(),
        }
    }
}
