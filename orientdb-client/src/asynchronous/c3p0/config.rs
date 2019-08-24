use std::time::Duration;

pub struct Config {
    pub max: u32,
    pub min: u32,
    pub timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max: 10,
            min: 0,
            timeout: Duration::from_secs(30),
        }
    }
}
