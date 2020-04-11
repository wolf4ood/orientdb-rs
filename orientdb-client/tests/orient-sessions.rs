extern crate dotenv;
extern crate orientdb_client;

mod common;

use common::{config, connect, create_database};

#[test]
fn test_open_sessions() {
    let client = connect();
    let config = config();

    create_database("test_open_sessions", &client, &config);

    let result = client.sessions(
        "test_open_sessions",
        &config.username,
        &config.password,
        None,
    );

    assert!(result.is_ok());

    let pool = result.unwrap();

    let session = pool.get().unwrap();

    assert_eq!(19, pool.idle());

    drop(session);

    assert_eq!(20, pool.idle());
}

#[cfg(feature = "async")]
mod asynchronous {

    use super::common::asynchronous::{connect, create_database};
    use super::config;

    use std::time::Duration;

    #[cfg(feature = "async-std-runtime")]
    use async_std::task;

    #[cfg(feature = "tokio-runtime")]
    use tokio::task;

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn test_open_sessions() {
        let client = connect().await;
        let config = config();

        create_database("async_test_open_sessions", &client, &config).await;

        let result = client
            .sessions(
                "async_test_open_sessions",
                &config.username,
                &config.password,
                None,
                Some(20),
            )
            .await;

        assert!(result.is_ok());

        let pool = result.unwrap();

        let session = pool.get().await.unwrap();

        assert_eq!(1, pool.used().await);

        drop(session);

        task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(200));
        })
        .await;

        assert_eq!(0, pool.used().await);

        assert_eq!(1, pool.idle().await);
    }
}
