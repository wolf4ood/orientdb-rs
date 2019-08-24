extern crate dotenv;
extern crate orientdb_client;

mod common;

use common::connect;

use orientdb_client::DatabaseType;

#[test]
fn test_client_connect_close() {
    connect();
}

#[test]
fn test_client_create_exist_drop_db() {
    let client = connect();

    let res = client.create_database("test", "root", "root", DatabaseType::Memory);

    assert!(res.is_ok());

    let res = client.exist_database("test", "root", "root", DatabaseType::Memory);

    assert!(res.is_ok());

    assert_eq!(true, res.unwrap());

    let res = client.drop_database("test", "root", "root", DatabaseType::Memory);

    assert!(res.is_ok());
}


#[cfg(feature = "async")]
mod asynchronous {
    use super::common::config;

    use async_std::task::block_on;
    use orientdb_client::asynchronous::OrientDB;
    use orientdb_client::DatabaseType;


    #[test]
    fn test_client_connect_close() {
        block_on(async {
            let cfg = config();

            let _client = OrientDB::connect(cfg.host, cfg.port).await.expect("Failed to connect");
        });
    }


    #[test]
    fn test_client_create_exist_drop_db() {
        block_on(async {
            let cfg = config();

            let client = OrientDB::connect(cfg.host, cfg.port).await.expect("Failed to connect");

            let res = client.create_database("test_async", "root", "root", DatabaseType::Memory).await;

            assert!(res.is_ok(), res.err());

            let res = client.exist_database("test_async", "root", "root", DatabaseType::Memory).await;

            assert!(res.is_ok(), res.err());

            assert_eq!(true, res.unwrap());

            let res = client.drop_database("test_async", "root", "root", DatabaseType::Memory).await;

            assert!(res.is_ok(), res.err());
        });
    }
}