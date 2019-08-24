mod common;

use common::config;

use orientdb_client::common::protocol::messages::request::Open;
use orientdb_client::sync::network::conn::Connection;

use std::error::Error;

#[test]
fn test_connection_connect_close() {
    let config = config();
    let addr = config.address.parse().unwrap();

    let res = Connection::connect(&addr);
    assert!(res.is_ok());

    let mut c = res.unwrap();
    let res = c.close();
    assert!(res.is_ok());
}

#[test]
fn test_connection_wrong_address() {
    let addr = "127.0.0.1:3333".parse().unwrap();
    let conn = Connection::connect(&addr);

    assert!(conn.is_err());
}

#[test]
fn test_connection_send_open_wrong_db() {
    let config = config();
    let addr = config.address.parse().unwrap();

    let res = Connection::connect(&addr);
    assert!(res.is_ok());

    let mut conn = res.unwrap();

    let res = conn.send(
        Open {
            db: String::from("wrong_database"),
            username: config.username,
            password: config.password,
        }
        .into(),
    );
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!("Cannot open database \'wrong_database\'", err.description());
}

#[cfg(feature = "async")]
mod asynchronous {

    use super::config;

    use async_std::task;
    use orientdb_client::asynchronous::network::Connection;
    use orientdb_client::common::protocol::messages::request::Open;
    use orientdb_client::OrientResult;
    use std::error::Error;

    #[test]
    fn test_connection_connect_close() -> OrientResult<()> {
        let config = config();
        let addr = config.address.parse().unwrap();

        task::block_on(async move {
            let res = Connection::connect(&addr).await;

            assert!(res.is_ok());

            let c = res.unwrap();
            let res = c.close();
            assert!(res.is_ok());

            Ok(())
        })
    }

    #[test]
    fn test_connection_wrong_address() -> OrientResult<()> {
        let addr = "127.0.0.1:3333".parse().unwrap();
        task::block_on(async move {
            let conn = Connection::connect(&addr).await;
            assert!(conn.is_err());
            Ok(())
        })
    }

    #[test]
    fn test_connection_send_open_wrong_db() -> OrientResult<()> {
        let config = config();
        let addr = config.address.parse().unwrap();

        task::block_on(async move {
            let res = Connection::connect(&addr).await;
            assert!(res.is_ok());

            let mut conn = res.unwrap();

            let res = conn
                .send(
                    Open {
                        db: String::from("wrong_database"),
                        username: config.username,
                        password: config.password,
                    }
                    .into(),
                )
                .await;

            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!("Cannot open database \'wrong_database\'", err.description());
            Ok(())
        })
    }
}
