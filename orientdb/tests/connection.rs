mod common;

use common::config;

use orientdb::network::conn::Connection;
use orientdb::protocol::messages::request::Open;

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
