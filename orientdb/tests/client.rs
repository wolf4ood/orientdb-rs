extern crate dotenv;
extern crate orientdb;

mod common;

use common::connect;

use orientdb::DatabaseType;

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
