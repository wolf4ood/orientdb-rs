extern crate dotenv;
extern crate orientdb;

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
