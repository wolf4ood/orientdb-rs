use dotenv::dotenv;
use std::env;

use orientdb_client::DatabaseType;
use orientdb_client::OSession;
use orientdb_client::OrientDB;

#[derive(Debug)]
pub struct OrientDBTest {
    pub address: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub r_username: String,
    pub r_password: String,
}

impl OrientDBTest {
    fn config() -> OrientDBTest {
        dotenv().expect("Failed to read .env file");
        let address = read_var("ORIENTDB_ADDRESS");

        let (host, port) = {
            let parsed: Vec<&str> = address.split(":").collect();
            let host = String::from(parsed[0]);
            let port = parsed[1].parse().unwrap();
            (host, port)
        };

        OrientDBTest {
            address,
            host,
            port,
            username: read_var("D_USERNAME"),
            password: read_var("D_PASSWORD"),
            r_username: read_var("R_USERNAME"),
            r_password: read_var("R_PASSWORD"),
        }
    }
}

fn read_var(var: &str) -> String {
    env::var(var).expect(&format!("Failed to read {} variable", var))
}

pub fn config() -> OrientDBTest {
    OrientDBTest::config()
}

pub fn connect() -> OrientDB {
    let config = config();
    let result = OrientDB::connect((config.host, config.port));
    assert!(result.is_ok());
    result.unwrap()
}

#[allow(dead_code)]
pub fn session(db: &str) -> OSession {
    let driver = connect();
    let config = config();

    create_database(db, &driver, &config);

    let result = driver.session(&db, &config.username, &config.password);
    assert!(result.is_ok());
    result.unwrap()
}

pub fn create_database(db: &str, odb: &OrientDB, config: &OrientDBTest) {
    let exist = odb
        .exist_database(
            db,
            &config.r_username,
            &config.r_password,
            DatabaseType::Memory,
        )
        .expect(&format!("Cannot check if database with name {} exists", db));

    if exist {
        odb.drop_database(
            db,
            &config.r_username,
            &config.r_password,
            DatabaseType::Memory,
        )
        .expect(&format!("Cannot drop database with name {}", db));
    }
    odb.create_database(
        db,
        &config.r_password,
        &config.r_password,
        DatabaseType::Memory,
    )
    .expect(&format!("Cannot create database with name {}", db));
}

#[allow(dead_code)]
pub fn run_with_session<T>(db: &str, test: T)
where
    T: Fn(&OSession),
{
    let odb = connect();
    let config = config();

    create_database(db, &odb, &config);

    ODBTest {
        test,
        db,
        odb,
        config,
    }
    .run();
}

#[allow(dead_code)]
struct ODBTest<'a, T: Fn(&OSession)> {
    test: T,
    db: &'a str,
    config: OrientDBTest,
    odb: OrientDB,
}

#[allow(unused_must_use)]
impl<'a, T: Fn(&OSession)> Drop for ODBTest<'a, T> {
    fn drop(&mut self) {
        self.odb.drop_database(
            self.db,
            &self.config.r_username,
            &self.config.r_password,
            DatabaseType::Memory,
        );
    }
}

impl<'a, T: Fn(&OSession)> ODBTest<'a, T> {
    #[allow(dead_code)]
    fn run(&self) {
        let session = self
            .odb
            .session(self.db, &self.config.username, &self.config.password)
            .expect("Cannot get a session");
        (self.test)(&session);
    }
}

#[cfg(feature = "async")]
pub mod asynchronous {
    use super::{config, OrientDBTest};
    use orientdb_client::asynchronous::{OSession, OrientDB};
    use orientdb_client::DatabaseType;

    pub async fn connect() -> OrientDB {
        let config = config();
        let result = OrientDB::connect((config.host, config.port)).await;
        assert!(result.is_ok());
        result.unwrap()
    }

    #[allow(dead_code)]
    pub async fn session(db: &str) -> OSession {
        let driver = connect().await;
        let config = config();

        create_database(db, &driver, &config).await;

        let result = driver
            .session(&db, &config.username, &config.password)
            .await;
        assert!(result.is_ok(), result.err());
        result.unwrap()
    }

    pub async fn create_database(db: &str, odb: &OrientDB, config: &OrientDBTest) {
        let exist = odb
            .exist_database(
                db,
                &config.r_username,
                &config.r_password,
                DatabaseType::Memory,
            )
            .await
            .expect(&format!("Cannot check if database with name {} exists", db));

        if exist {
            odb.drop_database(
                db,
                &config.r_username,
                &config.r_password,
                DatabaseType::Memory,
            )
            .await
            .expect(&format!("Cannot drop database with name {}", db));
        }
        odb.create_database(
            db,
            &config.r_password,
            &config.r_password,
            DatabaseType::Memory,
        )
        .await
        .expect(&format!("Cannot create database with name {}", db));
    }
}
