use dotenv::dotenv;
use orientdb_client::sync::types::resultset::ResultSet;
use orientdb_client::DatabaseType;
use orientdb_client::OSession;
use orientdb_client::OrientDB;
use std::env;

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
        match dotenv() {
            Ok(_) => {
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
            Err(_) => OrientDBTest {
                address: String::from("127.0.0.1:2424"),
                host: String::from("127.0.0.1"),
                port: 2424,
                username: String::from("admin"),
                password: String::from("admin"),
                r_username: String::from("root"),
                r_password: String::from("root"),
            },
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
    odb.execute(
        &config.r_password,
        &config.r_password,
        &format!("create database {} memory users(admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)", db) 
    )
    .expect(&format!("Cannot create database with name {}", db)).
    run()
    .expect(&format!("Cannot create database with name {}", db))
    .close()
    .expect(&format!("Cannot create database with name {}", db))
    ;
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
    use orientdb_client::asynchronous::{OSession, OrientDB, SessionPool};
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

    #[allow(dead_code)]
    pub async fn sessions(db: &str) -> SessionPool {
        let driver = connect().await;
        let config = config();

        create_database(db, &driver, &config).await;

        let result = driver
            .sessions(&db, &config.username, &config.password, None, None)
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

        let _ = odb.execute(
        &config.r_password,
        &config.r_password,
        &format!("create database {} memory users(admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)", db) 
    ).await
    .expect(&format!("Cannot create database with name {}", db)).
    run().await
    .expect(&format!("Cannot create database with name {}", db))
    ;
    }
}
