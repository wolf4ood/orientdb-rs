extern crate orientdb_client;

use orientdb_client::{DatabaseType, OrientDB};
use std::thread;

#[allow(unused_must_use)]
fn main() {
    let client =
        OrientDB::connect("localhost", 2424).expect("Failed to connect to OrientDB instance");
    let db = "pooled_session";

    let n_threads = 4;
    let exists = client
        .exist_database(db, "root", "root", DatabaseType::Memory)
        .unwrap();
    if exists {
        client
            .drop_database(db, "root", "root", DatabaseType::Memory)
            .unwrap();
    }

    client
        .create_database(db, "root", "root", DatabaseType::Memory)
        .unwrap();

    let pool = client
        .sessions(db, "admin", "admin", None)
        .expect("Unable to open pool");

    let session = pool
        .get()
        .expect("Unable to acquire a session from the pool");

    session
        .command("create class Foo extends V")
        .run()
        .expect("Unable to create class Foo");

    let mut threads = Vec::new();

    for n in 0..n_threads {
        let t_pool = pool.clone();
        let t_id = n;
        let handle = thread::spawn(move || {
            let session = t_pool
                .get()
                .expect("Unable to acquire a session from the pool");
            for i in 0..10 {
                session
                    .command("insert into Foo set id = ?, thread_id = ?")
                    .positional(&[&i, &t_id])
                    .run()
                    .expect("Unable to insert data");
            }
        });
        threads.push(handle);
    }
    for t in threads {
        t.join();
    }
    for res in session
        .query("select from Foo")
        .run()
        .expect("Unable to query the class Foo")
    {
        println!("{:?}", res);
    }
    println!("Session count : {} of {}", pool.idle(), pool.size());
}
