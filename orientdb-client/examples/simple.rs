extern crate orientdb_client;

use orientdb_client::OrientDB;

#[allow(unused_must_use)]
fn main() {
    let client =
        OrientDB::connect(("localhost", 2424)).expect("Failed to connect to OrientDB instance");

    let db = "demodb";

    let session = client
        .session(db, "admin", "admin")
        .expect("Unable to open a session");

    let mut i = 0;
    for _res in session
        .query("select from V limit 200")
        .run()
        .expect("Unable to query the class Foo")
    {
        i += 1;
    }
    println!("{:?}", i);
}
