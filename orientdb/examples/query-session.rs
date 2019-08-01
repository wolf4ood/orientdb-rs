extern crate orientdb;

use orientdb::{DatabaseType, OrientDB};

#[derive(Debug)]
struct Person {
    name: String,
    surname: String,
}

#[allow(unused_must_use)]
fn main() {
    let client =
        OrientDB::connect("localhost", 2424).expect("Failed to connect to OrientDB instance");
    let db = "simple-session";

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

    let session = client
        .session(db, "admin", "admin")
        .expect("Unable to open a session");

    session
        .command("create class Person extends V")
        .run()
        .unwrap();

    session
        .command("create property Person.name string")
        .run()
        .unwrap();
    session
        .command("create property Person.surname string")
        .run()
        .unwrap();

    let person = Person {
        name: String::from("John"),
        surname: String::from("Reese"),
    };

    session
        .command("insert into Person set name = ? , surname = ?")
        .positional(&[&person.name, &person.surname])
        .run()
        .unwrap();

    for res in session
        .query("select from Person")
        .run()
        .expect("Unable to query the class Foo")
    {
        let r = res.unwrap();
        let person = Person {
            name: r.get("name"),
            surname: r.get("surname"),
        };
        println!("{:?}", person);
    }

    let results = session
        .query("select from Person")
        .run()
        .expect("Unable to query the class Foo")
        .map(|e| {
            e.map(|i| Person {
                name: i.get("name"),
                surname: i.get("surname"),
            })
        })
        .collect::<Result<Vec<Person>, _>>();

    println!("{:?}", results);

    session.close();
}
