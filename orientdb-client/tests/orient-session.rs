use orientdb_client::sync::types::resultset::ResultSet;

use orientdb_client::types::value::{EmbeddedMap, OValue};
use orientdb_client::types::ODocument;
use orientdb_client::OSession;

mod common;

use common::{run_with_session, session};

#[test]
fn test_open_session_close() {
    let session = session("test_open_session_close");
    assert!(session.session_id > 0);
    match session.token {
        Some(ref t) => assert!(t.len() > 0),
        None => assert!(false),
    }
    let result = session.close();
    assert!(result.is_ok());
}

#[test]
fn session_query_test_simple() {
    run_with_session("session_query_test_simple", |session| {
        let result: Vec<_> = session.query("select from OUser").run().unwrap().collect();
        assert_eq!(3, result.len());
    });
}

#[test]
fn session_query_with_positional_params() {
    run_with_session("session_query_with_positional_params", |session| {
        let result: Vec<_> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .run()
            .unwrap()
            .collect();
        assert_eq!(1, result.len());
    });
}

#[test]
fn session_query_with_more_positional_params() {
    run_with_session("session_query_with_more_positional_params", |session| {
        let result: Vec<_> = session
            .query("select from OUser where name = ? and void = ?")
            .positional(&[&"admin", &1])
            .run()
            .unwrap()
            .collect();

        assert_eq!(0, result.len());
    });
}

#[test]
fn session_query_with_named_params() {
    run_with_session("session_query_with_named_params", |session| {
        let result: Vec<_> = session
            .query("select from OUser where name = :name")
            .named(&[("name", &"admin")])
            .run()
            .unwrap()
            .collect();

        assert_eq!(1, result.len());
    });
}

#[test]
fn session_query_with_more_named_params() {
    run_with_session("session_query_with_more_named_params", |session| {
        let result: Vec<_> = session
            .query("select from OUser where name = :name and void =:void")
            .named(&[("name", &"admin"), ("void", &1)])
            .run()
            .unwrap()
            .collect();
        assert_eq!(0, result.len());
    });
}

#[test]
fn session_query_test_with_page_size() {
    run_with_session("session_query_test_with_page_size", |session| {
        let result: Vec<_> = session
            .query("select from OUser")
            .page_size(1)
            .run()
            .unwrap()
            .collect();

        assert_eq!(3, result.len());
    });
}

#[test]
fn session_query_test_with_page_size_and_close() {
    run_with_session("session_query_test_with_page_size_and_close", |session| {
        let mut iter = session
            .query("select from OUser")
            .page_size(1)
            .run()
            .unwrap();

        let first = match iter.next() {
            Some(Ok(elem)) => elem,
            Some(Err(_)) => panic!("Received error on next"),
            None => panic!("Nothing returned from ResultSet#next"),
        };

        assert_eq!(
            Some(&OValue::String(String::from("admin"))),
            first.get_raw("name")
        );
        assert_eq!(String::from("admin"), first.get::<String>("name"));
        assert_eq!(String::from("ACTIVE"), first.get::<String>("status"));

        let result = iter.close();

        assert!(result.is_ok());
    });
}

#[cfg(feature = "sugar")]
#[test]
fn session_query_one() {
    run_with_session("session_query_one", |session| {
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }

        let result: Option<Person> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .fetch_one()
            .unwrap();

        assert_eq!(
            Some(Person {
                name: String::from("admin"),
            }),
            result,
        )
    });
}

#[cfg(feature = "sugar")]
#[test]
fn session_query_one_result() {
    use orientdb_client::types::OResult;

    run_with_session("session_query_one_result", |session| {
        let result: Option<OResult> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .fetch_one()
            .unwrap();

        assert_eq!("admin", result.unwrap().get::<String>("name"))
    });
}

#[cfg(feature = "sugar")]
#[test]
fn session_query_all() {
    run_with_session("session_query_all", |session| {
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }

        let results: Vec<Person> = session.query("select from OUser").fetch().unwrap();

        assert_eq!(3, results.len(),)
    });
}

#[cfg(feature = "sugar")]
#[test]
fn session_query_iter() {
    run_with_session("session_query_iter", |session| {
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }

        let results: Result<Vec<Person>, _> =
            session.query("select from OUser").iter().unwrap().collect();

        assert_eq!(3, results.unwrap().len())
    });
}

#[cfg(feature = "uuid")]
#[test]
fn session_query_with_uuid() {
    use uuid::Uuid;

    run_with_session("session_query_with_uuid", |session| {
        crate_schema(&session);

        let uuid = Uuid::new_v4();
        session
            .command("create vertex Person set gid = ?")
            .positional(&[&uuid])
            .run()
            .unwrap();

        let mut iter = session
            .query("select from Person")
            .page_size(1)
            .run()
            .unwrap();

        let first = match iter.next() {
            Some(Ok(elem)) => elem,
            Some(Err(_)) => panic!("Received error on next"),
            None => panic!("Nothing returned from ResultSet#next"),
        };

        assert_eq!(uuid, first.get::<Uuid>("gid"));

        let result = iter.close();

        assert!(result.is_ok());
    });
}

#[test]
fn session_query_with_projection() {
    run_with_session("session_query_with_projection", |session| {
        let result = session
            .query("select name from OUser where name = :name")
            .named(&[("name", &"writer")])
            .run()
            .unwrap()
            .take(1)
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(String::from("writer"), result.get::<String>("name"));
    });
}

#[test]
fn session_query_with_complex_document() {
    run_with_session("session_query_with_complex_document", |session| {
        crate_schema(&session);

        session
            .command("create vertex Person content  { name : 'Jonh', address : { street : 'test street'}}")
            .run()
            .unwrap();

        let result = session
            .query("select from Person where name = :name")
            .named(&[("name", &"Jonh")])
            .run()
            .unwrap()
            .take(1)
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(String::from("Jonh"), result.get::<String>("name"));
        assert_eq!(
            ODocument::builder()
                .set_class_name("Address")
                .set("street", "test street")
                .build(),
            result.get::<ODocument>("address")
        );
    });
}

#[test]
fn session_query_projected_with_complex_document() {
    run_with_session("session_query_projected_with_complex_document", |session| {
        crate_schema(&session);

        let script = r#"
            let v1 = create vertex Person content  { name : 'Foo', address : { street : 'test street'}};
            let v2 = create vertex Person content  { name : 'Jonh', address : { street : 'test street'}};
            let e  = create edge HasFriend from $v1 to $v2;
        "#;
        session.script_sql(script).run().unwrap();

        let result = session
            .query("select name,address from Person where name = :name")
            .named(&[("name", &"Jonh")])
            .run()
            .unwrap()
            .take(1)
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(String::from("Jonh"), result.get::<String>("name"));
        let mut map = EmbeddedMap::new();
        map.insert(
            String::from("street"),
            OValue::String(String::from("test street")),
        );
        map.insert(
            String::from("@class"),
            OValue::String(String::from("Address")),
        );
        assert_eq!(map, result.get::<EmbeddedMap>("address"));
    });
}

fn crate_schema(session: &OSession) {
    session
        .command("create class Person extends V")
        .run()
        .unwrap();

    session
        .command("create class HasFriend extends E")
        .run()
        .unwrap();

    session
        .command("create class Address abstract")
        .run()
        .unwrap();

    session
        .command("create property Person.address EMBEDDED Address")
        .run()
        .unwrap();
}

#[cfg(feature = "async")]
mod asynchronous {
    use super::common::asynchronous::{session, sessions};

    use futures::StreamExt;

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn test_open_session_close() {
        let session = session("test_async_open_session_close").await;
        assert!(session.session_id > 0);
        match session.token {
            Some(ref t) => assert!(t.len() > 0),
            None => assert!(false),
        }
        let result = session.close().await;
        assert!(result.is_ok());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
    async fn session_query_test_simple() {
        let session = session("async_session_query_test_simple").await;

        let mut results = vec![];
        let mut s = session.query("select from OUser").run().await.unwrap();

        while let Some(v) = s.next().await {
            results.push(v);
        }
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_query_with_positional_params() {
        let session = session("async_session_query_with_positional_params").await;
        let result: Vec<_> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(1, result.len());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_query_with_more_positional_params() {
        let session = session("async_session_query_with_more_positional_params").await;
        let result: Vec<_> = session
            .query("select from OUser where name = ? and void = ?")
            .positional(&[&"admin", &1])
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(0, result.len());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_query_with_named_params() {
        let session = session("async_session_query_with_named_params").await;
        let result: Vec<_> = session
            .query("select from OUser where name = :name")
            .named(&[("name", &"admin")])
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(1, result.len());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_query_with_more_named_params() {
        let session = session("async_session_query_with_more_named_params").await;
        let result: Vec<_> = session
            .query("select from OUser where name = :name and void =:void")
            .named(&[("name", &"admin"), ("void", &1)])
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(0, result.len());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_query_test_with_page_size() {
        let session = session("async_session_query_test_with_page_size").await;
        let result: Vec<_> = session
            .query("select from OUser")
            .page_size(1)
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(3, result.len());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[allow(unused_must_use)]
    async fn session_query_test_with_retry() {
        #[cfg(feature = "async-std-runtime")]
        use async_std::task;

        #[cfg(feature = "tokio-runtime")]
        use tokio::task;

        use orientdb_client::types::OResult;
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;

        let pool = sessions("async_session_query_test_with_retry").await;

        let counter = Arc::new(AtomicI64::new(0));
        let session = pool.get().await.unwrap();

        let _result: Vec<Result<OResult, _>> = session
            .command("insert into V set id = 1")
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        let _result: Vec<Result<OResult, _>> = session
            .command("insert into V set id = 2")
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        drop(session);

        let handles : Vec<_> =(0..10).map(|_| {
                let cloned = pool.clone();
                let new_counter = counter.clone();
                task::spawn( async move {
                    let s = cloned.get().await.unwrap();
                    let inner_resut = s.with_retry(10,|s| async move {
                        s.command("create edge from (select from v where id = '1') to (select from v where id = '2')").run().await
                    }).await;

                    match inner_resut {
                        Ok(_e) => {
                            new_counter.fetch_add(1,Ordering::SeqCst);
                        },
                        _=> {}
                    };
                })
            }).collect();

        for t in handles {
            t.await;
        }

        assert_eq!(10, counter.load(Ordering::SeqCst));
    }

    #[cfg(feature = "uuid")]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]

    async fn session_query_with_uuid_async() {
        use uuid::Uuid;

        let session = session("session_query_with_uuid_async").await;

        let uuid = Uuid::new_v4();
        let _result = session
            .command("create vertex V set gid = ?")
            .positional(&[&uuid])
            .run()
            .await
            .unwrap()
            .next()
            .await;

        let item = session
            .query("select from V")
            .page_size(1)
            .run()
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(uuid, item.get::<Uuid>("gid"));
    }
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[allow(unused_must_use)]
    async fn session_query_test_with_retry_transaction() {
        #[cfg(feature = "async-std-runtime")]
        use async_std::task;

        #[cfg(feature = "tokio-runtime")]
        use tokio::task;

        use orientdb_client::types::OResult;
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;

        let pool = sessions("async_session_query_test_with_retry_transaction").await;

        let counter = Arc::new(AtomicI64::new(0));
        let session = pool.get().await.unwrap();

        let _result: Vec<Result<OResult, _>> = session
            .command("insert into V set id = 1")
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        let _result: Vec<Result<OResult, _>> = session
            .command("insert into V set id = 2")
            .run()
            .await
            .unwrap()
            .collect()
            .await;

        drop(session);

        let handles : Vec<_> =(0..10).map(|_| {
                let cloned = pool.clone();
                let new_counter = counter.clone();
                task::spawn( async move {
                    let s = cloned.get().await.unwrap();
                    let inner_resut = s.transaction(10,|s| async move {
                        s.command("create edge from (select from v where id = '1') to (select from v where id = '2')").run().await
                    }).await;

                    match inner_resut {
                        Ok(_e) => {
                            new_counter.fetch_add(1,Ordering::SeqCst);
                        },
                        _=> {}
                    };
                })
            }).collect();

        for t in handles {
            t.await;
        }

        assert_eq!(10, counter.load(Ordering::SeqCst));
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn live_query_test() {
        #[cfg(feature = "async-std-runtime")]
        use async_std::task;

        #[cfg(feature = "tokio-runtime")]
        use tokio::task;

        use orientdb_client::types::OResult;

        let pool = sessions("live_query_test").await;

        let session = pool.get().await.unwrap();

        let (unsubscriber, mut stream) = session
            .live_query("live select from V")
            .run()
            .await
            .unwrap();

        let inner_session = pool.get().await.unwrap();
        task::spawn(async move {
            let _result: Vec<Result<OResult, _>> = inner_session
                .command("insert into v set id = 1")
                .run()
                .await
                .unwrap()
                .collect()
                .await;

            let _result: Vec<Result<OResult, _>> = inner_session
                .command("update v set id = 2 where id = 1")
                .run()
                .await
                .unwrap()
                .collect()
                .await;

            let _result: Vec<Result<OResult, _>> = inner_session
                .command("delete vertex from V where id = 2")
                .run()
                .await
                .unwrap()
                .collect()
                .await;

            unsubscriber.unsubscribe().await.unwrap();
        });

        let mut counter = 0;
        while let Some(_item) = stream.next().await {
            counter += 1;
        }
        assert_eq!(3, counter);
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg(feature = "sugar")]
    async fn session_query_one() {
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }
        let session = session("async_session_query_one").await;
        let result: Option<Person> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .fetch_one()
            .await
            .unwrap();

        assert_eq!(
            Some(Person {
                name: String::from("admin"),
            }),
            result,
        )
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg(feature = "sugar")]
    async fn session_query_one_result() {
        use orientdb_client::types::OResult;

        let session = session("async_session_query_one_result").await;
        let result: Option<OResult> = session
            .query("select from OUser where name = ?")
            .positional(&[&"admin"])
            .fetch_one()
            .await
            .unwrap();

        assert_eq!("admin", result.unwrap().get::<String>("name"))
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg(feature = "sugar")]
    async fn session_query_all() {
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }
        let session = session("async_session_query_all").await;
        let result: Vec<Person> = session.query("select from OUser").fetch().await.unwrap();

        assert_eq!(3, result.len(),)
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg(feature = "sugar")]
    async fn session_query_stream() {
        use futures::StreamExt;
        #[derive(orientdb_client::derive::FromResult, Debug, PartialEq)]
        struct Person {
            name: String,
        }
        let session = session("async_session_query_stream").await;
        let results = session
            .query("select from OUser")
            .stream::<Person>()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(3, results.len(),)
    }
}
