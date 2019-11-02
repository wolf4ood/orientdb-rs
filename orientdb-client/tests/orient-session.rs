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

    use async_std::task::block_on;
    use futures::StreamExt;

    #[test]
    fn test_open_session_close() {
        block_on(async {
            let session = session("test_async_open_session_close").await;
            assert!(session.session_id > 0);
            match session.token {
                Some(ref t) => assert!(t.len() > 0),
                None => assert!(false),
            }
            let result = session.close().await;
            assert!(result.is_ok());
        })
    }

    #[test]
    fn session_query_test_simple() {
        block_on(async {
            let session = session("async_session_query_test_simple").await;

            let mut results = vec![];
            let mut s = session.query("select from OUser").run().await.unwrap();

            while let Some(v) = s.next().await {
                results.push(v);
            }
        })
    }

    #[test]
    fn session_query_with_positional_params() {
        block_on(async {
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
        })
    }

    #[test]
    fn session_query_with_more_positional_params() {
        block_on(async {
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
        })
    }

    #[test]
    fn session_query_with_named_params() {
        block_on(async {
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
        })
    }

    #[test]
    fn session_query_with_more_named_params() {
        block_on(async {
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
        })
    }

    #[test]
    fn session_query_test_with_page_size() {
        block_on(async {
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
        })
    }

    #[test]
    fn session_query_test_with_retry() {
        use async_std::task;
        use orientdb_client::types::OResult;
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;

        block_on(async {
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
        })
    }

    #[test]
    fn live_query_test() {
        use async_std::task;
        use orientdb_client::types::OResult;

        block_on(async {
            let pool = sessions("live_query_test").await;

            let session = pool.get().await.unwrap();

            let (unsubscriber, mut stream) =
                session.live_query("live select from V").await.unwrap();

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
        })
    }
}
