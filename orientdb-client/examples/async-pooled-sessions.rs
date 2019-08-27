use orientdb_client::{DatabaseType, OrientResult};

use futures::StreamExt;
use orientdb_client::aio::OrientDB;

use async_std::task::{self, block_on, JoinHandle};

#[allow(unused_must_use)]
fn main() -> OrientResult<()> {
    block_on(async {
        let client = OrientDB::connect(("localhost", 2424)).await?;
        let db = "pooled_session";

        let n_tasks = 20;
        let exists = client
            .exist_database(db, "root", "root", DatabaseType::Memory)
            .await?;

        if exists {
            client
                .drop_database(db, "root", "root", DatabaseType::Memory)
                .await?;
        }

        client
            .create_database(db, "root", "root", DatabaseType::Memory)
            .await?;

        let pool = client
            .sessions(db, "admin", "admin", None, Some(20))
            .await?;

        let session = pool.get().await?;

        session.command("create class Foo extends V").run().await?;

        let mut tasks = Vec::new();

        for n in 0..n_tasks {
            let t_pool = pool.clone();
            let t_id = n;
            let handle: JoinHandle<OrientResult<()>> = task::spawn(async move {
                let session = t_pool.get().await?;

                for i in 0..10 {
                    session
                        .command("insert into Foo set id = ?, thread_id = ?")
                        .positional(&[&i, &t_id])
                        .run()
                        .await?;
                }
                Ok(())
            });
            tasks.push(handle);
        }
        for t in tasks {
            t.await?;
        }
        let results: Vec<_> = session
            .query("select from Foo")
            .run()
            .await?
            .collect()
            .await;

        println!(
            "Session count : {} of {}, Records created {}",
            pool.idle().await,
            pool.size().await,
            results.len()
        );

        Ok(())
    })
}
