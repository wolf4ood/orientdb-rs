use async_std::task::block_on;
use futures::StreamExt;
use orientdb_client::aio::OrientDB;
use orientdb_client::OrientResult;

fn main() -> OrientResult<()> {
    block_on(async {
        let client = OrientDB::connect(("localhost", 2424)).await?;

        let session = client.session("demodb", "admin", "admin").await?;

        let mut stream = session.query("select from V limit 10").run().await?;

        while let Some(item) = stream.next().await {
            println!("Record {:?}", item?);
        }

        Ok(())
    })
}
