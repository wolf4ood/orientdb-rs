use async_std::task::block_on;
use futures::StreamExt;
use orientdb_client::aio::OrientDB;
use orientdb_client::derive::FromResult;
use orientdb_client::OrientResult;

#[derive(FromResult, Debug)]
struct User {
    name: String,
}

fn main() -> OrientResult<()> {
    block_on(async {
        let client = OrientDB::connect(("localhost", 2424)).await?;

        let session = client.session("demodb", "admin", "admin").await?;

        // fetch one
        let user: Option<User> = session
            .query("select from OUser limit 1")
            .fetch_one()
            .await?;

        println!("User {:?}", user);

        // fetch all to vec
        let users: Vec<User> = session.query("select from OUser").fetch().await?;

        println!("Users {:?}", users);

        // fetch all to vec
        let stream = session
            .query("select from OUser")
            .stream::<User>()
            .await?
            .collect::<Vec<_>>()
            .await;

        println!("Users {:?}", stream);

        Ok(())
    })
}
