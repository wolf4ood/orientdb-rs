cd orientdb-client
if [[ $TRAVIS_RUST_VERSION == "stable" ]]; then
      cargo test --all --features=async-std-runtime,uuid,sugar
      cargo test --all --features=tokio-runtime,uuid,sugar
elif [[ $TRAVIS_RUST_VERSION == "nightly" ]]; then
      cargo test --all --features=async-std-runtime,uuid,sugar
      cargo test --all --features=tokio-runtime,uuid,sugar
fi
cd ..