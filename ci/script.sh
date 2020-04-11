cd orientdb-client
if [[ $TRAVIS_RUST_VERSION == "stable" ]]; then
      cargo test --all --features=async-std-runtime
      cargo test --all --features=tokio-runtime
elif [[ $TRAVIS_RUST_VERSION == "nightly" ]]; then
      cargo test --all --features=async-std-runtime
      cargo test --all --features=tokio-runtime
fi
cd ..