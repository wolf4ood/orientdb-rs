cd orientdb-client
if [[ $TRAVIS_RUST_VERSION == "stable" ]]; then
      cargo test --all --features=async-std-runtime,uuid,derive
      cargo test --all --features=tokio-runtime,uuid,derive
elif [[ $TRAVIS_RUST_VERSION == "nightly" ]]; then
      cargo test --all --features=async-std-runtime,uuid,derive
      cargo test --all --features=tokio-runtime,uuid,derive
fi
cd ..