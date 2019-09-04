export RUST_TEST_THREADS=1
if [[ $TRAVIS_RUST_VERSION == "stable" ]]; then
      cargo test --all --tests
elif [[ $TRAVIS_RUST_VERSION == "nightly" ]]; then
      cargo test --all --all-features
fi