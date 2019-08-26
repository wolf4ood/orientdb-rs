if [[ $TRAVIS_RUST_VERSION == "stable" ]]; then
      cargo test --all
fi
if [[ $TRAVIS_RUST_VERSION == "nightly" ]]; then
      cargo test --all --all-features
fi