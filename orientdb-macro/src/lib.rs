use proc_macro::TokenStream;

mod result;

#[proc_macro_derive(FromResult)]
pub fn derive_from_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match result::derive(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
