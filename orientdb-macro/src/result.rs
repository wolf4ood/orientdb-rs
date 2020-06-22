use quote::quote;
use syn::{parse_quote, Data, DataStruct, DeriveInput, Fields, FieldsNamed, Stmt};

pub fn derive(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => {
            let ident = &input.ident;

            let reads = named.iter().filter_map(|field| -> Option<Stmt> {
                let id = &field.ident.as_ref()?;
                let id_s = id.to_string();
                let ty = &field.ty;

                Some(parse_quote!(
                    let #id: #ty = result.get_or_null(#id_s)?;
                ))
            });

            let names = named.iter().map(|field| &field.ident);

            Ok(quote! {

                impl orientdb_client::types::result::FromResult for #ident {

                    fn from_result(result : orientdb_client::types::OResult) -> orientdb_client::OrientResult<Self> where Self: Sized {

                        #(#reads)*

                        Ok(#ident {
                            #(#names),*
                        })
                    }
                }
            })
        }
        _ => Err(syn::Error::new_spanned(
            input,
            "Only structs are supported for Repo derive",
        )),
    }
}
