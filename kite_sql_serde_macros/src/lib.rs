mod reference_serialization;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ReferenceSerialization, attributes(reference_serialization))]
pub fn reference_serialization(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let result = reference_serialization::handle(ast);
    match result {
        Ok(codegen) => codegen.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
