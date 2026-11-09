// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod maybe_async;
mod orm;
mod projection;
mod reference_serialization;
mod row;

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

#[proc_macro_derive(Model, attributes(model))]
pub fn model(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let result = orm::handle(ast);
    match result {
        Ok(codegen) => codegen.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(Projection, attributes(projection))]
pub fn projection(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let result = projection::handle(ast);
    match result {
        Ok(codegen) => codegen.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Compiles one async source as the sync path, or as the async path with the crate's `async`
/// feature, in place and under the same names.
///
/// ```ignore
/// #[maybe_async]
/// impl Scan {
///     async fn count<T: StorageTransaction>(&self, txn: &T) -> Result<usize, DatabaseError> { .. }
/// }
/// ```
///
/// `#[sync_only]` / `#[async_only]` keep an impl item, trait item or nested item in one path only.
#[proc_macro_attribute]
pub fn maybe_async(attr: TokenStream, item: TokenStream) -> TokenStream {
    maybe_async::handle(attr.into(), item.into()).into()
}
