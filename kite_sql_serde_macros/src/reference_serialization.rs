use darling::ast::Data;
use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{
    AngleBracketedGenericArguments, DeriveInput, Error, GenericArgument, PathArguments, Type,
    TypePath,
};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(record))]
struct SerializationOpts {
    ident: Ident,
    data: Data<SerializationVariantOpts, SerializationFieldOpt>,
}

#[derive(Debug, FromVariant)]
#[darling(attributes(record))]
struct SerializationVariantOpts {
    ident: Ident,
    fields: darling::ast::Fields<SerializationFieldOpt>,
}

#[derive(Debug, FromField)]
#[darling(attributes(record))]
struct SerializationFieldOpt {
    ident: Option<Ident>,
    ty: Type,
}

fn process_type(ty: &Type) -> TokenStream {
    if let Type::Path(TypePath { path, .. }) = ty {
        let ident = &path.segments.last().unwrap().ident;

        match ident.to_string().as_str() {
            "Vec" | "Option" | "Arc" | "Box" | "PhantomData" | "Bound" | "CountMinSketch" => {
                if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                    args, ..
                }) = &path.segments.last().unwrap().arguments
                {
                    if let Some(GenericArgument::Type(inner_ty)) = args.first() {
                        let inner_processed = process_type(inner_ty);

                        return quote! {
                            #ident::<#inner_processed>
                        };
                    }
                }
            }
            _ => {}
        }

        quote! { #ty }
    } else {
        quote! { #ty }
    }
}

pub(crate) fn handle(ast: DeriveInput) -> Result<TokenStream, Error> {
    let record_opts: SerializationOpts = SerializationOpts::from_derive_input(&ast)?;
    let struct_name = &record_opts.ident;

    Ok(match record_opts.data {
        Data::Struct(data_struct) => {
            let mut encode_fields: Vec<TokenStream> = Vec::new();
            let mut decode_fields: Vec<TokenStream> = Vec::new();
            let mut init_fields: Vec<TokenStream> = Vec::new();
            let mut is_tuple = false;

            for (i, field_opts) in data_struct.fields.into_iter().enumerate() {
                is_tuple = is_tuple || field_opts.ident.is_none();

                let field_name = field_opts
                    .ident
                    .unwrap_or_else(|| Ident::new(&format!("filed_{}", i), Span::call_site()));
                let ty = process_type(&field_opts.ty);

                encode_fields.push(quote! {
                    #field_name.encode(writer, is_direct, reference_tables)?;
                });
                decode_fields.push(quote! {
                    let #field_name = #ty::decode(reader, drive, reference_tables)?;
                });
                init_fields.push(quote! {
                    #field_name,
                })
            }
            let init_stream = if is_tuple {
                quote! { #struct_name ( #(#init_fields)* ) }
            } else {
                quote! { #struct_name { #(#init_fields)* } }
            };

            quote! {
                impl crate::serdes::ReferenceSerialization for #struct_name {
                    fn encode<W: std::io::Write>(
                        &self,
                        writer: &mut W,
                        is_direct: bool,
                        reference_tables: &mut crate::serdes::ReferenceTables,
                    ) -> Result<(), crate::errors::DatabaseError> {
                        let #init_stream = self;

                        #(#encode_fields)*

                        Ok(())
                    }

                    fn decode<T: crate::storage::Transaction, R: std::io::Read>(
                        reader: &mut R,
                        drive: Option<(&T, &crate::storage::TableCache)>,
                        reference_tables: &crate::serdes::ReferenceTables,
                    ) -> Result<Self, crate::errors::DatabaseError> {
                        #(#decode_fields)*

                        Ok(#init_stream)
                    }
                }
            }
        }
        Data::Enum(data_enum) => {
            let mut variant_encode_fields: Vec<TokenStream> = Vec::new();
            let mut variant_decode_fields: Vec<TokenStream> = Vec::new();

            for (i, variant_opts) in data_enum.into_iter().enumerate() {
                let i = i as u8;
                let mut encode_fields: Vec<TokenStream> = Vec::new();
                let mut decode_fields: Vec<TokenStream> = Vec::new();
                let mut init_fields: Vec<TokenStream> = Vec::new();
                let enum_name = variant_opts.ident;
                let mut is_tuple = false;

                for (i, field_opts) in variant_opts.fields.into_iter().enumerate() {
                    is_tuple = is_tuple || field_opts.ident.is_none();

                    let field_name = field_opts
                        .ident
                        .unwrap_or_else(|| Ident::new(&format!("filed_{}", i), Span::call_site()));
                    let ty = process_type(&field_opts.ty);

                    encode_fields.push(quote! {
                        #field_name.encode(writer, is_direct, reference_tables)?;
                    });
                    decode_fields.push(quote! {
                        let #field_name = #ty::decode(reader, drive, reference_tables)?;
                    });
                    init_fields.push(quote! {
                        #field_name,
                    })
                }

                let init_stream = if is_tuple {
                    quote! { #struct_name::#enum_name ( #(#init_fields)* ) }
                } else {
                    quote! { #struct_name::#enum_name { #(#init_fields)* } }
                };
                variant_encode_fields.push(quote! {
                    #init_stream => {
                        std::io::Write::write_all(writer, &[#i])?;

                        #(#encode_fields)*
                    }
                });
                variant_decode_fields.push(quote! {
                    #i => {
                        #(#decode_fields)*

                        #init_stream
                    }
                });
            }

            quote! {
                impl crate::serdes::ReferenceSerialization for #struct_name {
                    fn encode<W: std::io::Write>(
                        &self,
                        writer: &mut W,
                        is_direct: bool,
                        reference_tables: &mut crate::serdes::ReferenceTables,
                    ) -> Result<(), crate::errors::DatabaseError> {
                        match self {
                            #(#variant_encode_fields)*
                        }

                        Ok(())
                    }

                    fn decode<T: crate::storage::Transaction, R: std::io::Read>(
                        reader: &mut R,
                        drive: Option<(&T, &crate::storage::TableCache)>,
                        reference_tables: &crate::serdes::ReferenceTables,
                    ) -> Result<Self, crate::errors::DatabaseError> {
                        let mut type_bytes = [0u8; 1];
                        std::io::Read::read_exact(reader, &mut type_bytes)?;

                        Ok(match type_bytes[0] {
                            #(#variant_decode_fields)*
                            _ => unreachable!(),
                        })
                    }
                }
            }
        }
    })
}
