//! Various utilities shared by the `zbus` and `zbus_macros` crates. The `zgvariant` crate
//! shares them too, from its 2.0 release.
//!
//! The code generation of the derives, the `derive` and `macros` modules, needs the `codegen`
//! feature, which is on by default.

pub mod case;
#[cfg(feature = "codegen")]
pub mod derive;
#[cfg(feature = "codegen")]
pub mod macros;
pub mod names;
pub mod object_path;
pub mod signature;

#[cfg(test)]
mod test_strings;
