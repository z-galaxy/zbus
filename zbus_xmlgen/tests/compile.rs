//! Verify that the generated code in test fixtures compiles against the current
//! zbus types. Catches regressions where xmlgen emits code that no longer
//! satisfies the trait bounds expected by the `#[proxy]` macro.

// The fixtures are generated from the sample XML, which gives their names and types, such as a
// `foo` argument and deeply nested return types: lints about those would be about the inputs, not
// about xmlgen, and would fail `cargo clippy --all-targets`.
#![allow(clippy::disallowed_names, clippy::type_complexity)]

mod sample_object0 {
    use zbus::proxy;
    include!("data/sample_object0.rs");
}

mod struct_return {
    use zbus::proxy;
    include!("data/struct_return.rs");
}

mod property_setters {
    use zbus::proxy;
    include!("data/property_setters.rs");
}

mod telepathy_docstrings {
    // The generated type aliases are only referenced from the (macro-consumed) trait.
    #![allow(dead_code)]

    use zbus::proxy;
    include!("data/telepathy_docstrings.rs");
}

mod telepathy_edge_cases {
    // The generated type aliases are only referenced from the (macro-consumed) trait.
    #![allow(dead_code)]

    use zbus::proxy;
    include!("data/telepathy_edge_cases.rs");
}

mod uppercase_args {
    // Argument names are converted to snake case, so the generated code compiles without the
    // `non_snake_case` warning.
    #![deny(non_snake_case)]

    use zbus::proxy;
    include!("data/uppercase_args.rs");
}
