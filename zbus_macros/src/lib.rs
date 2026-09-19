#![deny(rust_2018_idioms)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/z-galaxy/zbus/9f7a90d2b594ddc48b7a5f39fda5e00cd56a7dfb/logo.png"
)]
#![doc = include_str!("../README.md")]
#![doc(test(attr(
    warn(unused),
    deny(warnings),
    allow(dead_code),
    // W/o this, we seem to get some bogus warning about `extern crate zbus`.
    allow(unused_extern_crates),
)))]

use proc_macro::TokenStream;
use syn::DeriveInput;
#[cfg(feature = "service")]
use syn::ItemImpl;
#[cfg(feature = "proxy")]
use syn::ItemTrait;
#[cfg(feature = "comms")]
use syn::parse_macro_input;
#[cfg(any(feature = "proxy", feature = "service"))]
use syn::{Meta, Token, punctuated::Punctuated};

#[cfg(feature = "comms")]
mod error;
#[cfg(feature = "service")]
mod iface;
#[cfg(feature = "proxy")]
mod proxy;
mod utils;

/// Attribute macro for defining D-Bus proxies (using [`zbus::Proxy`] and
/// [`zbus::blocking::Proxy`]).
///
/// The macro must be applied on a `trait T`. Two matching `impl T` will provide an asynchronous
/// Proxy implementation, named `TraitNameProxy` and a blocking one, named `TraitNameProxyBlocking`.
/// The proxy instances can be created with the associated `new()` or `builder()` methods. The
/// former doesn't take any argument and uses the default service name and path. The later allows
/// you to specify non-default proxy arguments.
///
/// The following attributes are supported:
///
/// * `interface` - the name of the D-Bus interface this proxy is for.
///
/// * `default_service` - the default service this proxy should connect to.
///
/// * `default_path` - The default object path the method calls will be sent on and signals will be
///   sent for by the target service.
///
/// * `gen_async` - Whether or not to generate the asynchronous Proxy type.
///
/// * `gen_blocking` - Whether or not to generate the blocking Proxy type. If the `blocking-api`
///   cargo feature of `zbus` is disabled, this attribute is ignored and blocking Proxy type is not
///   generated.
///
/// * `async_name` - Specify the exact name of the asynchronous proxy type.
///
/// * `blocking_name` - Specify the exact name of the blocking proxy type.
///
/// * `assume_defaults` - whether to auto-generate values for `default_path` and `default_service`
///   if none are specified (default: `false`). `proxy` generates a warning if neither this
///   attribute nor one of the default values are specified. Please make sure to explicitly set
///   either this attribute or the default values, according to your needs.
///
/// * `crate` - specify the path to the `zbus` crate if it's renamed or re-exported.
///
/// Each trait method will be expanded to call to the associated D-Bus remote interface.
///
/// Trait methods accept `proxy` attributes:
///
/// * `name` - override the D-Bus name (pascal case form by default)
///
/// * `property` - expose the method as a property. If the method takes an argument, it must be a
///   setter, with a `set_` prefix. Otherwise, it's a getter. Additional sub-attributes exists to
///   control specific property behaviors:
///   * `emits_changed_signal` - specifies how property changes are signaled. Valid values are those
///     documented in [DBus specifications][dbus_emits_changed_signal]:
///     * `"true"` - (default) change signal is always emitted with the value included. This uses
///       the default caching behavior of the proxy, and generates a listener method for the change
///       signal.
///     * `"invalidates"` - change signal is emitted, but the value is not included in the signal.
///       This has the same behavior as `"true"`.
///     * `"const"` - property never changes, thus no signal is ever emitted for it. This uses the
///       default caching behavior of the proxy, but does not generate a listener method for the
///       change signal.
///     * `"false"` - change signal is not (guaranteed to be) emitted if the property changes. This
///       disables property value caching, and does not generate a listener method for the change
///       signal.
///
/// * `signal` - declare a signal just like a D-Bus method. Read the [Signals](#signals) section
///   below for details.
///
/// * `no_reply` - declare a method call that does not wait for a reply.
///
/// * `no_autostart` - declare a method call that will not trigger the bus to automatically launch
///   the destination service if it is not already running.
///
/// * `allow_interactive_auth` - declare a method call that is allowed to trigger an interactive
///   prompt for authorization or confirmation from the receiver.
///
/// * `object` - methods or properties that return an [`ObjectPath`] can be annotated with the
///   `object` attribute to specify the proxy object to be constructed from the returned
///   [`ObjectPath`].
///
/// * `async_object` - if the assumptions made by `object` attribute about naming of the
///   asynchronous proxy type, don't fit your bill, you can use this to specify its exact name.
///
/// * `blocking_object` - if the assumptions made by `object` attribute about naming of the blocking
///   proxy type, don't fit your bill, you can use this to specify its exact name.
///
/// * `object_vec` - this method returns a list of [`ObjectPath`]s (DBus signature `ao`) that should
///   be converted to the proxy object type named by `object`, `async_object` and `blocking_object`
///   attributes, and returned as a `Vec<_>`.
///
///   NB: Any doc comments provided shall be appended to the ones added by the macro.
///
/// # Signals
///
/// For each signal method declared, this macro will provide a method, named `receive_<method_name>`
/// to create a [`zbus::SignalStream`] ([`zbus::blocking::SignalIterator`] for the blocking proxy)
/// wrapper, named `<SignalName>Stream` (`<SignalName>Iterator` for the blocking proxy) that yield
/// a [`zbus::message::Message`] wrapper, named `<SignalName>`. This wrapper provides type safe
/// access to the signal arguments. It also implements `Deref<Target = Message>` to allow easy
/// access to the underlying [`zbus::message::Message`].
///
/// For each property with `emits_changed_signal` set to `"true"` (default) or `"invalidates"`,
/// this macro will provide a method named `receive_<property_name>_changed` that creates a
/// [`zbus::proxy::PropertyStream`] for the property.
///
/// # Example
///
/// ```no_run
/// # use std::error::Error;
/// use zbus::proxy;
/// use zbus::{blocking::Connection, Result, fdo, Value};
/// use futures_util::stream::StreamExt;
/// use zbus::block_on;
///
/// #[proxy(
///     interface = "org.test.SomeIface",
///     default_service = "org.test.SomeService",
///     default_path = "/org/test/SomeObject"
/// )]
/// trait SomeIface {
///     fn do_this(&self, with: &str, some: u32, arg: &Value<'_>) -> Result<bool>;
///
///     #[zbus(property)]
///     fn a_property(&self) -> fdo::Result<String>;
///
///     #[zbus(property)]
///     fn set_a_property(&self, a_property: &str) -> fdo::Result<()>;
///
///     #[zbus(signal)]
///     fn some_signal(&self, arg1: &str, arg2: u32) -> fdo::Result<()>;
///
///     #[zbus(object = "SomeOtherIface", blocking_object = "SomeOtherInterfaceBlock")]
///     // The method will return a `SomeOtherIfaceProxy` or `SomeOtherIfaceProxyBlock`, depending
///     // on whether it is called on `SomeIfaceProxy` or `SomeIfaceProxyBlocking`, respectively.
///     //
///     // NB: We explicitly specified the exact name of the blocking proxy type. If we hadn't,
///     // `SomeOtherIfaceProxyBlock` would have been assumed and expected. We could also specify
///     // the specific name of the asynchronous proxy types, using the `async_object` attribute.
///     fn some_method(&self, arg1: &str);
///
///     #[zbus(property, object = "SomeOtherIface", blocking_object = "SomeOtherInterfaceBlock")]
///     // Properties that return an ObjectPath can also use the `object` attribute.
///     fn related_object(&self);
/// }
///
/// #[proxy(
///     interface = "org.test.SomeOtherIface",
///     default_service = "org.test.SomeOtherService",
///     blocking_name = "SomeOtherInterfaceBlock",
/// )]
/// trait SomeOtherIface {}
///
/// let connection = Connection::session()?;
/// // Use `builder` to override the default arguments, `new` otherwise.
/// let proxy = SomeIfaceProxyBlocking::builder(&connection)
///                .destination("org.another.Service")
///                .cache_properties(zbus::proxy::CacheProperties::No)
///                .build()?;
/// let _ = proxy.do_this("foo", 32, &Value::new(true));
/// let _ = proxy.set_a_property("val");
///
/// let signal = proxy.receive_some_signal()?.next().unwrap();
/// let args = signal.args()?;
/// println!("arg1: {}, arg2: {}", args.arg1(), args.arg2());
///
/// // Now the same again, but asynchronous.
/// block_on(async move {
///     let proxy = SomeIfaceProxy::builder(&connection.into())
///                    .cache_properties(zbus::proxy::CacheProperties::No)
///                    .build()
///                    .await
///                    .unwrap();
///     let _ = proxy.do_this("foo", 32, &Value::new(true)).await;
///     let _ = proxy.set_a_property("val").await;
///
///     let signal = proxy.receive_some_signal().await?.next().await.unwrap();
///     let args = signal.args()?;
///     println!("arg1: {}, arg2: {}", args.arg1(), args.arg2());
///
///     Ok::<(), zbus::Error>(())
/// })?;
///
/// # Ok::<_, Box<dyn Error + Send + Sync>>(())
/// ```
///
/// [`zbus_polkit`] is a good example of how to bind a real D-Bus API.
///
/// [`zbus_polkit`]: https://docs.rs/zbus_polkit/1.0.0/zbus_polkit/policykit1/index.html
/// [`zbus::Proxy`]: https://docs.rs/zbus/latest/zbus/proxy/struct.Proxy.html
/// [`zbus::message::Message`]: https://docs.rs/zbus/latest/zbus/message/struct.Message.html
/// [`zbus::proxy::PropertyStream`]: https://docs.rs/zbus/latest/zbus/proxy/struct.PropertyStream.html
/// [`zbus::blocking::Proxy`]: https://docs.rs/zbus/latest/zbus/blocking/proxy/struct.Proxy.html
/// [`zbus::SignalStream`]: https://docs.rs/zbus/latest/zbus/proxy/struct.SignalStream.html
/// [`zbus::blocking::SignalIterator`]: https://docs.rs/zbus/latest/zbus/blocking/proxy/struct.SignalIterator.html
/// [`ObjectPath`]: https://docs.rs/zbus/latest/zbus/struct.ObjectPath.html
/// [dbus_emits_changed_signal]: https://dbus.freedesktop.org/doc/dbus-specification.html#introspection-format
#[cfg(feature = "proxy")]
#[proc_macro_attribute]
pub fn proxy(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated<Meta, Token![,]>::parse_terminated);
    let input = parse_macro_input!(item as ItemTrait);
    proxy::expand(args, input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Attribute macro for implementing a D-Bus interface.
///
/// The macro must be applied on an `impl T`. All methods will be exported, either as methods,
/// properties or signal depending on the item attributes. It will implement the [`Interface`] trait
/// `for T` on your behalf, to handle the message dispatching and introspection support.
///
/// The trait accepts the `interface` attributes:
///
/// * `name` - the D-Bus interface name
///
/// * `spawn` - Controls the spawning of tasks for method calls. By default, `true`, allowing zbus
///   to spawn a separate task for each method call. This default behavior can lead to methods being
///   handled out of their received order, which might not always align with expected or desired
///   behavior.
///
///   - **When True (Default):** Suitable for interfaces where method calls are independent of each
///     other or can be processed asynchronously without strict ordering. In scenarios where a
///     client must wait for a reply before making further dependent calls, this default behavior is
///     appropriate.
///
///   - **When False:** Use this setting to ensure methods are handled in the order they are
///     received, which is crucial for interfaces requiring sequential processing of method calls.
///     However, care must be taken to avoid making D-Bus method calls from within your interface
///     methods when this setting is false, as it may lead to deadlocks under certain conditions.
///
/// * `proxy` - If specified, a proxy type will also be generated for the interface. This attribute
#[cfg_attr(
    feature = "proxy",
    doc = "  supports all the [`macro@proxy`]-specific sub-attributes (e.g `gen_async`). The common",
    doc = "  sub-attributes (e.g `name`) are automatically forwarded to the [`macro@proxy`] macro."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "  supports all the `proxy` macro-specific sub-attributes (e.g `gen_async`). The common",
    doc = "  sub-attributes (e.g `name`) are automatically forwarded to the `proxy` macro."
)]
///   If the `proxy` cargo feature of `zbus` is disabled, the attribute is accepted but no proxy
///   type is generated.
///
/// * `introspection_docs` - whether to include the documentation in the introspection data
///   (Default: `true`). If your interface is well-known or well-documented, you may want to set
///   this to `false` to reduce the the size of your binary and D-Bus traffic.
///
/// * `crate` - specify the path to the `zbus` crate if it's renamed or re-exported.
///
/// The methods accepts the `interface` attributes:
///
/// * `name` - override the D-Bus name (pascal case form of the method by default)
///
/// * `property` - expose the method as a property. If the method takes an argument, it must be a
///   setter, with a `set_` prefix. Otherwise, it's a getter. If it may fail, a property method must
///   return `zbus::fdo::Result`. An additional sub-attribute exists to control the emission of
///   signals on changes to the property:
///   * `emits_changed_signal` - specifies how property changes are signaled. Valid values are those
///     documented in [DBus specifications][dbus_emits_changed_signal]:
///     * `"true"` - (default) the change signal is always emitted when the property's setter is
///       called. The value of the property is included in the signal.
///     * `"invalidates"` - the change signal is emitted, but the value is not included in the
///       signal.
///     * `"const"` - the property never changes, thus no signal is ever emitted for it.
///     * `"false"` - the change signal is not emitted if the property changes. If a property is
///       write-only, the change signal will not be emitted in this interface.
///
/// * `signal` - the method is a "signal". It must be a method declaration (without body). Its code
///   block will be expanded to emit the signal from the object path associated with the interface
///   instance. Moreover, `interface` will also generate a trait named `<Interface>Signals` that
///   provides all the signal methods but without the `SignalEmitter` argument. The macro implements
///   this trait for two types, `zbus::object_server::InterfaceRef<Interface>` and
///   `SignalEmitter<'_>`. The former is useful for emitting signals from outside the context of an
///   interface method and the latter is useful for emitting signals from inside interface methods.
///
///   You can call a signal method from a an interface method, or from an [`ObjectServer::with`]
///   function.
///
/// * `out_args` - When returning multiple values from a method, naming the out arguments become
///   important. You can use `out_args` to specify their names.
#[cfg_attr(
    feature = "proxy",
    doc = "* `proxy` - Use this to specify the [`macro@proxy`]-specific method sub-attributes (e.g",
    doc = "  `object`). The common sub-attributes (e.g `name`) are automatically forworded to the",
    doc = "  [`macro@proxy`] macro. Moreover, you can use `visibility` sub-attribute to specify the",
    doc = "  visibility of the generated proxy type(s)."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "* `proxy` - Use this to specify the `proxy` macro-specific method sub-attributes (e.g",
    doc = "  `object`). The common sub-attributes (e.g `name`) are automatically forworded to the",
    doc = "  `proxy` macro. Moreover, you can use `visibility` sub-attribute to specify the",
    doc = "  visibility of the generated proxy type(s)."
)]
///
///   In such case, your method must return a tuple containing
///   your out arguments, in the same order as passed to `out_args`.
///
/// The `struct_return` attribute (from zbus 1.x) is no longer supported. If you want to return a
/// single structure from a method, declare it to return a tuple containing either a named structure
/// or a nested tuple.
///
/// Note: a `<property_name_in_snake_case>_changed` method is generated for each property: this
/// method emits the "PropertiesChanged" signal for the associated property. The setter (if it
/// exists) will automatically call this method. For instance, a property setter named `set_foo`
/// will be called to set the property "Foo", and will emit the "PropertiesChanged" signal with the
/// new value for "Foo". Other changes to the "Foo" property can be signaled manually with the
/// generated `foo_changed` method. In addition, a `<property_name_in_snake_case>_invalidated`
/// method is also generated that much like `_changed` method, emits a "PropertyChanged" signal
/// but does not send over the new value of the property along with it. It is usually best to avoid
/// using this since it will force all interested peers to fetch the new value and hence result in
/// excess traffic on the bus.
///
/// The method arguments support the following `zbus` attributes:
///
/// * `object_server` - This marks the method argument to receive a reference to the
///   [`ObjectServer`] this method was called by.
/// * `connection` - This marks the method argument to receive a reference to the [`Connection`] on
///   which the method call was received.
/// * `header` - This marks the method argument to receive the message header associated with the
///   D-Bus method call being handled. For property methods, this will be an `Option<Header<'_>>`,
///   which will be set to `None` if the method is called for reasons other than to respond to an
///   external property access.
/// * `signal_emitter` - This marks the method argument to receive a [`SignalEmitter`] instance,
///   which is needed for emitting signals the easy way.
///
/// # Example
///
/// ```
/// # use std::error::Error;
/// use zbus::interface;
/// use zbus::{ObjectServer, object_server::SignalEmitter, message::Header};
///
/// struct Example {
///     _some_data: String,
/// }
///
/// #[interface(name = "org.myservice.Example")]
/// impl Example {
///     // "Quit" method. A method may throw errors.
///     async fn quit(
///         &self,
///         #[zbus(header)]
///         hdr: Header<'_>,
///         #[zbus(signal_emitter)]
///         emitter: SignalEmitter<'_>,
///         #[zbus(object_server)]
///         _server: &ObjectServer,
///     ) -> zbus::fdo::Result<()> {
///         let path = hdr.path().unwrap();
///         let msg = format!("You are leaving me on the {} path?", path);
///         emitter.bye(&msg).await?;
///
///         // Do some asynchronous tasks before quitting..
///
///         Ok(())
///     }
///
///     // "TheAnswer" property (note: the "name" attribute), with its associated getter.
///     // A `the_answer_changed` method has also been generated to emit the
///     // "PropertiesChanged" signal for this property.
///     #[zbus(property, name = "TheAnswer")]
///     fn answer(&self) -> u32 {
///         2 * 3 * 7
///     }
///
///     // "IFail" property with its associated getter.
///     // An `i_fail_changed` method has also been generated to emit the
///     // "PropertiesChanged" signal for this property.
///     #[zbus(property)]
///     fn i_fail(&self) -> zbus::fdo::Result<i32> {
///         Err(zbus::fdo::Error::UnknownProperty("IFail".into()))
///     }
///
///     // "Bye" signal (note: no implementation body).
///     #[zbus(signal)]
///     async fn bye(signal_emitter: &SignalEmitter<'_>, message: &str) -> zbus::Result<()>;
///
///     #[zbus(out_args("answer", "question"))]
///     fn meaning_of_life(&self) -> zbus::fdo::Result<(i32, String)> {
///         Ok((42, String::from("Meaning of life")))
///     }
/// }
///
/// # Ok::<_, Box<dyn Error + Send + Sync>>(())
/// ```
///
/// See also [`ObjectServer`] documentation to learn how to export an interface over a `Connection`.
///
/// [`ObjectServer`]: https://docs.rs/zbus/latest/zbus/object_server/struct.ObjectServer.html
/// [`ObjectServer::with`]: https://docs.rs/zbus/latest/zbus/object_server/struct.ObjectServer.html#method.with
/// [`Connection`]: https://docs.rs/zbus/latest/zbus/connection/struct.Connection.html
/// [`Connection::emit_signal()`]: https://docs.rs/zbus/latest/zbus/connection/struct.Connection.html#method.emit_signal
/// [`SignalEmitter`]: https://docs.rs/zbus/latest/zbus/object_server/struct.SignalEmitter.html
/// [`Interface`]: https://docs.rs/zbus/latest/zbus/object_server/trait.Interface.html
/// [dbus_emits_changed_signal]: https://dbus.freedesktop.org/doc/dbus-specification.html#introspection-format
#[cfg(feature = "service")]
#[proc_macro_attribute]
pub fn interface(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated<Meta, Token![,]>::parse_terminated);
    let input = parse_macro_input!(item as ItemImpl);
    iface::expand(args, input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Derive macro for implementing [`zbus::DBusError`] trait.
///
/// This macro makes it easy to implement the [`zbus::DBusError`] trait for your custom error type
/// (currently only enums are supported).
///
/// If a special variant marked with the `zbus` attribute is present, `From<zbus::Error>` is
/// also implemented for your type. This variant can only have a single unnamed field of type
/// [`zbus::Error`]. This implementation makes it possible for you to declare proxy methods to
/// directly return this type, rather than [`zbus::Error`].
///
/// Each variant (except for the special `zbus` one) can optionally have a (named or unnamed)
/// `String` field (which is used as the human-readable error description).
///
/// The following type-level attributes are supported:
///
/// * `prefix` - the D-Bus error name prefix.
/// * `crate` - specify the path to the `zbus` crate if it's renamed or re-exported.
///
/// # Example
///
/// ```
/// use zbus::DBusError;
///
/// #[derive(DBusError, Debug)]
/// #[zbus(prefix = "org.myservice.App")]
/// enum Error {
///     #[zbus(error)]
///     ZBus(zbus::Error),
///     FileNotFound(String),
///     OutOfMemory,
/// }
/// ```
///
/// [`zbus::DBusError`]: https://docs.rs/zbus/latest/zbus/trait.DBusError.html
/// [`zbus::Error`]: https://docs.rs/zbus/latest/zbus/enum.Error.html
/// [`serde::Serialize`]: https://docs.rs/serde/1.0.132/serde/trait.Serialize.html
#[cfg(feature = "comms")]
#[proc_macro_derive(DBusError, attributes(zbus))]
pub fn derive_dbus_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    error::expand_derive(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Derive macro to add [`Type`] implementation to structs and enums.
///
/// # Examples
///
/// For structs it works just like serde's [`Serialize`] and [`Deserialize`] macros:
///
/// ```
/// use zbus::{Type, wire::{serialized::Context, to_bytes, LE}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
/// struct Struct<'s> {
///     field1: u16,
///     field2: i64,
///     field3: &'s str,
/// }
///
/// assert_eq!(Struct::SIGNATURE, "(qxs)");
/// let s = Struct {
///     field1: 42,
///     field2: i64::max_value(),
///     field3: "hello",
/// };
/// let ctxt = Context::new(LE, 0);
/// let encoded = to_bytes(ctxt, &s).unwrap();
/// let decoded: Struct = encoded.deserialize().unwrap().0;
/// assert_eq!(decoded, s);
/// ```
///
/// Same with enum, except that all variants of the enum must have the same number and types of
/// fields (if any). If you want the encoding size of the (unit-type) enum to be dictated by
/// `repr` attribute (like in the example below), you'll also need [serde_repr] crate.
///
/// ```
/// use zbus::{Type, wire::{serialized::Context, to_bytes, LE}};
/// use serde::{Deserialize, Serialize};
/// use serde_repr::{Deserialize_repr, Serialize_repr};
///
/// #[repr(u8)]
/// #[derive(Deserialize_repr, Serialize_repr, Type, Debug, PartialEq)]
/// enum Enum {
///     Variant1,
///     Variant2,
/// }
/// assert_eq!(Enum::SIGNATURE, u8::SIGNATURE);
/// let ctxt = Context::new(LE, 0);
/// let encoded = to_bytes(ctxt, &Enum::Variant2).unwrap();
/// let decoded: Enum = encoded.deserialize().unwrap().0;
/// assert_eq!(decoded, Enum::Variant2);
///
/// #[repr(i64)]
/// #[derive(Deserialize_repr, Serialize_repr, Type)]
/// enum Enum2 {
///     Variant1,
///     Variant2,
/// }
/// assert_eq!(Enum2::SIGNATURE, i64::SIGNATURE);
///
/// // w/o repr attribute, u32 representation is chosen
/// #[derive(Deserialize, Serialize, Type)]
/// enum NoReprEnum {
///     Variant1,
///     Variant2,
/// }
/// assert_eq!(NoReprEnum::SIGNATURE, u32::SIGNATURE);
///
/// // Not-unit enums are represented as a structure, with the first field being a u32 denoting the
/// // variant and the second as the actual value.
/// #[derive(Deserialize, Serialize, Type)]
/// enum NewType {
///     Variant1(f64),
///     Variant2(f64),
/// }
/// assert_eq!(NewType::SIGNATURE, "(ud)");
///
/// #[derive(Deserialize, Serialize, Type)]
/// enum StructFields {
///     Variant1(u16, i64, &'static str),
///     Variant2 { field1: u16, field2: i64, field3: &'static str },
/// }
/// assert_eq!(StructFields::SIGNATURE, "(u(qxs))");
/// ```
///
/// # Custom signatures
///
/// There are times when you'd find yourself wanting to specify a hardcoded signature yourself for
/// the type. The `signature` attribute exists for this purpose. A typical use case is when you'd
/// need to encode your type as a dictionary (signature `a{sv}`) type. For convenience, `dict` is
/// an alias for `a{sv}`. Here is an example:
///
/// ```
/// use zbus::{
///     as_value, Type, wire::{serialized::Context, to_bytes, LE},
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
/// // `#[zvariant(signature = "a{sv}")]` would be the same.
/// #[zvariant(signature = "dict")]
/// struct Struct {
///     #[serde(with = "as_value")]
///     field1: u16,
///     #[serde(with = "as_value")]
///     field2: i64,
///     #[serde(with = "as_value")]
///     field3: String,
/// }
///
/// assert_eq!(Struct::SIGNATURE, "a{sv}");
/// let s = Struct {
///     field1: 42,
///     field2: i64::max_value(),
///     field3: "hello".to_string(),
/// };
/// let ctxt = Context::new(LE, 0);
/// let encoded = to_bytes(ctxt, &s).unwrap();
/// let decoded: Struct = encoded.deserialize().unwrap().0;
/// assert_eq!(decoded, s);
/// ```
///
/// Another common use for custom signatures is (de)serialization of unit enums as strings:
///
/// ```
/// use zbus::{Type, wire::{serialized::Context, to_bytes, LE}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
/// #[zvariant(signature = "s")]
/// enum StrEnum {
///     Variant1,
///     Variant2,
///     Variant3,
/// }
///
/// assert_eq!(StrEnum::SIGNATURE, "s");
/// let ctxt = Context::new(LE, 0);
/// let encoded = to_bytes(ctxt, &StrEnum::Variant2).unwrap();
/// assert_eq!(encoded.len(), 13);
/// let decoded: StrEnum = encoded.deserialize().unwrap().0;
/// assert_eq!(decoded, StrEnum::Variant2);
/// ```
///
/// # Custom crate path
///
/// If you've renamed `zbus` in your `Cargo.toml` or are using it through a re-export, point
/// the derive at the module holding the wire types with the `crate` attribute:
///
/// ```
/// use zbus::Type;
///
/// #[derive(Type)]
/// #[zbus(crate = "zbus::wire")]
/// struct MyStruct {
///     field: String,
/// }
/// ```
///
/// [`Type`]: https://docs.rs/zbus/latest/zbus/trait.Type.html
/// [`Serialize`]: https://docs.serde.rs/serde/trait.Serialize.html
/// [`Deserialize`]: https://docs.serde.rs/serde/de/trait.Deserialize.html
/// [serde_repr]: https://crates.io/crates/serde_repr
#[proc_macro_derive(Type, attributes(zbus, zvariant))]
pub fn type_macro_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    zbus_utils::derive::expand_type_derive(ast, &utils::derive_config())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Adds [`Serialize`] implementation to structs to be serialized as a D-Bus dictionary type.
///
/// The dictionary type is determined by the `signature` attribute. The default is `a{sv}`
/// (string keys, variant values), but nested forms like `a{sa{sv}}` and `a{oa{sv}}` are also
/// supported — fields whose value type is itself a dict (or any non-`Variant` type) are
/// serialized directly through their own `Serialize` impl rather than wrapped as a variant.
///
/// Such dictionary types are very commonly used with
/// [D-Bus](https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-properties)
/// and GVariant.
///
/// # Alternative Approaches
///
/// There are two approaches to serializing structs as dictionaries:
///
/// 1. Using this macro (simpler, but less control).
/// 2. Using the `Serialize` derive with `zbus::as_value` (more verbose, but more control).
///
/// See the example below and the relevant [FAQ entry] in our book for more details on the
/// alternative approach.
///
/// # Example
///
/// ## Approach #1
///
/// ```
/// use zbus::{SerializeDict, Type};
///
/// #[derive(Debug, Default, SerializeDict, Type)]
/// #[zvariant(signature = "a{sv}", rename_all = "PascalCase")]
/// pub struct MyStruct {
///     field1: Option<u32>,
///     field2: String,
/// }
/// ```
///
/// ## Approach #2
///
/// ```
/// use serde::Serialize;
/// use zbus::{Type, as_value};
///
/// #[derive(Debug, Default, Serialize, Type)]
/// #[zvariant(signature = "a{sv}")]
/// #[serde(default, rename_all = "PascalCase")]
/// pub struct MyStruct {
///     #[serde(with = "as_value::optional", skip_serializing_if = "Option::is_none")]
///     field1: Option<u32>,
///     #[serde(with = "as_value")]
///     field2: String,
/// }
/// ```
///
/// ## Nested dictionaries
///
/// To represent shapes like `a{sa{sv}}` (the body type of
/// `org.freedesktop.DBus.ObjectManager.GetManagedObjects` and similar APIs), nest one
/// `SerializeDict`/`DeserializeDict` struct inside another:
///
/// ```
/// use zbus::{DeserializeDict, SerializeDict, Type};
///
/// #[derive(SerializeDict, DeserializeDict, Type, Default)]
/// #[zvariant(signature = "a{sv}", rename_all = "PascalCase")]
/// pub struct AdapterProperties {
///     address: Option<String>,
///     name: Option<String>,
/// }
///
/// #[derive(SerializeDict, DeserializeDict, Type, Default)]
/// #[zvariant(signature = "a{sa{sv}}")]
/// pub struct InterfaceProperties {
///     #[zvariant(rename = "org.bluez.Adapter1")]
///     adapter: Option<AdapterProperties>,
/// }
/// ```
///
/// # Custom crate path
///
/// If you've renamed `zbus` in your `Cargo.toml` or are using it through a re-export, point
/// the derive at the module holding the wire types with the `crate` attribute:
///
/// ```
/// use zbus::{SerializeDict, Type};
///
/// #[derive(SerializeDict, Type)]
/// #[zbus(signature = "a{sv}", crate = "zbus::wire")]
/// struct MyStruct {
///     field: String,
/// }
/// ```
///
/// [`Serialize`]: https://docs.serde.rs/serde/trait.Serialize.html
/// [FAQ entry]: https://z-galaxy.github.io/zbus/faq.html#how-to-use-a-struct-as-a-dictionary
#[proc_macro_derive(SerializeDict, attributes(zbus, zvariant))]
pub fn serialize_dict_macro_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    zbus_utils::derive::expand_serialize_dict_derive(input, &utils::derive_config())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Adds [`Deserialize`] implementation to structs to be deserialized from a D-Bus dictionary type.
///
/// The dictionary type is determined by the `signature` attribute. The default is `a{sv}`
/// (string keys, variant values), but nested forms like `a{sa{sv}}` and `a{oa{sv}}` are also
/// supported — fields whose value type is itself a dict (or any non-`Variant` type) are
/// deserialized directly through their own `Deserialize` impl rather than unwrapped from a
/// variant. See [`SerializeDict`] for a nested example.
///
/// Such dictionary types are very commonly used with
/// [D-Bus](https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-properties)
/// and GVariant.
///
/// # Alternative Approaches
///
/// There are two approaches to deserializing dictionaries as structs:
///
/// 1. Using this macro (simpler, but less control).
/// 2. Using the `Deserialize` derive with `zbus::as_value` (more verbose, but more control).
///
/// See the example below and the relevant [FAQ entry] in our book for more details on the
/// alternative approach.
///
/// # Example
///
/// ## Approach #1
///
/// ```
/// use zbus::{DeserializeDict, Type};
///
/// #[derive(Debug, Default, DeserializeDict, Type)]
/// #[zvariant(signature = "a{sv}", rename_all = "PascalCase")]
/// pub struct MyStruct {
///     field1: Option<u32>,
///     field2: String,
/// }
/// ```
///
/// ## Approach #2
///
/// ```
/// use serde::Deserialize;
/// use zbus::{Type, as_value};
///
/// #[derive(Debug, Default, Deserialize, Type)]
/// #[zvariant(signature = "a{sv}")]
/// #[serde(default, rename_all = "PascalCase")]
/// pub struct MyStruct {
///     #[serde(with = "as_value::optional")]
///     field1: Option<u32>,
///     #[serde(with = "as_value")]
///     field2: String,
/// }
/// ```
///
/// # Custom crate path
///
/// If you've renamed `zbus` in your `Cargo.toml` or are using it through a re-export, point
/// the derive at the module holding the wire types with the `crate` attribute:
///
/// ```
/// use zbus::{DeserializeDict, Type};
///
/// #[derive(DeserializeDict, Type)]
/// #[zbus(signature = "a{sv}", crate = "zbus::wire")]
/// struct MyStruct {
///     field: String,
/// }
/// ```
///
/// [`Deserialize`]: https://docs.serde.rs/serde/de/trait.Deserialize.html
/// [FAQ entry]: https://z-galaxy.github.io/zbus/faq.html#how-to-use-a-struct-as-a-dictionary
#[proc_macro_derive(DeserializeDict, attributes(zbus, zvariant))]
pub fn deserialize_dict_macro_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    zbus_utils::derive::expand_deserialize_dict_derive(input, &utils::derive_config())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Implements conversions for your type to/from [`Value`].
///
/// Implements `TryFrom<Value>` and `Into<Value>` for your type.
///
/// # Examples
///
/// Simple owned strutures:
///
/// ```
/// use zbus::{OwnedObjectPath, OwnedValue, Value};
///
/// #[derive(Clone, Value, OwnedValue)]
/// struct OwnedStruct {
///     owned_str: String,
///     owned_path: OwnedObjectPath,
/// }
///
/// let s = OwnedStruct {
///     owned_str: String::from("hi"),
///     owned_path: OwnedObjectPath::try_from("/blah").unwrap(),
/// };
/// let value = Value::from(s.clone());
/// let _ = OwnedStruct::try_from(value).unwrap();
/// let value = OwnedValue::try_from(s).unwrap();
/// let s = OwnedStruct::try_from(value).unwrap();
/// assert_eq!(s.owned_str, "hi");
/// assert_eq!(s.owned_path.as_str(), "/blah");
/// ```
///
/// Now for the more exciting case of unowned structures:
///
/// ```
/// use zbus::{ObjectPath, Str};
/// # use zbus::{OwnedValue, Value};
/// #
/// #[derive(Clone, Value, OwnedValue)]
/// struct UnownedStruct<'a> {
///     s: Str<'a>,
///     path: ObjectPath<'a>,
/// }
///
/// let hi = String::from("hi");
/// let s = UnownedStruct {
///     s: Str::from(&hi),
///     path: ObjectPath::try_from("/blah").unwrap(),
/// };
/// let value = Value::from(s.clone());
/// let s = UnownedStruct::try_from(value).unwrap();
///
/// let value = OwnedValue::try_from(s).unwrap();
/// let s = UnownedStruct::try_from(value).unwrap();
/// assert_eq!(s.s, "hi");
/// assert_eq!(s.path, "/blah");
/// ```
///
/// Generic structures also supported:
///
/// ```
/// # use zbus::{OwnedObjectPath, OwnedValue, Value};
/// #
/// #[derive(Clone, Value, OwnedValue)]
/// struct GenericStruct<S, O> {
///     field1: S,
///     field2: O,
/// }
///
/// let s = GenericStruct {
///     field1: String::from("hi"),
///     field2: OwnedObjectPath::try_from("/blah").unwrap(),
/// };
/// let value = Value::from(s.clone());
/// let _ = GenericStruct::<String, OwnedObjectPath>::try_from(value).unwrap();
/// let value = OwnedValue::try_from(s).unwrap();
/// let s = GenericStruct::<String, OwnedObjectPath>::try_from(value).unwrap();
/// assert_eq!(s.field1, "hi");
/// assert_eq!(s.field2.as_str(), "/blah");
/// ```
///
/// Enums also supported but currently only with unit variants:
///
/// ```
/// # use zbus::{OwnedValue, Value};
/// #
/// #[derive(Debug, PartialEq, Value, OwnedValue)]
/// // Default representation is `u32`.
/// #[repr(u8)]
/// enum Enum {
///     Variant1 = 0,
///     Variant2,
/// }
///
/// let value = Value::from(Enum::Variant1);
/// let e = Enum::try_from(value).unwrap();
/// assert_eq!(e, Enum::Variant1);
/// assert_eq!(e as u8, 0);
/// let value = OwnedValue::try_from(Enum::Variant2).unwrap();
/// let e = Enum::try_from(value).unwrap();
/// assert_eq!(e, Enum::Variant2);
/// ```
///
/// String-encoded enums are also supported:
///
/// ```
/// # use zbus::{OwnedValue, Value};
/// #
/// #[derive(Debug, PartialEq, Value, OwnedValue)]
/// #[zvariant(signature = "s")]
/// enum StrEnum {
///     Variant1,
///     Variant2,
/// }
///
/// let value = Value::from(StrEnum::Variant1);
/// let e = StrEnum::try_from(value).unwrap();
/// assert_eq!(e, StrEnum::Variant1);
/// let value = OwnedValue::try_from(StrEnum::Variant2).unwrap();
/// let e = StrEnum::try_from(value).unwrap();
/// assert_eq!(e, StrEnum::Variant2);
/// ```
///
/// # Renaming fields
///
/// ## Auto Renaming
///
/// The macro supports specifying a Serde-like `#[zvariant(rename_all = "case")]` attribute on
/// structures. The attribute allows to rename all the fields from snake case to another case
/// automatically.
///
/// Currently the macro supports the following values for `case`:
///
/// * `"lowercase"`
/// * `"UPPERCASE"`
/// * `"PascalCase"`
/// * `"camelCase"`
/// * `"snake_case"`
/// * `"kebab-case"`
///
/// ## Individual Fields
///
/// It's still possible to specify custom names for individual fields using the
/// `#[zvariant(rename = "another-name")]` attribute even when the `rename_all` attribute is
/// present.
///
/// Here is an example using both `rename` and `rename_all`:
///
/// ```
/// # use zbus::{OwnedValue, Value, Dict};
/// # use std::collections::HashMap;
/// #
/// #[derive(Clone, Value, OwnedValue)]
/// #[zvariant(signature = "dict", rename_all = "PascalCase")]
/// struct RenamedStruct {
///     #[zvariant(rename = "MyValue")]
///     field1: String,
///     field2: String,
/// }
///
/// let s = RenamedStruct {
///     field1: String::from("hello"),
///     field2: String::from("world")
/// };
/// let v = Value::from(s);
/// let d = Dict::try_from(v).unwrap();
/// let hm: HashMap<String, String> = HashMap::try_from(d).unwrap();
/// assert_eq!(hm.get("MyValue").unwrap().as_str(), "hello");
/// assert_eq!(hm.get("Field2").unwrap().as_str(), "world");
/// ```
///
/// # Dictionary encoding
///
/// For treating your type as a dictionary, you can use the `signature = "dict"` attribute. See
/// [`Type`] for more details and an example use. Please note that this macro can only handle
/// `dict` or `a{sv}` values. All other values will be ignored.
///
/// # Custom crate path
///
/// If you've renamed `zbus` in your `Cargo.toml` or are using it through a re-export, point
/// the derive at the module holding the wire types with the `crate` attribute:
///
/// ```
/// use zbus::Value;
///
/// #[derive(Clone, Value)]
/// #[zbus(crate = "zbus::wire")]
/// struct MyStruct {
///     field: String,
/// }
/// ```
///
/// [`Value`]: https://docs.rs/zbus/latest/zbus/enum.Value.html
/// [`Type`]: crate::Type#custom-signatures
#[proc_macro_derive(Value, attributes(zbus, zvariant))]
pub fn value_macro_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    zbus_utils::derive::expand_value_derive(
        ast,
        zbus_utils::derive::ValueType::Value,
        &utils::derive_config(),
    )
    .unwrap_or_else(|err| err.to_compile_error())
    .into()
}

/// Implements conversions for your type to/from [`OwnedValue`].
///
/// Implements `TryFrom<OwnedValue>` and `TryInto<OwnedValue>` for your type.
///
/// See [`Value`] documentation for examples.
///
/// [`OwnedValue`]: https://docs.rs/zbus/latest/zbus/struct.OwnedValue.html
#[proc_macro_derive(OwnedValue, attributes(zbus, zvariant))]
pub fn owned_value_macro_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    zbus_utils::derive::expand_value_derive(
        ast,
        zbus_utils::derive::ValueType::OwnedValue,
        &utils::derive_config(),
    )
    .unwrap_or_else(|err| err.to_compile_error())
    .into()
}

/// Constructs a const [`Signature`] with compile-time validation.
///
/// This macro creates a `Signature` from a string literal at compile time, validating
/// that the signature string is valid D-Bus signature. Invalid signatures will cause
/// a compilation error.
///
/// # Examples
///
/// ## Basic usage
///
/// ```
/// use zbus::signature;
///
/// // Create signatures for basic types
/// let sig = signature!("s");  // String signature
/// assert_eq!(sig.to_string(), "s");
///
/// let sig = signature!("i");  // 32-bit integer signature
/// assert_eq!(sig.to_string(), "i");
/// ```
///
/// ## Container types
///
/// ```
/// use zbus::signature;
///
/// // Array of strings
/// let sig = signature!("as");
/// assert_eq!(sig.to_string(), "as");
///
/// // Dictionary mapping strings to variants
/// let sig = signature!("a{sv}");
/// assert_eq!(sig.to_string(), "a{sv}");
///
/// // Structures
/// let sig = signature!("(isx)");
/// assert_eq!(sig.to_string(), "(isx)");
/// ```
///
/// ## Const signatures
///
/// The macro can be used to create const signatures, which is especially useful
/// for defining signatures at compile time:
///
/// ```
/// use zbus::{signature, Signature};
///
/// const MY_SIGNATURE: Signature = signature!("a{sv}");
///
/// fn process_data(_data: &str) {
///     assert_eq!(MY_SIGNATURE.to_string(), "a{sv}");
/// }
/// ```
///
/// ## Using the `dict` alias
///
/// For convenience, `dict` is an alias for `a{sv}` (string-to-variant dictionary):
///
/// ```
/// use zbus::signature;
///
/// let sig = signature!("dict");
/// assert_eq!(sig.to_string(), "a{sv}");
/// ```
///
/// ## Compile-time validation
///
/// Invalid signatures will be caught at compile time:
///
/// ```compile_fail
/// use zbus::signature;
///
/// // This will fail to compile because 'z' is not a valid D-Bus type
/// let sig = signature!("z");
/// ```
///
/// [`Signature`]: https://docs.rs/zbus/latest/zbus/enum.Signature.html
#[proc_macro]
pub fn signature(input: TokenStream) -> TokenStream {
    zbus_utils::derive::expand_signature_macro(input.into(), &utils::wire_path())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
