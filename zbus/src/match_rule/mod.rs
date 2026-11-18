//! Bus match rule API.

use std::{
    borrow::{Borrow, Cow},
    fmt::{Display, Write},
    ops::Deref,
};

use serde::{Deserialize, Serialize, de};

use crate::{
    Error, ObjectPath, Result, Str, Structure, Type as VariantType,
    message::Type,
    names::{BusName, InterfaceName, MemberName, UniqueName},
};

mod builder;
pub use builder::Builder;

/// A bus match rule for subscribing to specific messages.
///
/// This is mainly used to subscribe to specific signals as by default the bus will not send out
/// most broadcasted signals. This API is intended to make it easy to create and parse match rules.
/// See the [match rules section of the D-Bus specification][mrs] for a description of each
/// possible element of a match rule.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use zbus::MatchRule;
///
/// // Let's take the most typical example of match rule to subscribe to properties' changes:
/// let rule = MatchRule::builder()
///     .msg_type(zbus::message::Type::Signal)
///     .sender("org.freedesktop.DBus")
///     .interface("org.freedesktop.DBus.Properties")
///     .member("PropertiesChanged")
///     .add_arg("org.zbus")
///     // Sometimes it's useful to match empty strings (null check).
///     .add_arg("")
///     .build()?;
/// let rule_str = rule.to_string();
/// assert_eq!(
///     rule_str,
///     "type='signal',\
///      sender='org.freedesktop.DBus',\
///      interface='org.freedesktop.DBus.Properties',\
///      member='PropertiesChanged',\
///      arg0='org.zbus',\
///      arg1=''",
/// );
///
/// // Let's parse it back.
/// let parsed_rule = MatchRule::try_from(rule_str.as_str())?;
/// assert_eq!(rule, parsed_rule);
///
/// // Now for the `ObjectManager::InterfacesAdded` signal.
/// let rule = MatchRule::builder()
///     .msg_type(zbus::message::Type::Signal)
///     .sender("org.zbus")
///     .interface("org.freedesktop.DBus.ObjectManager")
///     .member("InterfacesAdded")
///     .arg_path(0, "/org/zbus/NewPath")
///     .build()?;
/// let rule_str = rule.to_string();
/// assert_eq!(
///     rule_str,
///     "type='signal',\
///      sender='org.zbus',\
///      interface='org.freedesktop.DBus.ObjectManager',\
///      member='InterfacesAdded',\
///      arg0path='/org/zbus/NewPath'",
/// );
///
/// // Let's parse it back.
/// let parsed_rule = MatchRule::try_from(rule_str.as_str())?;
/// assert_eq!(rule, parsed_rule);
///
/// # Ok(())
/// # }
/// ```
///
/// # Caveats
///
/// The `PartialEq` implementation assumes arguments in both rules are in the same order.
///
/// [mrs]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-bus-routing-match-rules
#[derive(Clone, Debug, PartialEq, Eq, Hash, VariantType)]
#[zbus(signature = "s")]
pub struct MatchRule<'m> {
    pub(crate) msg_type: Option<Type>,
    pub(crate) sender: Option<BusName<'m>>,
    pub(crate) interface: Option<InterfaceName<'m>>,
    pub(crate) member: Option<MemberName<'m>>,
    pub(crate) path_spec: Option<PathSpec<'m>>,
    pub(crate) destination: Option<UniqueName<'m>>,
    pub(crate) args: Vec<(u8, Str<'m>)>,
    pub(crate) arg_paths: Vec<(u8, ObjectPath<'m>)>,
    pub(crate) arg0ns: Option<Str<'m>>,
    pub(crate) eavesdrop: bool,
}

impl<'m> MatchRule<'m> {
    /// Create a builder for `MatchRule`.
    pub fn builder() -> Builder<'m> {
        Builder::new()
    }

    /// The sender, if set.
    pub fn sender(&self) -> Option<&BusName<'_>> {
        self.sender.as_ref()
    }

    /// The message type, if set.
    pub fn msg_type(&self) -> Option<Type> {
        self.msg_type
    }

    /// The interface, if set.
    pub fn interface(&self) -> Option<&InterfaceName<'_>> {
        self.interface.as_ref()
    }

    /// The member name, if set.
    pub fn member(&self) -> Option<&MemberName<'_>> {
        self.member.as_ref()
    }

    /// The path or path namespace, if set.
    pub fn path_spec(&self) -> Option<&PathSpec<'_>> {
        self.path_spec.as_ref()
    }

    /// The destination, if set.
    pub fn destination(&self) -> Option<&UniqueName<'_>> {
        self.destination.as_ref()
    }

    /// The arguments.
    pub fn args(&self) -> &[(u8, Str<'_>)] {
        self.args.as_ref()
    }

    /// The argument paths.
    pub fn arg_paths(&self) -> &[(u8, ObjectPath<'_>)] {
        self.arg_paths.as_ref()
    }

    /// Match messages whose first argument is within the specified namespace.
    pub fn arg0ns(&self) -> Option<&Str<'m>> {
        self.arg0ns.as_ref()
    }

    /// Whether the rule has `eavesdrop='true'`.
    ///
    /// This is the `eavesdrop` key of the match rules of the D-Bus specification. Since D-Bus
    /// 1.5.6, a rule only matches the messages that are sent to other connections if it has this.
    /// A connection gets the messages that are sent to it whatever its rules are, so this makes no
    /// difference to those.
    ///
    /// It is deprecated, in favour of the `BecomeMonitor` method of the bus, which treats every
    /// rule it is given as if it had this. [`Connection`] has an example of using it. A bus may
    /// restrict such rules to privileged connections, and dbus-broker refuses them in `AddMatch`.
    ///
    /// [`MatchRule::matches`] doesn't look at this, as it is the bus that decides which messages a
    /// connection gets.
    ///
    /// [`Connection`]: crate::Connection#monitoring-all-messages
    pub fn eavesdrop(&self) -> bool {
        self.eavesdrop
    }

    /// Create an owned clone of `self`.
    pub fn to_owned(&self) -> MatchRule<'static> {
        MatchRule {
            msg_type: self.msg_type,
            sender: self.sender.as_ref().map(|s| s.to_owned()),
            interface: self.interface.as_ref().map(|i| i.to_owned()),
            member: self.member.as_ref().map(|m| m.to_owned()),
            path_spec: self.path_spec.as_ref().map(|p| p.to_owned()),
            destination: self.destination.as_ref().map(|d| d.to_owned()),
            args: self.args.iter().map(|(i, s)| (*i, s.to_owned())).collect(),
            arg_paths: self
                .arg_paths
                .iter()
                .map(|(i, p)| (*i, p.to_owned()))
                .collect(),
            arg0ns: self.arg0ns.as_ref().map(|a| a.to_owned()),
            eavesdrop: self.eavesdrop,
        }
    }

    /// Convert into an owned clone of `self`.
    pub fn into_owned(self) -> MatchRule<'static> {
        MatchRule {
            msg_type: self.msg_type,
            sender: self.sender.map(|s| s.into_owned()),
            interface: self.interface.map(|i| i.into_owned()),
            member: self.member.map(|m| m.into_owned()),
            path_spec: self.path_spec.map(|p| p.into_owned()),
            destination: self.destination.map(|d| d.into_owned()),
            args: self
                .args
                .into_iter()
                .map(|(i, s)| (i, s.into_owned()))
                .collect(),
            arg_paths: self
                .arg_paths
                .into_iter()
                .map(|(i, p)| (i, p.into_owned()))
                .collect(),
            arg0ns: self.arg0ns.map(|a| a.into_owned()),
            eavesdrop: self.eavesdrop,
        }
    }

    /// Match the given message against this rule.
    ///
    /// # Caveats
    ///
    /// Since this method doesn't have any knowledge of names on the bus (or even connection to a
    /// bus) matching always succeeds for:
    ///
    /// * `sender` in the rule (if set) that is a well-known name. The `sender` on a message is
    ///   always a unique name.
    /// * `destination` in the rule when `destination` on the `msg` is a well-known name. The
    ///   `destination` on a match rule is always a unique name.
    ///
    /// Whether the rule has [`eavesdrop`](MatchRule::eavesdrop) makes no difference, as it is the
    /// bus that decides which messages a connection gets.
    pub fn matches(&self, msg: &zbus::message::Message) -> Result<bool> {
        let hdr = msg.header();

        // Start with message type.
        if let Some(msg_type) = self.msg_type()
            && msg_type != msg.message_type()
        {
            return Ok(false);
        }

        // Then check sender.
        if let Some(sender) = self.sender() {
            match sender {
                BusName::Unique(name) if Some(name) != hdr.sender() => {
                    return Ok(false);
                }
                BusName::Unique(_) => (),
                // We can't match against a well-known name.
                BusName::WellKnown(_) => (),
            }
        }

        // The interface.
        if let Some(interface) = self.interface() {
            match hdr.interface() {
                Some(msg_interface) if interface != msg_interface => return Ok(false),
                Some(_) => (),
                None => return Ok(false),
            }
        }

        // The member.
        if let Some(member) = self.member() {
            match hdr.member() {
                Some(msg_member) if member != msg_member => return Ok(false),
                Some(_) => (),
                None => return Ok(false),
            }
        }

        // The destination.
        if let Some(destination) = self.destination() {
            match hdr.destination() {
                Some(BusName::Unique(name)) if destination != name => {
                    return Ok(false);
                }
                Some(BusName::Unique(_)) | None => (),
                // We can't match against a well-known name.
                Some(BusName::WellKnown(_)) => (),
            };
        }

        // The path.
        if let Some(path_spec) = self.path_spec() {
            let msg_path = match hdr.path() {
                Some(p) => p,
                None => return Ok(false),
            };
            match path_spec {
                PathSpec::Path(path) if path != msg_path => return Ok(false),
                PathSpec::PathNamespace(path_ns) if !msg_path.starts_with(path_ns.as_str()) => {
                    return Ok(false);
                }
                PathSpec::Path(_) | PathSpec::PathNamespace(_) => (),
            }
        }

        // The arg0 namespace.
        if let Some(arg0_ns) = self.arg0ns() {
            if let Ok(arg0) = msg.body().deserialize_ignore_signature::<BusName<'_>>() {
                match arg0.strip_prefix(arg0_ns.as_str()) {
                    None => return Ok(false),
                    Some(s) if !s.is_empty() && !s.starts_with('.') => return Ok(false),
                    _ => (),
                }
            } else {
                return Ok(false);
            }
        }

        // Args
        if self.args().is_empty() && self.arg_paths().is_empty() {
            return Ok(true);
        }
        let body = msg.body();
        let structure = match body.deserialize::<Structure<'_>>() {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };
        let args = structure.fields();

        for (i, arg) in self.args() {
            match args.get(*i as usize) {
                Some(msg_arg) => match <&str>::try_from(msg_arg) {
                    Ok(msg_arg) if arg != msg_arg => return Ok(false),
                    Ok(_) => (),
                    Err(_) => return Ok(false),
                },
                None => return Ok(false),
            }
        }

        // Path args
        for (i, path) in self.arg_paths() {
            match args.get(*i as usize) {
                Some(msg_arg) => match <ObjectPath<'_>>::try_from(msg_arg) {
                    Ok(msg_arg) if *path != msg_arg => return Ok(false),
                    Ok(_) => (),
                    Err(_) => return Ok(false),
                },
                None => return Ok(false),
            }
        }

        Ok(true)
    }

    pub(crate) fn fdo_signal_builder<S>(signal_name: S) -> Builder<'m>
    where
        S: TryInto<MemberName<'m>>,
        S::Error: Into<Error>,
    {
        Builder::new()
            .msg_type(Type::Signal)
            .sender("org.freedesktop.DBus")
            .interface("org.freedesktop.DBus")
            .member(signal_name)
    }
}

/// The match rule string, which [`MatchRule::try_from`] parses back to an equal rule.
///
/// Each value is written within single quotes, and an apostrophe in a value is written as `'\''`:
/// the quotes are closed, the apostrophe is escaped and the quotes are opened again.
impl Display for MatchRule<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first_component = true;
        if let Some(msg_type) = self.msg_type() {
            let type_str = match msg_type {
                Type::Error => "error",
                Type::MethodCall => "method_call",
                Type::MethodReturn => "method_return",
                Type::Signal => "signal",
            };
            write_match_rule_string_component(f, "type", type_str, &mut first_component)?;
        }
        if let Some(sender) = self.sender() {
            write_match_rule_string_component(f, "sender", sender, &mut first_component)?;
        }
        if let Some(interface) = self.interface() {
            write_match_rule_string_component(f, "interface", interface, &mut first_component)?;
        }
        if let Some(member) = self.member() {
            write_match_rule_string_component(f, "member", member, &mut first_component)?;
        }
        if let Some(destination) = self.destination() {
            write_match_rule_string_component(f, "destination", destination, &mut first_component)?;
        }
        if let Some(path_spec) = self.path_spec() {
            let (key, value) = match path_spec {
                PathSpec::Path(path) => ("path", path),
                PathSpec::PathNamespace(ns) => ("path_namespace", ns),
            };
            write_match_rule_string_component(f, key, value, &mut first_component)?;
        }
        for (i, arg) in self.args() {
            let key = format_args!("arg{i}");
            write_match_rule_string_component(f, key, arg, &mut first_component)?;
        }
        for (i, arg_path) in self.arg_paths() {
            let key = format_args!("arg{i}path");
            write_match_rule_string_component(f, key, arg_path, &mut first_component)?;
        }
        if let Some(arg0namespace) = self.arg0ns() {
            write_match_rule_string_component(
                f,
                "arg0namespace",
                arg0namespace,
                &mut first_component,
            )?;
        }
        if self.eavesdrop() {
            write_match_rule_string_component(f, "eavesdrop", "true", &mut first_component)?;
        }

        Ok(())
    }
}

fn write_match_rule_string_component(
    f: &mut std::fmt::Formatter<'_>,
    key: impl Display,
    value: &str,
    first_component: &mut bool,
) -> std::fmt::Result {
    write_comma(f, first_component)?;
    write!(f, "{key}='")?;
    // Within quotes an apostrophe would end them, so they're closed around the escaped apostrophe.
    for (i, part) in value.split('\'').enumerate() {
        if i > 0 {
            f.write_str("'\\''")?;
        }
        f.write_str(part)?;
    }
    f.write_char('\'')?;

    Ok(())
}

fn write_comma(f: &mut std::fmt::Formatter<'_>, first_component: &mut bool) -> std::fmt::Result {
    if *first_component {
        *first_component = false;
    } else {
        f.write_char(',')?;
    }

    Ok(())
}

/// Parse a match rule string.
///
/// The string is a list of `key=value` pairs, separated by commas, as described in the [match
/// rules section of the D-Bus specification][mrs]. A value is a sequence of quoted and unquoted
/// text, which is concatenated:
///
/// * Within single quotes everything stands for itself, up to the apostrophe that ends them. This
///   includes commas and backslashes.
/// * Outside single quotes a comma ends the pair and `\'` (backslash, apostrophe) stands for an
///   apostrophe. Any other backslash stands for itself.
///
/// For example, `arg0=a'b,c'd` has the value `ab,cd`, and `arg0=''\'''` has the value `'`.
///
/// Whitespace before a key and between a key and its `=` is skipped, while the whitespace of a
/// value is part of it. The last pair can be followed by a comma, and the empty string is the rule
/// with no conditions. Anything else, like an unterminated quote or an unknown key, is an error.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use zbus::MatchRule;
/// // The quotes are optional, unless they protect a comma.
/// let rule = MatchRule::try_from("type=signal,arg0='a,b'")?;
/// assert_eq!(rule, MatchRule::try_from("type='signal',arg0='a,b'")?);
/// assert_eq!(rule.args()[0].1, "a,b");
///
/// // An apostrophe is escaped outside of the quotes.
/// let rule = MatchRule::try_from(r"arg0='it'\''s'")?;
/// assert_eq!(rule.args()[0].1, "it's");
/// # Ok(())
/// # }
/// ```
///
/// [mrs]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-bus-routing-match-rules
impl<'m> TryFrom<&'m str> for MatchRule<'m> {
    type Error = Error;

    fn try_from(s: &'m str) -> Result<Self> {
        let mut builder = MatchRule::builder();
        for pair in Pairs(s) {
            let (key, value) = pair?;
            builder = match key {
                "type" => {
                    let msg_type = match value.as_ref() {
                        "error" => Type::Error,
                        "method_call" => Type::MethodCall,
                        "method_return" => Type::MethodReturn,
                        "signal" => Type::Signal,
                        _ => return Err(Error::InvalidMatchRule),
                    };
                    builder.msg_type(msg_type)
                }
                "eavesdrop" => {
                    let eavesdrop = match value.as_ref() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(Error::InvalidMatchRule),
                    };
                    builder.eavesdrop(eavesdrop)
                }
                // Each component is converted here, rather than left to the builder, so that an
                // invalid rule string is reported by the component that's actually at fault.
                "sender" => builder.sender(BusName::try_from(value)?),
                "interface" => builder.interface(InterfaceName::try_from(value)?),
                "member" => builder.member(MemberName::try_from(value)?),
                "path" => builder.path(ObjectPath::try_from(value)?),
                "path_namespace" => builder.path_namespace(ObjectPath::try_from(value)?),
                "destination" => builder.destination(UniqueName::try_from(value)?),
                "arg0namespace" => builder.arg0ns(builder::validate_arg0ns(value.into())?),
                key if key.starts_with("arg") => {
                    if let Some(trailing_idx) = key.find("path") {
                        let idx = key[3..trailing_idx]
                            .parse::<u8>()
                            .map_err(|_| Error::InvalidMatchRule)?;
                        builder.arg_path(idx, ObjectPath::try_from(value)?)
                    } else {
                        let idx = key[3..]
                            .parse::<u8>()
                            .map_err(|_| Error::InvalidMatchRule)?;
                        builder.arg(idx, value)
                    }
                }
                _ => return Err(Error::InvalidMatchRule),
            };
        }

        builder.build()
    }
}

/// The `key=value` pairs of a match rule string.
///
/// The keys are left alone, other than for the whitespace around them, and the values are
/// unquoted. An error ends the iteration.
struct Pairs<'m>(&'m str);

impl<'m> Iterator for Pairs<'m> {
    type Item = Result<(&'m str, Cow<'m, str>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // The whitespace before a key is skipped. Once only whitespace is left there's no other
        // key, which is what lets the last pair be followed by a comma.
        let input = self.0.trim_start_matches(is_space);
        if input.is_empty() {
            return None;
        }

        match split_pair(input) {
            Ok((key, value, rest)) => {
                self.0 = rest;

                Some(Ok((key, value)))
            }
            Err(e) => {
                self.0 = "";

                Some(Err(e))
            }
        }
    }
}

/// Split the first pair off `input`.
///
/// The key is what precedes the first `=`, minus the whitespace right before the `=`. Returns the
/// key, the unquoted value and the rest of `input`, after the comma that ends the pair.
fn split_pair(input: &str) -> Result<(&str, Cow<'_, str>, &str)> {
    // A comma before the `=` ends a pair without a value, so it's not part of the key.
    let pos = input.find(['=', ',']).ok_or(Error::InvalidMatchRule)?;
    let (key, input) = input.split_at(pos);
    let input = input.strip_prefix('=').ok_or(Error::InvalidMatchRule)?;
    let (value, rest) = split_value(input)?;

    Ok((key.trim_end_matches(is_space), unquote(value), rest))
}

/// Split the value off `input`, which starts right after the `=` of a pair.
///
/// The value ends at the first comma that's not within single quotes, or at the end of `input`.
/// Returns the value, still quoted, and the rest of `input`, after that comma.
fn split_value(input: &str) -> Result<(&str, &str)> {
    let mut quoted = false;
    // Whether the last character was a backslash outside quotes, which escapes an apostrophe.
    let mut escaping = false;
    // The characters looked for are ASCII, so the slices start and end on character boundaries.
    for (i, c) in input.bytes().enumerate() {
        match c {
            b'\'' if quoted => quoted = false,
            b'\'' if !escaping => quoted = true,
            b',' if !quoted => return Ok((&input[..i], &input[i + 1..])),
            _ => (),
        }
        escaping = !quoted && c == b'\\';
    }
    if quoted {
        return Err(Error::InvalidMatchRule);
    }

    Ok((input, ""))
}

/// Unquote a value that [`split_value`] split off, so its quotes are all closed.
///
/// Within single quotes, a backslash stands for itself and an apostrophe ends them. Outside them,
/// `\'` stands for an apostrophe and any other backslash for itself.
///
/// The value borrows from `value` when it's a single piece of it, quoted or not, as most values
/// are. Otherwise, its pieces are copied into one string, big enough for all of them.
fn unquote(value: &str) -> Cow<'_, str> {
    let mut unquoted = Cow::Borrowed("");
    let mut rest = value;
    while let Some((text, after)) = rest.split_once('\'') {
        match text.strip_suffix('\\') {
            // The apostrophe is escaped. It's part of the value and does not start a quote.
            Some(text) => {
                push(&mut unquoted, text, value.len());
                push(&mut unquoted, "'", value.len());
                rest = after;
            }
            None => {
                // `split_value` made sure that the quote is closed.
                let (quoted, after) = after.split_once('\'').unwrap_or((after, ""));
                push(&mut unquoted, text, value.len());
                push(&mut unquoted, quoted, value.len());
                rest = after;
            }
        }
    }
    push(&mut unquoted, rest, value.len());

    unquoted
}

/// Add `piece` to the end of `value`.
///
/// `value` borrows its only non-empty piece. Once it gets another one, it's copied into a string of
/// `capacity`, which has to be enough for every piece that's added to it.
fn push<'a>(value: &mut Cow<'a, str>, piece: &'a str, capacity: usize) {
    if piece.is_empty() {
        return;
    }

    match value {
        Cow::Borrowed("") => *value = Cow::Borrowed(piece),
        Cow::Borrowed(borrowed) => {
            let mut owned = String::with_capacity(capacity);
            owned.push_str(borrowed);
            owned.push_str(piece);
            *value = Cow::Owned(owned);
        }
        Cow::Owned(owned) => owned.push_str(piece),
    }
}

/// Whether `c` is whitespace to skip around the key of a pair.
///
/// These are the characters dbus-daemon skips. `char::is_ascii_whitespace` is not used, as it also
/// takes the form feed.
fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

impl<'de: 'm, 'm> Deserialize<'de> for MatchRule<'m> {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rule = Str::deserialize(deserializer)?;

        match rule.as_borrowed() {
            Some(rule) => Self::try_from(rule),
            // `to_owned`, unlike `into_owned`, doesn't keep the spare capacity of the parsed rule.
            None => MatchRule::try_from(rule.as_str()).map(|rule| rule.to_owned()),
        }
        .map_err(de::Error::custom)
    }
}

impl Serialize for MatchRule<'_> {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// The path or path namespace.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathSpec<'m> {
    /// Match messages sent from or to exactly this object path, the `path` key of a match rule.
    Path(ObjectPath<'m>),
    /// Match messages sent from or to this object path or any object path below it, the
    /// `path_namespace` key of a match rule.
    PathNamespace(ObjectPath<'m>),
}

impl PathSpec<'_> {
    /// Create an owned clone of `self`.
    fn to_owned(&self) -> PathSpec<'static> {
        match self {
            PathSpec::Path(path) => PathSpec::Path(path.to_owned()),
            PathSpec::PathNamespace(ns) => PathSpec::PathNamespace(ns.to_owned()),
        }
    }

    /// Convert into an owned clone of `self`.
    pub fn into_owned(self) -> PathSpec<'static> {
        match self {
            PathSpec::Path(path) => PathSpec::Path(path.into_owned()),
            PathSpec::PathNamespace(ns) => PathSpec::PathNamespace(ns.into_owned()),
        }
    }
}

/// Owned sibling of [`MatchRule`].
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, VariantType)]
#[serde(transparent)]
pub struct OwnedMatchRule(#[serde(borrow)] MatchRule<'static>);

impl OwnedMatchRule {
    /// Convert to the inner `MatchRule`, consuming `self`.
    pub fn into_inner(self) -> MatchRule<'static> {
        self.0
    }

    /// Get a reference to the inner `MatchRule`.
    pub fn inner(&self) -> &MatchRule<'static> {
        &self.0
    }
}

impl Deref for OwnedMatchRule {
    type Target = MatchRule<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Borrow<MatchRule<'a>> for OwnedMatchRule {
    fn borrow(&self) -> &MatchRule<'a> {
        &self.0
    }
}

impl From<OwnedMatchRule> for MatchRule<'_> {
    fn from(o: OwnedMatchRule) -> Self {
        o.into_inner()
    }
}

impl<'unowned, 'owned: 'unowned> From<&'owned OwnedMatchRule> for MatchRule<'unowned> {
    fn from(rule: &'owned OwnedMatchRule) -> Self {
        rule.inner().clone()
    }
}

impl From<MatchRule<'_>> for OwnedMatchRule {
    fn from(rule: MatchRule<'_>) -> Self {
        OwnedMatchRule(rule.into_owned())
    }
}

impl TryFrom<&'_ str> for OwnedMatchRule {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Ok(Self::from(MatchRule::try_from(value)?))
    }
}

impl<'de> Deserialize<'de> for OwnedMatchRule {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(OwnedMatchRuleVisitor)
    }
}

/// Parses an [`OwnedMatchRule`] right from the string that the deserializer has, rather than from
/// a copy of it.
struct OwnedMatchRuleVisitor;

impl de::Visitor<'_> for OwnedMatchRuleVisitor {
    type Value = OwnedMatchRule;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a match rule string")
    }

    fn visit_str<E>(self, rule: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        // `to_owned`, unlike `into_owned`, doesn't keep the spare capacity of the parsed rule.
        MatchRule::try_from(rule)
            .map(|rule| OwnedMatchRule(rule.to_owned()))
            .map_err(E::custom)
    }
}

impl PartialEq<MatchRule<'_>> for OwnedMatchRule {
    fn eq(&self, other: &MatchRule<'_>) -> bool {
        self.0 == *other
    }
}
