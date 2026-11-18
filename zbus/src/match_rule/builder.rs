use crate::{
    Error, MatchRule, ObjectPath, Result, Str,
    match_rule::PathSpec,
    message::Type,
    names::{BusName, InterfaceName, MemberName, UniqueName},
};

const MAX_ARGS: u8 = 64;

/// Builder for [`MatchRule`].
///
/// This is created by [`MatchRule::builder`]. Each setter takes the value it is given, whether
/// that's an already parsed name or a string that still needs parsing, and converts it right
/// away, but it never fails: the first error a setter hits is recorded — a later call doesn't
/// clear it — and reported by [`Builder::build`], so a chain of setters never needs a `?` in
/// between.
#[derive(Debug)]
#[must_use]
pub struct Builder<'m> {
    rule: MatchRule<'m>,
    error: Option<Error>,
}

impl<'m> Builder<'m> {
    /// Build the `MatchRule`.
    ///
    /// # Errors
    ///
    /// The first error recorded by a setter.
    pub fn build(self) -> Result<MatchRule<'m>> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.rule),
        }
    }

    /// Set the sender.
    ///
    /// An invalid sender is reported by [`Builder::build`].
    pub fn sender<B>(mut self, sender: B) -> Self
    where
        B: TryInto<BusName<'m>>,
        B::Error: Into<Error>,
    {
        match sender.try_into() {
            Ok(sender) => self.rule.sender = Some(sender),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the message type.
    pub fn msg_type(mut self, msg_type: Type) -> Self {
        self.rule.msg_type = Some(msg_type);

        self
    }

    /// Set the interface.
    ///
    /// An invalid interface name is reported by [`Builder::build`].
    pub fn interface<I>(mut self, interface: I) -> Self
    where
        I: TryInto<InterfaceName<'m>>,
        I::Error: Into<Error>,
    {
        match interface.try_into() {
            Ok(interface) => self.rule.interface = Some(interface),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the member.
    ///
    /// An invalid member name is reported by [`Builder::build`].
    pub fn member<M>(mut self, member: M) -> Self
    where
        M: TryInto<MemberName<'m>>,
        M::Error: Into<Error>,
    {
        match member.try_into() {
            Ok(member) => self.rule.member = Some(member),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the path.
    ///
    /// Note: Since both a path and a path namespace are not allowed to appear in a match rule at
    /// the same time, this overrides any path namespace previously set.
    ///
    /// An invalid path is reported by [`Builder::build`].
    pub fn path<P>(mut self, path: P) -> Self
    where
        P: TryInto<ObjectPath<'m>>,
        P::Error: Into<Error>,
    {
        match path.try_into() {
            Ok(path) => self.rule.path_spec = Some(PathSpec::Path(path)),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the path namespace.
    ///
    /// Note: Since both a path and a path namespace are not allowed to appear in a match rule at
    /// the same time, this overrides any path previously set.
    ///
    /// An invalid path namespace is reported by [`Builder::build`].
    pub fn path_namespace<P>(mut self, path_namespace: P) -> Self
    where
        P: TryInto<ObjectPath<'m>>,
        P::Error: Into<Error>,
    {
        match path_namespace.try_into() {
            Ok(namespace) => self.rule.path_spec = Some(PathSpec::PathNamespace(namespace)),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the destination.
    ///
    /// An invalid destination is reported by [`Builder::build`].
    pub fn destination<B>(mut self, destination: B) -> Self
    where
        B: TryInto<UniqueName<'m>>,
        B::Error: Into<Error>,
    {
        match destination.try_into() {
            Ok(destination) => self.rule.destination = Some(destination),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Append an argument.
    ///
    /// Use this in instead of [`Builder::arg`] if you want to sequentially add args.
    ///
    /// An attempt to add the 65th argument is reported by [`Builder::build`].
    pub fn add_arg<S>(self, arg: S) -> Self
    where
        S: Into<Str<'m>>,
    {
        let idx = self.rule.args.len() as u8;

        self.arg(idx, arg)
    }

    /// Add an argument of a specified index.
    ///
    /// This replaces any argument previously added at `idx`.
    ///
    /// An `idx` of 64 or greater is reported by [`Builder::build`].
    pub fn arg<S>(mut self, idx: u8, arg: S) -> Self
    where
        S: Into<Str<'m>>,
    {
        if idx >= MAX_ARGS {
            self.record(Error::InvalidMatchRule);

            return self;
        }
        insert_indexed(&mut self.rule.args, idx, arg.into());

        self
    }

    /// Append a path argument.
    ///
    /// Use this in instead of [`Builder::arg_path`] if you want to sequentially add args.
    ///
    /// An invalid path, or an attempt to add the 65th path argument, is reported by
    /// [`Builder::build`].
    pub fn add_arg_path<P>(self, arg_path: P) -> Self
    where
        P: TryInto<ObjectPath<'m>>,
        P::Error: Into<Error>,
    {
        let idx = self.rule.arg_paths.len() as u8;

        self.arg_path(idx, arg_path)
    }

    /// Add a path argument of a specified index.
    ///
    /// This replaces any path argument previously added at `idx`.
    ///
    /// An invalid path, or an `idx` of 64 or greater, is reported by [`Builder::build`].
    pub fn arg_path<P>(mut self, idx: u8, arg_path: P) -> Self
    where
        P: TryInto<ObjectPath<'m>>,
        P::Error: Into<Error>,
    {
        if idx >= MAX_ARGS {
            self.record(Error::InvalidMatchRule);

            return self;
        }
        match arg_path.try_into() {
            Ok(arg_path) => insert_indexed(&mut self.rule.arg_paths, idx, arg_path),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set 0th argument's namespace.
    ///
    /// The namespace must be a valid bus name or a valid element of a bus name. For more
    /// information, see [the spec](https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus).
    ///
    /// An invalid namespace is reported by [`Builder::build`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use zbus::MatchRule;
    /// // Valid namespaces
    /// MatchRule::builder().arg0ns("org.mpris.MediaPlayer2").build().unwrap();
    /// MatchRule::builder().arg0ns("org").build().unwrap();
    /// MatchRule::builder().arg0ns(":org").build().unwrap();
    /// MatchRule::builder().arg0ns(":1org").build().unwrap();
    ///
    /// // Invalid namespaces
    /// MatchRule::builder().arg0ns("org.").build().unwrap_err();
    /// MatchRule::builder().arg0ns(".org").build().unwrap_err();
    /// MatchRule::builder().arg0ns("1org").build().unwrap_err();
    /// MatchRule::builder().arg0ns(".").build().unwrap_err();
    /// MatchRule::builder().arg0ns("org..freedesktop").build().unwrap_err();
    /// ````
    pub fn arg0ns<S>(mut self, namespace: S) -> Self
    where
        S: Into<Str<'m>>,
    {
        match validate_arg0ns(namespace.into()) {
            Ok(namespace) => self.rule.arg0ns = Some(namespace),
            Err(e) => self.record(e),
        }

        self
    }

    /// Set whether the rule has `eavesdrop='true'`, which it doesn't by default.
    ///
    /// See [`MatchRule::eavesdrop`] for what that means.
    pub fn eavesdrop(mut self, eavesdrop: bool) -> Self {
        self.rule.eavesdrop = eavesdrop;

        self
    }

    /// Create a builder for `MatchRule`.
    pub(crate) fn new() -> Self {
        Self {
            rule: MatchRule {
                msg_type: None,
                sender: None,
                interface: None,
                member: None,
                path_spec: None,
                destination: None,
                args: Vec::with_capacity(MAX_ARGS as usize),
                arg_paths: Vec::with_capacity(MAX_ARGS as usize),
                arg0ns: None,
                eavesdrop: false,
            },
            error: None,
        }
    }

    /// Record `error`, unless an earlier setter already recorded one.
    fn record(&mut self, error: Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

/// Add `value` at `idx`, replacing the value already recorded at that index, if any.
fn insert_indexed<T>(entries: &mut Vec<(u8, T)>, idx: u8, value: T) {
    match entries.binary_search_by(|(i, _)| i.cmp(&idx)) {
        Ok(i) => entries[i] = (idx, value),
        Err(i) => entries.insert(i, (idx, value)),
    }
}

/// Check that `namespace` is a valid bus name or a valid element of a bus name.
pub(crate) fn validate_arg0ns(namespace: Str<'_>) -> Result<Str<'_>> {
    // Rules: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus
    // minus the requirement to have more than one element.

    if namespace.is_empty() || namespace.len() > 255 {
        return Err(Error::InvalidMatchRule);
    }

    let (is_unique, namespace_str) = match namespace.strip_prefix(':') {
        Some(s) => (true, s),
        None => (false, namespace.as_str()),
    };

    let valid_first_char = |s: &str| match s.chars().next() {
        None | Some('.') => false,
        Some('0'..='9') if !is_unique => false,
        _ => true,
    };

    if !valid_first_char(namespace_str)
        || !namespace_str.split('.').all(valid_first_char)
        || !namespace_str
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(Error::InvalidMatchRule);
    }

    Ok(namespace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, OwnedMatchRule, names::OwnedBusName};
    use test_log::test;

    #[test]
    fn strings_are_converted() {
        let rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .sender("org.freedesktop.DBus")
            .interface("org.freedesktop.DBus.Properties")
            .member("PropertiesChanged")
            .path("/org/zbus")
            .destination(":1.11")
            .add_arg("org.zbus")
            .add_arg_path("/org/zbus/Arg")
            .arg0ns("org.zbus")
            .build()
            .unwrap();

        assert_eq!(
            rule.to_string(),
            "type='signal',\
             sender='org.freedesktop.DBus',\
             interface='org.freedesktop.DBus.Properties',\
             member='PropertiesChanged',\
             destination=':1.11',\
             path='/org/zbus',\
             arg0='org.zbus',\
             arg0path='/org/zbus/Arg',\
             arg0namespace='org.zbus'",
        );
    }

    #[test]
    fn invalid_string_is_reported_by_build() {
        let err = MatchRule::builder()
            .sender("not a bus name")
            .member("Whatever")
            .build()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            BusName::try_from("not a bus name").unwrap_err().to_string(),
        );
    }

    #[test]
    fn typed_values_need_no_error_handling() {
        let sender = BusName::try_from("org.zbus.Sender").unwrap();
        let path = ObjectPath::try_from("/org/zbus").unwrap();
        let rule = MatchRule::builder()
            .sender(&sender)
            .path(path.clone())
            .build()
            .unwrap();

        assert_eq!(rule.sender(), Some(&sender));
        assert_eq!(rule.path_spec(), Some(&PathSpec::Path(path)));

        let rule = MatchRule::builder()
            .sender(OwnedBusName::from(sender.clone()))
            .build()
            .unwrap();

        assert_eq!(rule.sender(), Some(&sender));
    }

    #[test]
    fn a_later_valid_call_keeps_the_first_error() {
        // Setting the field again, even to a valid value, doesn't clear the error it recorded.
        let err = MatchRule::builder()
            .path("bad")
            .path("/good")
            .build()
            .unwrap_err();

        assert_eq!(err, ObjectPath::try_from("bad").unwrap_err());

        let mut builder = MatchRule::builder();
        for i in 0..MAX_ARGS {
            builder = builder.add_arg(i.to_string());
        }

        // The 65th argument doesn't fit and the later valid argument doesn't undo that.
        let err = builder
            .add_arg("one too many")
            .arg(0, "replaced")
            .build()
            .unwrap_err();

        assert_eq!(err, Error::InvalidMatchRule);
    }

    #[test]
    fn rule_string_rejects_an_invalid_namespace_even_if_repeated() {
        // The parser validates each component itself, so a later valid `arg0namespace` must not
        // hide an earlier invalid one.
        let err = MatchRule::try_from("arg0namespace='org.',arg0namespace='org'").unwrap_err();

        assert_eq!(err, Error::InvalidMatchRule);
        MatchRule::try_from("arg0namespace='org'").unwrap();
    }

    #[test]
    fn rule_string_follows_the_quoting_of_the_specification() {
        // The two examples of the specification, which have the same arguments.
        for rule in [
            r"arg0=''\''',arg1='\',arg2=',',arg3='\\'",
            r"arg0=\',arg1=\,arg2=',',arg3=\\",
        ] {
            let parsed = MatchRule::try_from(rule).unwrap();

            assert_eq!(
                args(&parsed),
                [(0, "'"), (1, "\\"), (2, ","), (3, "\\\\")],
                "{rule}"
            );
        }
    }

    #[test]
    fn rule_string_values_need_no_quotes() {
        assert_eq!(
            MatchRule::try_from("type=signal").unwrap(),
            MatchRule::try_from("type='signal'").unwrap(),
        );

        let unquoted =
            MatchRule::try_from("type=signal,sender=org.zbus,member=Foo,arg0=bar,arg1=").unwrap();
        let quoted =
            MatchRule::try_from("type='signal',sender='org.zbus',member='Foo',arg0='bar',arg1=''")
                .unwrap();

        assert_eq!(unquoted, quoted);
        assert_eq!(unquoted.msg_type(), Some(Type::Signal));
        assert_eq!(args(&unquoted), [(0, "bar"), (1, "")]);
    }

    #[test]
    fn rule_string_keeps_a_quoted_comma_in_the_value() {
        let rule =
            MatchRule::try_from("type='signal',arg0='a,b',arg1=c,arg2=',',arg3='a=b,c'").unwrap();

        assert_eq!(rule.msg_type(), Some(Type::Signal));
        assert_eq!(args(&rule), [(0, "a,b"), (1, "c"), (2, ","), (3, "a=b,c")]);
    }

    #[test]
    fn rule_string_concatenates_quoted_and_unquoted_text() {
        // The quotes can start and end anywhere in a value, and a value can have several of them.
        let rule = MatchRule::try_from("arg0=a'b,c'd,arg1='a'b'c',arg2=''x'',arg3=a''b").unwrap();

        assert_eq!(args(&rule), [(0, "ab,cd"), (1, "abc"), (2, "x"), (3, "ab")]);
    }

    #[test]
    fn rule_string_escapes_an_apostrophe_outside_quotes() {
        let rule = MatchRule::try_from(r"arg0=\',arg1=a\'b,arg2=\'\',arg3='a'\''b'").unwrap();

        assert_eq!(args(&rule), [(0, "'"), (1, "a'b"), (2, "''"), (3, "a'b")]);
    }

    #[test]
    fn rule_string_backslash_escapes_only_an_apostrophe_outside_quotes() {
        // Outside quotes, a backslash stands for itself unless an apostrophe follows it. So it
        // doesn't escape a comma, nor another backslash, and it can end the string.
        let rule = MatchRule::try_from(r"arg0=a\b,arg1=\,arg2=\\',arg3=c\").unwrap();

        assert_eq!(
            args(&rule),
            [(0, "a\\b"), (1, "\\"), (2, "\\'"), (3, "c\\")]
        );

        // Within quotes, it always does.
        let rule = MatchRule::try_from(r"arg0='\',arg1='a\b\\'").unwrap();

        assert_eq!(args(&rule), [(0, "\\"), (1, "a\\b\\\\")]);
    }

    #[test]
    fn rule_string_rejects_an_unterminated_quote() {
        for rule in [
            "arg0='foo",
            "arg0='",
            "arg0='foo,arg1=bar",
            "arg0=foo'",
            "arg0='a'b'",
            r"arg0='\''",
        ] {
            assert_invalid(rule);
        }
    }

    #[test]
    fn rule_string_skips_whitespace_around_keys_but_not_in_values() {
        let rule = MatchRule::try_from(" type =signal,\targ0\n=a b ,  arg1 =' c', ").unwrap();

        assert_eq!(rule.msg_type(), Some(Type::Signal));
        assert_eq!(args(&rule), [(0, "a b "), (1, " c")]);

        for rule in [
            "type= signal",
            "type=signal ",
            "type='signal' ",
            "type=signal ,member=Foo",
        ] {
            assert_invalid(rule);
        }
    }

    #[test]
    fn rule_string_accepts_an_empty_value() {
        for rule in ["arg0=''", "arg0=", "arg0=,", "arg0='',", "arg0=''''"] {
            let rule = MatchRule::try_from(rule).unwrap();

            assert_eq!(args(&rule), [(0, "")]);
        }

        let rule = MatchRule::try_from("arg0=,arg1=b").unwrap();

        assert_eq!(args(&rule), [(0, ""), (1, "b")]);

        // A name is never empty, though.
        MatchRule::try_from("member=").unwrap_err();
        MatchRule::try_from("member=''").unwrap_err();
    }

    #[test]
    fn rule_string_accepts_a_trailing_comma_only() {
        let expected = MatchRule::try_from("type=signal").unwrap();

        assert_eq!(MatchRule::try_from("type=signal,").unwrap(), expected);
        assert_eq!(MatchRule::try_from("type=signal, ").unwrap(), expected);

        for rule in [
            ",",
            ",type=signal",
            "type=signal,,member=Foo",
            "type=signal,,",
        ] {
            assert_invalid(rule);
        }
    }

    #[test]
    fn rule_string_can_be_empty() {
        let rule = MatchRule::builder().build().unwrap();

        assert_eq!(rule.to_string(), "");
        assert_eq!(MatchRule::try_from("").unwrap(), rule);
        assert_eq!(MatchRule::try_from(" \t\r\n").unwrap(), rule);
    }

    #[test]
    fn rule_string_rejects_a_pair_without_a_key_or_a_value() {
        for rule in [
            "type",
            "type,member=Foo",
            "type=signal,member",
            "type=signal,member,",
            "=signal",
            "='signal'",
            "unknown=signal",
            // A comma before the `=` ends a pair without a value, even when the key would take the
            // next pair in.
            "arg0path,path='/foo'",
            "arg0path,,path='/foo'",
        ] {
            assert_invalid(rule);
        }
    }

    #[test]
    fn rule_string_reports_the_component_at_fault() {
        // The error is the same, whether the value is borrowed from the rule string or a copy.
        let expected = MemberName::try_from("not a member")
            .unwrap_err()
            .to_string();
        for rule in [
            "member=not a member",
            "member='not a member'",
            "member='not'' a member'",
        ] {
            let err = MatchRule::try_from(rule).unwrap_err();

            assert_eq!(err.to_string(), expected, "{rule}");
        }

        assert_invalid("type=bogus");
    }

    #[test]
    fn rule_string_values_are_borrowed_unless_they_need_unquoting() {
        let text = "sender=org.zbus,path='/org/zbus',arg0=foo,arg1='bar',arg2=''baz'',\
                    arg3=a'b'c,arg4=d\\'e";
        let in_text = |s: &str| text.as_bytes().as_ptr_range().contains(&s.as_ptr());
        let rule = MatchRule::try_from(text).unwrap();

        assert!(in_text(rule.sender().unwrap().as_str()));
        let Some(PathSpec::Path(path)) = rule.path_spec() else {
            panic!("expected a path, got {:?}", rule.path_spec());
        };
        assert!(in_text(path.as_str()));
        let values = args(&rule);
        assert_eq!(values.len(), 5);
        // Neither quotes around the whole value nor empty quoted text next to it need a copy.
        assert!(in_text(values[0].1) && in_text(values[1].1) && in_text(values[2].1));
        // Concatenating text, or escaping an apostrophe in it, does.
        assert_eq!(values[3], (3, "abc"));
        assert_eq!(values[4], (4, "d'e"));
        assert!(!in_text(values[3].1) && !in_text(values[4].1));
    }

    #[test]
    fn display_escapes_apostrophes() {
        let rule = MatchRule::builder()
            .add_arg("it's")
            .add_arg("'")
            .add_arg("''")
            .add_arg("no apostrophe")
            .build()
            .unwrap();

        assert_eq!(
            rule.to_string(),
            r"arg0='it'\''s',arg1=''\''',arg2=''\'''\''',arg3='no apostrophe'",
        );
    }

    #[test]
    fn display_round_trips() {
        let values = [
            "",
            "'",
            "''",
            "'a'",
            "it's",
            "a,b",
            ",",
            "\\",
            "\\\\",
            "\\'",
            "'\\",
            "a\\'b",
            "\\,'",
            " a ",
            "a=b,c=d",
            "\"quoted\"",
            "é'日本",
            "x'y,z\\w'",
        ];

        // Each value on its own, next to other components.
        for value in values {
            let rule = MatchRule::builder()
                .msg_type(Type::Signal)
                .sender("org.zbus.Sender")
                .add_arg(value)
                .arg_path(1, "/org/zbus")
                .build()
                .unwrap();
            let text = rule.to_string();
            let parsed = MatchRule::try_from(text.as_str()).unwrap();

            assert_eq!(parsed, rule, "{value:?}");
        }

        // All of them in one rule, along with every other kind of component.
        let mut builder = MatchRule::builder()
            .msg_type(Type::Signal)
            .sender("org.freedesktop.DBus")
            .interface("org.freedesktop.DBus.Properties")
            .member("PropertiesChanged")
            .path_namespace("/org/zbus")
            .destination(":1.11")
            .arg_path(2, "/org/zbus/Arg")
            .arg0ns("org.zbus");
        for value in values {
            builder = builder.add_arg(value);
        }
        let rule = builder.build().unwrap();
        let text = rule.to_string();
        let parsed = MatchRule::try_from(text.as_str()).unwrap();

        assert_eq!(parsed, rule);
    }

    #[test]
    fn eavesdrop_is_part_of_the_rule() {
        let without = MatchRule::builder().msg_type(Type::Signal).build().unwrap();
        let with = MatchRule::builder()
            .msg_type(Type::Signal)
            .eavesdrop(true)
            .build()
            .unwrap();

        assert!(!without.eavesdrop());
        assert!(with.eavesdrop());
        assert_ne!(with, without);
        assert!(with.to_owned().eavesdrop());
        assert!(with.clone().into_owned().eavesdrop());
        assert!(OwnedMatchRule::from(with).eavesdrop());
    }

    #[test]
    fn rule_string_parses_eavesdrop() {
        for (rule, eavesdrop) in [
            ("eavesdrop=true", true),
            ("eavesdrop='true'", true),
            ("eavesdrop=false", false),
            ("type=signal", false),
            // A repeated key keeps its last value, so a user can override the one a tool added.
            ("eavesdrop=true,type=signal,eavesdrop=false", false),
        ] {
            let parsed = MatchRule::try_from(rule).unwrap();

            assert_eq!(parsed.eavesdrop(), eavesdrop, "{rule}");
        }

        for rule in [
            "eavesdrop=maybe",
            "eavesdrop=",
            "eavesdrop=TRUE",
            "eavesdrop=true ",
        ] {
            assert_invalid(rule);
        }
    }

    #[test]
    fn display_writes_eavesdrop_only_if_set() {
        let rule = |eavesdrop| {
            MatchRule::builder()
                .msg_type(Type::Signal)
                .add_arg("a")
                .eavesdrop(eavesdrop)
                .build()
                .unwrap()
        };

        assert_eq!(
            rule(true).to_string(),
            "type='signal',arg0='a',eavesdrop='true'"
        );
        assert_eq!(rule(false).to_string(), "type='signal',arg0='a'");
        for eavesdrop in [true, false] {
            let text = rule(eavesdrop).to_string();

            assert_eq!(MatchRule::try_from(text.as_str()).unwrap(), rule(eavesdrop));
        }
    }

    #[test]
    fn matches_ignores_eavesdrop() {
        let rule = |eavesdrop| {
            MatchRule::builder()
                .msg_type(Type::Signal)
                .member("Event")
                .eavesdrop(eavesdrop)
                .build()
                .unwrap()
        };
        let signal = |member| Message::signal("/org/zbus", "org.zbus.Interface", member);
        let broadcast = signal("Event").sender(":1.42").build(&()).unwrap();
        // Whether a connection gets a message for another one is for the bus to decide.
        let unicast = signal("Event")
            .sender(":1.42")
            .destination(":1.43")
            .build(&())
            .unwrap();
        let other = signal("Other").sender(":1.42").build(&()).unwrap();

        for (msg, matched) in [(&broadcast, true), (&unicast, true), (&other, false)] {
            for eavesdrop in [true, false] {
                assert_eq!(rule(eavesdrop).matches(msg).unwrap(), matched, "{msg}");
            }
        }
    }

    #[test]
    fn rule_deserializes_from_a_rule_string() {
        use crate::wire::{LE, serialized::Context, to_bytes};
        use serde::{
            Deserialize,
            de::value::{self, BorrowedBytesDeserializer},
        };
        use serde_json::Value;

        let rule = r"type='signal',member='Changed',arg0='it'\''s'";
        let expected = MatchRule::try_from(rule).unwrap();

        // Borrowed from the message.
        let ctxt = Context::new(LE, 0);
        let encoded = to_bytes(ctxt, &rule).unwrap();
        let parsed: MatchRule<'_> = encoded.deserialize().unwrap().0;
        assert_eq!(parsed, expected);
        let member = parsed.member().unwrap();
        assert!(encoded.bytes().as_ptr_range().contains(&member.as_ptr()));

        // Lent as bytes.
        let de = BorrowedBytesDeserializer::<value::Error>::new(rule.as_bytes());
        assert_eq!(MatchRule::deserialize(de).unwrap(), expected);
        // Handed over owned.
        assert_eq!(MatchRule::deserialize(Value::from(rule)).unwrap(), expected);
        // Only shown, as a reader can't lend it.
        let json = serde_json::to_string(rule).unwrap();
        let mut de = serde_json::Deserializer::from_reader(json.as_bytes());
        assert_eq!(MatchRule::deserialize(&mut de).unwrap(), expected);

        let encoded = to_bytes(ctxt, &"type='bogus'").unwrap();
        encoded.deserialize::<MatchRule<'_>>().unwrap_err();
        MatchRule::deserialize(Value::from("type='bogus'")).unwrap_err();

        // RON tells the plain string that `MatchRule` serializes as from a newtype struct, and
        // escapes the apostrophes in it, so it's not lent.
        let ron = ron::to_string(&expected).unwrap();
        assert_eq!(ron::from_str::<MatchRule<'_>>(&ron).unwrap(), expected);

        let owned = OwnedMatchRule::from(expected);
        assert_eq!(ron::to_string(&owned).unwrap(), ron);
        assert_eq!(ron::from_str::<OwnedMatchRule>(&ron).unwrap(), owned);
    }

    #[test]
    fn owned_rule_deserializes_from_a_rule_string() {
        use crate::wire::{LE, serialized::Context, to_bytes};

        let ctxt = Context::new(LE, 0);
        let rule = r"type='signal',arg0='it'\''s'";
        let encoded = to_bytes(ctxt, &rule).unwrap();
        let owned: OwnedMatchRule = encoded.deserialize().unwrap().0;

        assert_eq!(owned, MatchRule::try_from(rule).unwrap());

        let encoded = to_bytes(ctxt, &"type='bogus'").unwrap();
        encoded.deserialize::<OwnedMatchRule>().unwrap_err();
    }

    /// Assert that `rule` is not a valid rule string, whatever the reason.
    fn assert_invalid(rule: &str) {
        assert_eq!(
            MatchRule::try_from(rule).unwrap_err(),
            Error::InvalidMatchRule,
            "{rule}",
        );
    }

    /// The arguments of `rule`, along with their indices.
    fn args<'a>(rule: &'a MatchRule<'_>) -> Vec<(u8, &'a str)> {
        rule.args()
            .iter()
            .map(|(idx, arg)| (*idx, arg.as_str()))
            .collect()
    }
}
