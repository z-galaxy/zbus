use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};
use test_log::test;

use super::parse;
use crate::{
    Error, Signature, Variant,
    message::{Fields, Header, Message, PrimaryHeader, Type, header::MIN_MESSAGE_SIZE},
    names::{BusName, UniqueName, WellKnownName},
    wire::{
        BE, Endian, LE,
        serialized::{Context, Data},
        to_bytes,
    },
};

#[test]
fn same_as_decoding_a_header() {
    for bytes in headers() {
        parse(&bytes).unwrap();
        assert_same(&bytes);

        // Every change of one byte of the header to another value.
        let len = header_len(&bytes);
        let mut changed = bytes.clone();
        for i in 0..len {
            for value in VALUES {
                changed[i] = value;
                assert_same(&changed);
            }
            changed[i] = bytes[i];
        }

        // Every header cut short.
        for len in 0..len {
            assert_same(&bytes[..len]);
        }
    }
}

#[test]
fn fields_of_another_type() {
    // A member field whose value is an object path.
    let bytes = raw_header(&[3, 1, b'o', 0, 2, 0, 0, 0, b'/', b'a', 0]);
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField);
}

#[test]
fn decoding_fields_of_another_type() {
    // Decoding a header through serde refuses a field whose value is of another type than its code
    // calls for from the signature of its variant, before it reads the value, as the parser does: a
    // path field that holds an `ay` of 1 GiB, of which no byte follows, is refused for its type
    // rather than for running past the end of the bytes.
    let bytes = raw_header(&[1, 2, b'a', b'y', 0, 0, 0, 0, 0, 0, 0, 0x40]);
    let data = Data::new(&bytes[..], Context::new(LE, 0));
    let decoded = data.deserialize::<Header<'_>>();
    assert_eq!(
        decoded.unwrap_err(),
        Error::Failure("invalid value: string \"ay\", expected a value of signature `o`".into())
    );
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField);
}

#[test]
fn decoding_fields_in_a_self_describing_format() {
    // In a self-describing format, such as JSON, a variant is a map of its signature and its value.
    // A header goes through one and back, and the value of each field is read as the type that its
    // code calls for there too: a path field that holds an array of bytes, the last of which isn't
    // one, is refused as the array starts, rather than for that element.
    let msg = Message::method_call("/org/zbus/Test", "Ping")
        .interface("org.zbus.Test")
        .build(&(42u32, "Testtest"))
        .unwrap();
    let header = msg.header();
    let json = serde_json::to_string(&header).unwrap();
    let decoded: Header<'_> = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.path(), header.path());
    assert_eq!(decoded.interface(), header.interface());
    assert_eq!(decoded.member(), header.member());
    assert_eq!(decoded.signature(), header.signature());

    let path = r#"{"signature":"o","value":"/org/zbus/Test"}"#;
    assert!(json.contains(path), "{json}");
    let json = json.replace(path, r#"{"signature":"ay","value":[1,2,"x"]}"#);
    let error = serde_json::from_str::<Header<'_>>(&json).unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid type: sequence, expected a string"),
        "{error}"
    );
}

#[test]
fn unknown_fields() {
    // The specification asks for a field with an unknown code to be ignored, but decoding a
    // header through serde rejects it, and so does the parser.
    let bytes = raw_header(&[10, 1, b's', 0, 1, 0, 0, 0, b'a', 0]);
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField);
}

#[test]
fn ends_of_strings() {
    // A string ends with a 0, as does the signature of a variant, which libdbus checks, unlike
    // decoding a header through serde.
    let bytes = raw_header(&[3, 1, b's', 0, 4, 0, 0, 0, b'P', b'i', b'n', b'g', 0]);
    assert_eq!(header(&bytes).member().unwrap(), "Ping");
    for (i, byte) in [(3, b'x'), (12, b'y')] {
        let mut bytes = bytes.clone();
        bytes[MIN_MESSAGE_SIZE + i] = byte;
        assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField, "{bytes:?}");
    }
}

#[test]
fn repeated_fields() {
    // A field that comes twice makes the header invalid, as it does with libdbus, and each of
    // them is checked.
    let mut fields = vec![
        3, 1, b's', 0, 4, 0, 0, 0, b'P', b'i', b'n', b'g', 0, 0, 0, 0,
    ];
    fields.extend([3, 1, b's', 0, 4, 0, 0, 0, b'P', b'o', b'n', b'g', 0]);
    let bytes = raw_header(&fields);
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField);

    fields[8] = b'0';
    let bytes = raw_header(&fields);
    assert!(matches!(parse(&bytes).unwrap_err(), Error::InvalidName(_)));
}

#[test]
fn padding() {
    let mut fields = vec![
        3, 1, b's', 0, 4, 0, 0, 0, b'P', b'i', b'n', b'g', 0, 0, 0, 0,
    ];
    fields.extend([3, 1, b's', 0, 4, 0, 0, 0, b'P', b'o', b'n', b'g', 0]);
    fields[14] = 7;
    let bytes = raw_header(&fields);
    assert_eq!(parse(&bytes).unwrap_err(), Error::PaddingNot0(7));
}

#[test]
fn destinations() {
    for (destination, unique) in [
        (":1.42", true),
        // The bus itself has a unique name that looks like a well-known one.
        ("org.freedesktop.DBus", true),
        ("org.zbus.Test", false),
    ] {
        let msg = Message::method_call("/", "Ping")
            .destination(destination)
            .build(&())
            .unwrap();
        let header = header(msg.data());
        match header.destination().unwrap() {
            BusName::Unique(name) if unique => assert_eq!(name, destination),
            BusName::WellKnown(name) if !unique => assert_eq!(name, destination),
            name => panic!("{destination} parsed as {name:?}"),
        }
    }
}

#[test]
fn primary_header() {
    let mut bytes = raw_header(&[]);
    assert_eq!(header(&bytes).message_type(), Type::MethodCall);

    // Unknown flags are kept, and any protocol version is taken, which only a received message
    // is checked for.
    bytes[2] = 0x9;
    bytes[3] = 2;
    let primary = header(&bytes).into_primary();
    assert_eq!(primary.flags().bits(), 0x9);
    assert_eq!(primary.protocol_version(), 2);

    bytes[1] = 5;
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidField);
    bytes[1] = 1;

    bytes[8..12].copy_from_slice(&[0; 4]);
    assert_eq!(parse(&bytes).unwrap_err(), Error::InvalidSerial);

    bytes[0] = b'x';
    assert_eq!(parse(&bytes).unwrap_err(), Error::IncorrectEndian);
    assert_eq!(parse(&[]).unwrap_err(), Error::OutOfBounds);
}

#[test]
fn fields_past_the_end() {
    let mut bytes = raw_header(&[3, 1, b's', 0, 4, 0, 0, 0, b'P', b'i', b'n', b'g', 0]);
    bytes.pop();
    assert_eq!(parse(&bytes).unwrap_err(), Error::OutOfBounds);

    // A string that claims to run past the end of the fields.
    let bytes = raw_header(&[3, 1, b's', 0, 5, 0, 0, 0, b'P', b'i', b'n', b'g', 0]);
    assert_eq!(parse(&bytes).unwrap_err(), Error::OutOfBounds);
}

/// Check that parsing `bytes` finds what decoding a [`Header`] from them through serde does.
fn assert_same(bytes: &[u8]) {
    let parsed = parse(bytes);
    let endian = match bytes.first() {
        Some(b'B') => BE,
        _ => LE,
    };
    let data = Data::new(bytes, Context::new(endian, 0));
    let decoded = data.deserialize::<Header<'_>>();

    // Decoding through serde skips the byte that ends a string without checking that it's
    // there, so it takes a header whose last string ends right where the bytes do, without
    // that byte, while the parser rejects any header that runs past the end of the bytes.
    if bytes.len() >= MIN_MESSAGE_SIZE {
        let fields_len = endian.read_u32(&bytes[MIN_MESSAGE_SIZE - 4..MIN_MESSAGE_SIZE]);
        if MIN_MESSAGE_SIZE + fields_len as usize > bytes.len() {
            assert!(parsed.is_err(), "{bytes:?}: {parsed:?}");

            return;
        }
    }

    match (parsed, decoded) {
        (Ok((primary, len, quick_fields)), Ok((decoded, decoded_len))) => {
            assert_eq!(len, decoded_len);
            assert!(canonical_fields(bytes, endian), "{bytes:?}");
            // SAFETY: `quick_fields` were parsed from `bytes`.
            let fields = unsafe { quick_fields.fields(bytes) };
            assert_eq_headers(&Header::new(primary, fields), &decoded, bytes);
        }
        (Err(_), Err(_)) => (),
        // Decoding through serde doesn't check that strings end with a 0, nor that no field comes
        // twice.
        (Err(Error::InvalidField), Ok(_)) if !canonical_fields(bytes, endian) => (),
        // Where zbus_utils is built for GVariant as well, as building the whole workspace with all
        // its features has it, the parser takes the maybe type of GVariant in the signature of the
        // body, which decoding through serde rejects.
        (Ok(_), Err(e)) if e == crate::wire::dbus::maybe_not_in_dbus() => (),
        (parsed, decoded) => panic!("{bytes:?}: parsed: {parsed:?}, decoded: {decoded:?}"),
    }
}

/// Whether the fields of the header in `bytes`, which decodes through serde, end each of their
/// strings and signatures with a 0, none of them coming twice, which decoding through serde doesn't
/// check.
fn canonical_fields(bytes: &[u8], endian: Endian) -> bool {
    let end = MIN_MESSAGE_SIZE + endian.read_u32(&bytes[MIN_MESSAGE_SIZE - 4..]) as usize;
    let byte = |i: usize| bytes.get(i).copied();
    let mut codes = HashSet::new();
    let mut field = MIN_MESSAGE_SIZE;
    while field < end {
        // A field starts with its code, and the signature of its value: 1 type, and a 0. Its
        // value comes next, on a 4-byte boundary, as the field starts on an 8-byte one.
        let value = field + 4;
        let value_end = match byte(field + 2) {
            Some(b'u') => value + 4,
            Some(b'g') => match byte(value) {
                Some(len) => value + 1 + len as usize + 1,
                None => return false,
            },
            _ => match bytes.get(value..value + 4) {
                Some(len) => value + 4 + endian.read_u32(len) as usize + 1,
                None => return false,
            },
        };
        let ends_with_0 = byte(field + 3) == Some(0)
            && (byte(field + 2) == Some(b'u') || byte(value_end - 1) == Some(0));
        if !codes.insert(byte(field)) || !ends_with_0 {
            return false;
        }
        field = value_end.next_multiple_of(8);
    }

    true
}

fn assert_eq_headers(parsed: &Header<'_>, decoded: &Header<'_>, bytes: &[u8]) {
    let (primary, decoded_primary) = (parsed.primary(), decoded.primary());
    assert_eq!(
        primary.endian_sig(),
        decoded_primary.endian_sig(),
        "{bytes:?}"
    );
    assert_eq!(primary.msg_type(), decoded_primary.msg_type(), "{bytes:?}");
    assert_eq!(primary.flags(), decoded_primary.flags(), "{bytes:?}");
    assert_eq!(
        primary.protocol_version(),
        decoded_primary.protocol_version(),
        "{bytes:?}"
    );
    assert_eq!(primary.body_len(), decoded_primary.body_len(), "{bytes:?}");
    assert_eq!(
        primary.serial_num(),
        decoded_primary.serial_num(),
        "{bytes:?}"
    );

    assert_eq!(parsed.path(), decoded.path(), "{bytes:?}");
    assert_eq!(parsed.interface(), decoded.interface(), "{bytes:?}");
    assert_eq!(parsed.member(), decoded.member(), "{bytes:?}");
    assert_eq!(parsed.error_name(), decoded.error_name(), "{bytes:?}");
    assert_eq!(parsed.reply_serial(), decoded.reply_serial(), "{bytes:?}");
    assert_eq!(parsed.destination(), decoded.destination(), "{bytes:?}");
    assert_eq!(parsed.sender(), decoded.sender(), "{bytes:?}");
    assert_eq!(parsed.signature(), decoded.signature(), "{bytes:?}");
    assert_eq!(parsed.unix_fds(), decoded.unix_fds(), "{bytes:?}");
}

/// The header of the message in `bytes`, which must be valid.
fn header(bytes: &[u8]) -> Header<'_> {
    let (primary, _, quick_fields) = parse(bytes).unwrap();
    // SAFETY: `quick_fields` were parsed from `bytes`.
    let fields = unsafe { quick_fields.fields(bytes) };
    // The fields borrow `quick_fields`, which goes out of scope, so they're copied.
    let fields = Fields {
        path: fields.path.map(|path| path.to_owned()),
        interface: fields.interface.map(|name| name.to_owned()),
        member: fields.member.map(|name| name.to_owned()),
        error_name: fields.error_name.map(|name| name.to_owned()),
        reply_serial: fields.reply_serial,
        destination: fields.destination.map(|name| name.to_owned()),
        sender: fields.sender.map(|name| name.to_owned()),
        signature: Cow::Owned(fields.signature.into_owned()),
        unix_fds: fields.unix_fds,
    };

    Header::new(primary, fields)
}

/// The length of the header at the start of `bytes`, which must be valid.
fn header_len(bytes: &[u8]) -> usize {
    let (_, len, _) = parse(bytes).unwrap();

    len
}

/// A little-endian header of a method call, with `fields` as the bytes of its fields.
fn raw_header(fields: &[u8]) -> Vec<u8> {
    let mut bytes = vec![b'l', 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0];
    bytes.extend(u32::try_from(fields.len()).unwrap().to_le_bytes());
    bytes.extend(fields);

    bytes
}

/// Headers with every field, of every type of message, in both byte orders.
fn headers() -> Vec<Vec<u8>> {
    let mut headers = vec![];
    for endian in [Endian::Little, Endian::Big] {
        let call = Message::method_call("/org/zbus/Test", "Ping")
            .destination("org.zbus.Test")
            .interface("org.zbus.Test")
            .sender(":1.42")
            .endian(endian)
            .build(&(42u32, "Testtest"))
            .unwrap();
        let call_header = call.header();
        let signal = Message::signal("/org/zbus/Test", "org.zbus.Test", "PropertiesChanged")
            .sender(":1.42")
            .endian(endian)
            .build(&(
                "org.zbus.Test",
                HashMap::from([("Answer", Variant::from(42u32))]),
                vec!["Question"],
            ))
            .unwrap();
        let method_return = Message::method_return(&call_header)
            .sender("org.freedesktop.DBus")
            .build(&())
            .unwrap();
        let error = Message::error(&call_header, "org.zbus.Error")
            .build(&"kaboom")
            .unwrap();
        for msg in [&call, &signal, &method_return, &error] {
            headers.push(msg.data().bytes().to_vec());
        }

        // Every field at once, which the builder doesn't make: the number of file descriptors
        // is only set by passing them.
        let fields = Fields {
            path: Some("/org/zbus/Test".try_into().unwrap()),
            interface: Some("org.zbus.Test".try_into().unwrap()),
            member: Some("Ping".try_into().unwrap()),
            error_name: Some("org.zbus.Error".try_into().unwrap()),
            reply_serial: Some(7.try_into().unwrap()),
            destination: Some(BusName::WellKnown(
                WellKnownName::try_from("org.zbus.Test").unwrap(),
            )),
            sender: Some(UniqueName::try_from(":1.42").unwrap()),
            signature: Cow::Owned(Signature::try_from("a{sv}(is)").unwrap()),
            unix_fds: Some(1),
        };
        let mut primary = PrimaryHeader::new(Type::MethodCall, 0);
        primary.set_endian_sig(endian.into());
        let header = Header::new(primary, fields);
        let ctxt = Context::new(endian, 0);
        headers.push(to_bytes(ctxt, &header).unwrap().bytes().to_vec());
    }

    headers
}

/// What the bytes of a header are changed to: the codes of the fields and of the types, what
/// names and paths are made of, and others.
const VALUES: [u8; 38] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0x7f, 0x80, 0xff, b'B', b'l', b'o', b's', b'g', b'u', b'v',
    b'y', b'a', b'(', b')', b'{', b'}', b'm', b'h', b'.', b'/', b':', b'-', b'_', b'0', b'A', b'z',
    b' ',
];
