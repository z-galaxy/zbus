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
fn unknown_fields() {
    // Field code 0 is invalid and must still be rejected by both the parser and serde.
    let invalid_zero = raw_header(&[0, 1, b's', 0, 1, 0, 0, 0, b'a', 0]);
    assert_eq!(parse(&invalid_zero).unwrap_err(), Error::InvalidField);
    let data = Data::new(&invalid_zero, Context::new(Endian::Little, 0));
    assert!(data.deserialize::<Header<'_>>().is_err());

    // Unknown field codes (> UNIX_FDS = 9) are ignored after validating their variant values,
    // including complex container types, 8-byte aligned values, repeated unknown codes, and both
    // byte orders.
    for endian in [Endian::Little, Endian::Big] {
        let base = Message::method_call("/org/zbus/Test", "Ping")
            .interface("org.zbus.Test")
            .sender(":1.42")
            .endian(endian)
            .build(&(42u32, "hello"))
            .unwrap();

        let unknown_values: Vec<(u8, Variant<'_>)> = vec![
            (10, Variant::from(7u8)),
            (10, Variant::from(true)),
            (11, Variant::from(0x0102_0304_0506_0708u64)),
            (12, Variant::from("unknown")),
            (
                13,
                Variant::from(crate::ObjectPath::from_static_str_unchecked(
                    "/org/zbus/Extra",
                )),
            ),
            (14, Variant::from(Signature::try_from("a{sv}").unwrap())),
            (15, Variant::from(vec![1u8, 2, 3, 4, 5])),
            (16, Variant::from((99u32, "nested"))),
            (
                200,
                Variant::from(HashMap::from([
                    ("a", Variant::from(1u32)),
                    ("b", Variant::from(vec!["x", "y"])),
                ])),
            ),
            (
                255,
                Variant::Variant(Box::new(Variant::from(HashMap::from([(
                    "k",
                    Variant::from(true),
                )])))),
            ),
        ];

        // Interleave unknown fields before, between, and after the known fields.
        let bytes = message_with_unknown_fields(&base, endian, &unknown_values);
        assert_same(&bytes);

        let msg = Message::from_parts(
            bytes.clone(),
            #[cfg(unix)]
            vec![],
        )
        .unwrap();
        let hdr = msg.header();
        assert_eq!(hdr.path().unwrap(), "/org/zbus/Test");
        assert_eq!(hdr.interface().unwrap(), "org.zbus.Test");
        assert_eq!(hdr.member().unwrap(), "Ping");
        assert_eq!(hdr.sender().unwrap(), ":1.42");
        assert_eq!(
            msg.body().deserialize::<(u32, String)>().unwrap(),
            (42, "hello".into())
        );

        let parsed = Header::parse(&bytes).unwrap();
        assert_eq!(parsed.path(), hdr.path());
        assert_eq!(parsed.interface(), hdr.interface());
        assert_eq!(parsed.member(), hdr.member());
        assert_eq!(parsed.sender(), hdr.sender());

        #[cfg(feature = "bus-impl")]
        {
            let new_sender = UniqueName::from_static_str_unchecked(":bus.99");
            #[allow(deprecated)]
            let relayed = msg.with_sender(&new_sender).unwrap();
            let reparsed = Message::from_parts(
                relayed.data().to_vec(),
                #[cfg(unix)]
                vec![],
            )
            .unwrap();
            assert_eq!(reparsed.header().sender(), Some(&new_sender));
            assert_eq!(reparsed.header().path(), hdr.path());
            assert_eq!(reparsed.header().member(), hdr.member());
            assert_eq!(
                reparsed.body().deserialize::<(u32, String)>().unwrap(),
                (42, "hello".into())
            );
        }
    }

    // Malformed variants in unknown fields must still be rejected.
    // Truncated variant (missing value byte for `y`).
    let truncated = raw_header(&[10, 1, b'y', 0]);
    assert_eq!(parse(&truncated).unwrap_err(), Error::OutOfBounds);

    // Empty variant signature `""`.
    let empty_sig = raw_header(&[10, 0, 0]);
    assert!(matches!(
        parse(&empty_sig).unwrap_err(),
        Error::SignatureParse(_)
    ));

    // Multi-type variant signature `"yy"`.
    let multi_sig = raw_header(&[10, 2, b'y', b'y', 0, 1, 2]);
    assert!(matches!(
        parse(&multi_sig).unwrap_err(),
        Error::SignatureParse(_)
    ));

    // Invalid signature character `'z'`.
    let bad_sig = raw_header(&[10, 1, b'z', 0, 0]);
    assert!(matches!(
        parse(&bad_sig).unwrap_err(),
        Error::SignatureParse(_)
    ));

    // Signature not terminated with 0.
    let bad_sig_end = raw_header(&[10, 1, b'y', b'x', 1]);
    assert!(
        parse(&bad_sig_end)
            .unwrap_err()
            .to_string()
            .ends_with("expected the 0 that ends a string")
    );

    // Invalid boolean value (2).
    let bad_bool = raw_header(&[10, 1, b'b', 0, 2, 0, 0, 0]);
    assert!(parse(&bad_bool).is_err());

    // Invalid UTF-8 in an unknown field string.
    let bad_utf8 = raw_header(&[10, 1, b's', 0, 1, 0, 0, 0, 0xff, 0]);
    assert!(matches!(parse(&bad_utf8).unwrap_err(), Error::Utf8(_)));

    // Interior NUL byte in an unknown field string.
    let interior_nul = raw_header(&[10, 1, b's', 0, 2, 0, 0, 0, b'a', 0, 0]);
    assert!(parse(&interior_nul).is_err());

    // String not terminated with 0.
    let bad_str_end = raw_header(&[10, 1, b's', 0, 1, 0, 0, 0, b'a', b'x']);
    assert!(
        parse(&bad_str_end)
            .unwrap_err()
            .to_string()
            .ends_with("expected the 0 that ends a string")
    );

    // Invalid object path in an unknown field.
    let bad_path = raw_header(&[10, 1, b'o', 0, 3, 0, 0, 0, b'b', b'a', b'd', 0]);
    assert_eq!(parse(&bad_path).unwrap_err(), Error::InvalidObjectPath);

    // Non-zero padding byte before an 8-byte aligned `u64` (`x`) inside an unknown field.
    // Field starts at offset 16: code(16), sig len 1(17), 'x'(18), nul(19), pad(20..24),
    // u64(24..32).
    let bad_pad = raw_header(&[10, 1, b'x', 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(parse(&bad_pad).unwrap_err(), Error::PaddingNot0(1));

    // File descriptor `h` inside an unknown field with no FDs attached.
    let bad_fd = raw_header(&[10, 1, b'h', 0, 0, 0, 0, 0]);
    #[cfg(unix)]
    assert_eq!(parse(&bad_fd).unwrap_err(), Error::UnknownFd);
    #[cfg(not(unix))]
    assert!(matches!(
        parse(&bad_fd).unwrap_err(),
        Error::SignatureParse(_)
    ));

    // Container nesting depth: `a(yv)` already accounts for 2 containers, so 62 nested variants
    // (total 64) are valid while 63 nested variants (total 65) exceed MAX_DEPTH.
    let make_nested_variants = |variants: usize| {
        let mut field = vec![10];
        for _ in 1..variants {
            field.extend([1, b'v', 0]);
        }
        field.extend([1, b'y', 0, 42]);
        raw_header(&field)
    };
    assert!(parse(&make_nested_variants(62)).is_ok());
    assert!(matches!(
        parse(&make_nested_variants(63)).unwrap_err(),
        Error::MaxDepthExceeded(_)
    ));
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
        // Decoding through serde doesn't check that strings end with a 0, nor that no known field
        // comes twice, nor strict variant checks on unknown fields (object paths, 0-terminated
        // strings, single-complete-type variant signatures).
        (Err(Error::InvalidField), Ok(_)) if !canonical_fields(bytes, endian) => (),
        (Err(e), Ok(_)) if lax_variant_error(&e) => (),
        // Decoding `Header` through serde counts the outer `Header` struct toward container depth
        // (1 more than the D-Bus wire `a(yv)` header).
        (Ok(_), Err(Error::MaxDepthExceeded(_))) => (),
        // Where zbus_utils is built for GVariant as well, as building the whole workspace with all
        // its features has it, the parser takes the maybe type of GVariant in the signature of the
        // body, which decoding through serde rejects.
        (Ok(_), Err(e)) if e == crate::wire::dbus::maybe_not_in_dbus() => (),
        (parsed, decoded) => panic!("{bytes:?}: parsed: {parsed:?}, decoded: {decoded:?}"),
    }
}

/// Whether `e` is of what decoding a header through serde doesn't check for an unknown field's
/// variant: an object path, the 0 that ends a string or signature, and whether a variant's
/// signature is a single complete type.
fn lax_variant_error(e: &Error) -> bool {
    matches!(e, Error::InvalidObjectPath | Error::SignatureParse(_))
        || e.to_string().ends_with("expected the 0 that ends a string")
}

/// Whether the fields of the header in `bytes`, which decodes through serde, end each of their
/// strings and signatures with a 0, none of the known ones coming twice, which decoding through
/// serde doesn't check.
fn canonical_fields(bytes: &[u8], endian: Endian) -> bool {
    use winnow::{Parser, stream::Location, token::take};

    let end = MIN_MESSAGE_SIZE + endian.read_u32(&bytes[MIN_MESSAGE_SIZE - 4..]) as usize;
    let byte = |i: usize| bytes.get(i).copied();
    let mut codes = HashSet::new();
    let mut field = MIN_MESSAGE_SIZE;
    while field < end {
        let Some(code) = byte(field) else {
            return false;
        };
        if code > 9 {
            let mut input = super::Input::new(&bytes[..end]);
            let Ok(_): Result<&[u8], ()> = take::<_, _, ()>(field + 1).parse_next(&mut input)
            else {
                return false;
            };
            let valid = match endian {
                Endian::Little => {
                    super::body::validate_variant::<super::LittleEndian>(&mut input, 2, 0)
                }
                Endian::Big => super::body::validate_variant::<super::BigEndian>(&mut input, 2, 0),
            };
            if valid.is_err() {
                return false;
            }
            field = input.current_token_start().next_multiple_of(8);
            continue;
        }
        // A known field starts with its code, and the signature of its value: 1 type, and a 0. Its
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
        if !codes.insert(code) || !ends_with_0 {
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

/// Return a copy of `msg`'s wire bytes with `unknown_fields` inserted before, inside, and after
/// its existing header fields.
fn message_with_unknown_fields(
    msg: &Message,
    endian: Endian,
    unknown_fields: &[(u8, Variant<'_>)],
) -> Vec<u8> {
    let orig = msg.data().bytes();
    let orig_fields_len = endian.read_u32(&orig[MIN_MESSAGE_SIZE - 4..MIN_MESSAGE_SIZE]) as usize;
    let orig_fields = &orig[MIN_MESSAGE_SIZE..MIN_MESSAGE_SIZE + orig_fields_len];
    let orig_body_start = (MIN_MESSAGE_SIZE + orig_fields_len).next_multiple_of(8);
    let orig_body = &orig[orig_body_start..];

    let append_unknown = |buf: &mut Vec<u8>, code: u8, value: &Variant<'_>| {
        let pad = buf.len().wrapping_neg() % 8;
        buf.resize(buf.len() + pad, 0);
        let ctxt = Context::new(endian, MIN_MESSAGE_SIZE + buf.len());
        let encoded = to_bytes(ctxt, &(code, value)).unwrap();
        buf.extend_from_slice(encoded.bytes());
    };

    let mut new_fields = Vec::new();
    let (first, rest) = unknown_fields.split_at(unknown_fields.len().min(1));
    for (code, value) in first {
        append_unknown(&mut new_fields, *code, value);
    }
    if !orig_fields.is_empty() {
        let pad = new_fields.len().wrapping_neg() % 8;
        new_fields.resize(new_fields.len() + pad, 0);
        new_fields.extend_from_slice(orig_fields);
    }
    for (code, value) in rest {
        append_unknown(&mut new_fields, *code, value);
    }

    let mut out = orig[..MIN_MESSAGE_SIZE - 4].to_vec();
    let mut len_bytes = [0u8; 4];
    endian.write_u32(&mut len_bytes, u32::try_from(new_fields.len()).unwrap());
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&new_fields);
    let header_pad = out.len().wrapping_neg() % 8;
    out.resize(out.len() + header_pad, 0);
    out.extend_from_slice(orig_body);

    out
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

        // A message with unknown header fields (including repeated codes and containers).
        let with_unknown = message_with_unknown_fields(
            &method_return,
            endian,
            &[
                (10, Variant::from((7u32, "extra"))),
                (10, Variant::from(vec![1u8, 2, 3])),
                (255, Variant::from(0x0102_0304_0506_0708u64)),
            ],
        );
        headers.push(with_unknown);

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
