#![no_main]

use zbus::{
    Message,
    message::{Header, Type},
    names::UniqueName,
    wire::{
        BE, Endian, LE,
        serialized::{Context, Data},
    },
};

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let endian = match data.first() {
        Some(b'B') => BE,
        _ => LE,
    };
    check(data, endian);

    // The lengths that most inputs give their fields and their body don't match the bytes after
    // them. So each input is also checked with lengths that do, for the fuzzer to get to the fields
    // sooner: once with fields as long as the bytes after them and no body, and once with the
    // fields of the input and a body as long as the bytes after them.
    if let Some(fields_len) = data.len().checked_sub(FIELDS_LEN.end) {
        let mut data = data.to_vec();
        endian.write_u32(&mut data[BODY_LEN], 0);
        endian.write_u32(&mut data[FIELDS_LEN], fields_len.try_into().unwrap());
        check(&data, endian);
    }
    if let Some(fields_len) = data.get(FIELDS_LEN) {
        // The body starts on an 8-byte boundary after the fields.
        let body_start =
            (FIELDS_LEN.end + endian.read_u32(fields_len) as usize).next_multiple_of(8);
        if let Some(body_len) = data.len().checked_sub(body_start) {
            let mut data = data.to_vec();
            endian.write_u32(&mut data[BODY_LEN], body_len.try_into().unwrap());
            check(&data, endian);
        }
    }
});

/// Check that a message decoded from `data` has the header that decoding a `Header` from it through
/// serde gives.
fn check(data: &[u8], endian: Endian) {
    let ctxt = Context::new(endian, 0);
    let received = Message::from_parts(
        data.to_vec(),
        #[cfg(unix)]
        vec![],
    );
    let encoded = Data::new(data, ctxt);
    let decoded = encoded.deserialize::<Header<'_>>();

    // Decoding through serde skips the byte that ends a string without checking that it's there,
    // so it takes a header whose last string ends right where the bytes do, without that byte,
    // while a message is never decoded from bytes that its header runs past.
    if data.len() >= FIELDS_LEN.end {
        let fields_len = endian.read_u32(&data[FIELDS_LEN]);
        if FIELDS_LEN.end + fields_len as usize > data.len() {
            assert!(received.is_err());

            return;
        }
    }

    // The body of a message isn't looked at, except for where it ends.
    let (received, decoded) = match (received, decoded) {
        (Ok(received), Ok((decoded, len))) => {
            assert!(whole(&decoded, len, data, endian));

            (received, decoded)
        }
        (Err(_), Err(_)) => return,
        (Err(e), Ok((decoded, len))) if !whole(&decoded, len, data, endian) || lax(&e) => return,
        // Decoding `Header` through serde counts the outer `Header` struct toward container depth
        // (1 more than the D-Bus wire `a(yv)` header).
        (Ok(_), Err(zbus::Error::MaxDepthExceeded(_))) => return,
        (received, decoded) => panic!(
            "decoded as a message: {:?}, as a header: {:?}",
            received.map(|_| ()),
            decoded.map(|_| ()),
        ),
    };

    let primary = received.primary_header();
    let decoded_primary = decoded.primary();
    assert_eq!(primary.endian_sig(), decoded_primary.endian_sig());
    assert_eq!(primary.msg_type(), decoded_primary.msg_type());
    assert_eq!(primary.flags(), decoded_primary.flags());
    assert_eq!(
        primary.protocol_version(),
        decoded_primary.protocol_version()
    );
    assert_eq!(primary.body_len(), decoded_primary.body_len());
    assert_eq!(primary.serial_num(), decoded_primary.serial_num());

    let header = received.header();
    assert_eq!(header.path(), decoded.path());
    assert_eq!(header.interface(), decoded.interface());
    assert_eq!(header.member(), decoded.member());
    assert_eq!(header.error_name(), decoded.error_name());
    assert_eq!(header.reply_serial(), decoded.reply_serial());
    assert_eq!(header.destination(), decoded.destination());
    assert_eq!(header.sender(), decoded.sender());
    assert_eq!(header.signature(), decoded.signature());
    assert_eq!(header.unix_fds(), decoded.unix_fds());

    // A bus that sets the sender of the message gets the same header but for its sender, which a
    // recipient reads from the bytes of the new message as well. `Message::with_sender` is
    // deprecated, and checked until zbus 7 removes it.
    let sender = UniqueName::from_static_str_unchecked(":busd.42");
    #[allow(deprecated)]
    let new = received.with_sender(&sender).unwrap();
    let reread = Message::from_parts(
        new.data().to_vec(),
        #[cfg(unix)]
        vec![],
    )
    .unwrap();
    for new in [&new, &reread] {
        let new_header = new.header();
        assert_eq!(new_header.sender(), Some(&sender));
        assert_eq!(new_header.path(), header.path());
        assert_eq!(new_header.interface(), header.interface());
        assert_eq!(new_header.member(), header.member());
        assert_eq!(new_header.error_name(), header.error_name());
        assert_eq!(new_header.reply_serial(), header.reply_serial());
        assert_eq!(new_header.destination(), header.destination());
        assert_eq!(new_header.signature(), header.signature());
        assert_eq!(new_header.unix_fds(), header.unix_fds());
        assert_eq!(new.body().data().bytes(), received.body().data().bytes());
    }
}

/// Whether `e` is of what decoding a header through serde doesn't check for an unknown field's
/// variant: an object path, the 0 that ends a string or signature, and whether a variant's
/// signature is a single complete type.
fn lax(e: &zbus::Error) -> bool {
    matches!(
        e,
        zbus::Error::InvalidObjectPath | zbus::Error::SignatureParse(_)
    ) || e.to_string().ends_with("expected the 0 that ends a string")
}

/// Whether a message whose header, `len` bytes long, decodes to `header` from the start of `data`,
/// in byte order `endian`, is whole, as a received message has to be beyond what decoding a header
/// through serde checks of its fields: of the only version of the protocol, with the fields that
/// its type requires, none of the known ones coming twice, with a 0 that ends each string and
/// signature in them, with padding of 0s after its header, with a body that ends where `data` does,
/// and with the file descriptors that its header says it brought, of which `data` has none.
fn whole(header: &Header<'_>, len: usize, data: &[u8], endian: Endian) -> bool {
    let required = match header.message_type() {
        Type::MethodCall => header.path().is_some() && header.member().is_some(),
        Type::MethodReturn => header.reply_serial().is_some(),
        Type::Error => header.error_name().is_some() && header.reply_serial().is_some(),
        Type::Signal => {
            header.path().is_some() && header.interface().is_some() && header.member().is_some()
        }
    };
    let padded = data
        .get(len..len.next_multiple_of(8))
        .is_some_and(|padding| padding.iter().all(|byte| *byte == 0));
    let body_len = header.primary().body_len() as usize;
    let ends = len.next_multiple_of(8).checked_add(body_len) == Some(data.len());
    let fds = !cfg!(unix) || header.unix_fds().unwrap_or(0) == 0;

    header.primary().protocol_version() == 1
        && required
        && canonical_fields(data, endian)
        && padded
        && ends
        && fds
}

/// Whether the known fields of the header at the start of `data`, which decodes through serde, end
/// each of their strings and signatures with a 0, none of them coming twice.
fn canonical_fields(data: &[u8], endian: Endian) -> bool {
    let end = FIELDS_LEN.end + endian.read_u32(&data[FIELDS_LEN]) as usize;
    let byte = |i: usize| data.get(i).copied();
    let mut codes = std::collections::HashSet::new();
    let mut field = FIELDS_LEN.end;
    while field < end {
        let Some(code) = byte(field) else {
            return false;
        };
        if code > 9 {
            let Some(rest) = data.get(field + 1..end) else {
                return false;
            };
            let ctxt = Context::new(endian, field + 1);
            let Ok((_, variant_len)) = Data::new(rest, ctxt)
                .deserialize_for_signature::<_, serde::de::IgnoredAny>(&zbus::Signature::Variant)
            else {
                return false;
            };
            field = (field + 1 + variant_len).next_multiple_of(8);
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
            _ => match data.get(value..value + 4) {
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

/// Where the length of the body lies in a message.
const BODY_LEN: std::ops::Range<usize> = 4..8;

/// Where the length of the header fields lies in a message, which is followed by the fields.
const FIELDS_LEN: std::ops::Range<usize> = 12..16;
