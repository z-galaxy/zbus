use serde::{Deserialize, Serialize};
#[cfg(feature = "bus-impl")]
use std::collections::BTreeMap;
use std::{collections::HashMap, hint::black_box, vec};
use zbus::Message;

use criterion::{Criterion, criterion_group, criterion_main};

use zbus::{Type, Variant};

fn msg_ser(c: &mut Criterion) {
    let mut g = c.benchmark_group("message-ser");
    g.bench_function("small", |b| {
        b.iter(|| {
            let msg = empty_message();
            black_box(msg);
        })
    });

    let mut strings = Vec::new();
    let big_boy = BigBoy::new(&mut strings);
    g.bench_function("big", |b| {
        b.iter(|| {
            let msg = big_boy_message(&big_boy);
            black_box(msg);
        })
    });
}

fn msg_de(c: &mut Criterion) {
    let mut g = c.benchmark_group("message-de");
    let msg = empty_message();
    g.bench_function("header", |b| {
        b.iter(|| {
            let header = msg.header();
            black_box(header);
            let body = msg.body();
            let signature = body.signature();
            black_box(signature);
        })
    });

    // A method call as a bus delivers it, with the name of its sender.
    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .destination("org.freedesktop.DBus.Something")
        .interface("org.freedesktop.DBus.Something")
        .sender(":1.42")
        .build(&(42u32, "Testtest"))
        .unwrap();
    let data = msg.data().clone();
    g.bench_function("received", |b| {
        b.iter(|| {
            // SAFETY: The bytes are those of a message that zbus built.
            let msg = unsafe { Message::from_bytes(black_box(data.clone())) }.unwrap();
            black_box(msg);
        })
    });

    let mut strings = Vec::new();
    let big_boy = BigBoy::new(&mut strings);
    let msg = big_boy_message(&big_boy);
    g.bench_function("body", |b| {
        b.iter(|| {
            let body = msg.body();
            let signature = body.signature();
            black_box(signature);
            let body: BigBoy<'_> = body.deserialize().unwrap();
            black_box(body);
        })
    });
}

/// Checking bodies as a bus does before passing them on.
#[cfg(feature = "bus-impl")]
fn msg_validate(c: &mut Criterion) {
    let mut g = c.benchmark_group("message-validate");
    // A method call as a bus delivers it, with the name of its sender.
    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .destination("org.freedesktop.DBus.Something")
        .interface("org.freedesktop.DBus.Something")
        .sender(":1.42")
        .build(&(42u32, "Testtest"))
        .unwrap();
    g.bench_function("small", |b| {
        b.iter(|| black_box(&msg).body().validate().unwrap())
    });

    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .build(&vec![42u8; 16 * 1024])
        .unwrap();
    g.bench_function("bytes", |b| {
        b.iter(|| black_box(&msg).body().validate().unwrap())
    });

    let mut strings = Vec::new();
    let big_boy = BigBoy::new(&mut strings);
    let msg = big_boy_message(&big_boy);
    g.bench_function("big", |b| {
        b.iter(|| black_box(&msg).body().validate().unwrap())
    });

    // The properties of an object, as `GetManagedObjects` and `InterfacesAdded` carry them: 100
    // variants, each with a signature of its own to check before its value can be. busd's `dicts`
    // benchmarks make their method calls with this body.
    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .build(&object_properties())
        .unwrap();
    g.bench_function("dicts", |b| {
        b.iter(|| black_box(&msg).body().validate().unwrap())
    });
}

/// The properties of an object, by the name of their interface, `a{sa{sv}}`: 10 interfaces of 10
/// properties each, whose values cycle through a number, a string, a boolean and an array of
/// strings, as the properties of real objects mix them.
#[cfg(feature = "bus-impl")]
fn object_properties() -> BTreeMap<String, BTreeMap<String, Variant<'static>>> {
    (0..10u32)
        .map(|i| {
            let properties = (0..10u32)
                .map(|j| {
                    let value: Variant<'static> = match j % 4 {
                        0 => (i * 10 + j).into(),
                        1 => format!("value {i}.{j}").into(),
                        2 => (i < 5).into(),
                        _ => (0..3)
                            .map(|k| format!("item {i}.{j}.{k}"))
                            .collect::<Vec<_>>()
                            .into(),
                    };

                    (format!("Property{j}"), value)
                })
                .collect();

            (format!("org.dbus2.benchmark.Interface{i}"), properties)
        })
        .collect()
}

/// Setting the sender of messages, as a bus does to those of peers that leave it out.
#[cfg(feature = "bus-impl")]
fn msg_with_sender(c: &mut Criterion) {
    let mut g = c.benchmark_group("message-with-sender");
    let sender = zbus::names::UniqueName::from_static_str_unchecked(":busd.42");
    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .destination("org.freedesktop.DBus.Something")
        .interface("org.freedesktop.DBus.Something")
        .build(&(42u32, "Testtest"))
        .unwrap();
    g.bench_function("small", |b| {
        b.iter(|| black_box(&msg).with_sender(&sender).unwrap())
    });

    let msg = Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .build(&vec![42u8; 16 * 1024])
        .unwrap();
    g.bench_function("bytes", |b| {
        b.iter(|| black_box(&msg).with_sender(&sender).unwrap())
    });
}

fn empty_message() -> Message {
    Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .destination("org.freedesktop.DBus.Something")
        .interface("org.freedesktop.DBus.Something")
        .build(&())
        .unwrap()
}

fn big_boy_message(big_boy: &BigBoy<'_>) -> Message {
    Message::method_call("/org/freedesktop/DBus/Something", "Ping")
        .destination("org.freedesktop.DBus.Something")
        .interface("org.freedesktop.DBus.Something")
        .build(&big_boy)
        .unwrap()
}

#[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
struct BigBoy<'s> {
    string1: &'s str,
    int1: u64,
    field: (u64, &'s str),
    int_array: Vec<u64>,
    string_array: Vec<&'s str>,
    asv_dict: HashMap<&'s str, Variant<'s>>,
}

impl<'s> BigBoy<'s> {
    fn new(strings: &'s mut Vec<String>) -> Self {
        let mut asv_dict = HashMap::new();
        let int_array = vec![0u64; 1024 * 10];
        let mut string_array: Vec<&str> = Vec::new();
        for idx in 0..1024 * 10 {
            strings.push(format!(
                "{idx}{idx}{idx}{idx}{idx}{idx}{idx}{idx}{idx}{idx}{idx}{idx}"
            ));
        }
        for s in strings {
            string_array.push(s.as_str());
            asv_dict.insert(s.as_str(), Variant::from(s.as_str()));
        }

        BigBoy {
            string1: "Testtest",
            int1: 0xFFFFFFFFFFFFFFFFu64,
            field: (0xFFFFFFFFFFFFFFFFu64, "TesttestTestest"),
            int_array,
            string_array,
            asv_dict,
        }
    }
}

fn address_parse(c: &mut Criterion) {
    const UNIX_ADDRESS: &str = "unix:path=/tmp/dbus/long/loooooooong/path/to/socket\
        /so/we/need/to/keeeeeeeeeep/going/where/no/man/has/gone/before,runtime=yes,\
        guid=0123456789ABCDEF0123456789ABCDEF";
    const TCP_ADDRESS: &str = "tcp:host=some.looong.name.so.we.must.keep.going.on.and.on.\
        on,port=1234,family=ipv4,guid=0123456789ABCDEF0123456789ABCDEF";

    let mut group = c.benchmark_group("parse_dbus_address");
    group.bench_function("unix", |b| {
        b.iter(|| {
            zbus::Address::try_from(black_box(UNIX_ADDRESS)).unwrap();
        })
    });

    group.bench_function("tcp", |b| {
        b.iter(|| {
            zbus::Address::try_from(black_box(TCP_ADDRESS)).unwrap();
        })
    });
}

#[cfg(not(feature = "bus-impl"))]
criterion_group!(benches, msg_ser, msg_de, address_parse);
#[cfg(feature = "bus-impl")]
criterion_group!(
    benches,
    msg_ser,
    msg_de,
    msg_validate,
    msg_with_sender,
    address_parse
);
criterion_main!(benches);
