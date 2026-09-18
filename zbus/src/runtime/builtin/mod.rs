//! The runtime zbus brings along, for a connection that is handed no runtime of its own.
//!
//! It is made of a scheduler, which holds the tasks and hands them out to be polled, a reactor,
//! which watches the sockets and keeps the timers, and a seat, which one thread at a time is
//! in to run the two.

mod scheduler;
