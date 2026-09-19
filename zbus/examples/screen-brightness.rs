// A simple cmdline app to change the screen brightness on laptops.
//
// NB: It only works on a GNOME (based) system.
//
// Usage is simple. Either pass a '+' as argument or no argument on commandline, and it increases
// the brightness by 5%. Pass '-' for decreasing it by 5%.
//
// The whole program runs inside one `zbus::block_on`: no async runtime to pick, zbus as the one
// dependency.

use zbus::{Connection, Result, proxy};

#[proxy(
    interface = "org.gnome.SettingsDaemon.Power.Screen",
    default_service = "org.gnome.SettingsDaemon.Power",
    default_path = "/org/gnome/SettingsDaemon/Power"
)]
trait Screen {
    fn step_up(&self) -> Result<(i32, String)>;
    fn step_down(&self) -> Result<(i32, String)>;
}

fn main() -> Result<()> {
    let step_up = match std::env::args().nth(1) {
        Some(s) => match s.as_str() {
            "+" => true,
            "-" => false,
            _ => panic!("Expected either '+' or '-' argument. Got: {s}"),
        },
        None => true,
    };

    zbus::block_on(async {
        let connection = Connection::session().await?;
        let screen = ScreenProxy::new(&connection).await?;

        let (percent, _) = if step_up {
            screen.step_up().await?
        } else {
            screen.step_down().await?
        };
        println!("New level: {percent}%");

        Ok(())
    })
}
