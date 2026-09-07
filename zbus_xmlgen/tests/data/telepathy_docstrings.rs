#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(signature = "s", crate = "zbus::wire")]
pub enum PlaylistOrdering {
    /// Alphabetical ordering by name, ascending.
    Alphabetical,
    /// A user-defined ordering.
    #[serde(rename = "User")]
    #[zvariant(rename = "User")]
    UserDefined,
}

/// A repeat / loop status.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde_repr::Deserialize_repr,
    serde_repr::Serialize_repr,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(crate = "zbus::wire")]
#[repr(u32)]
pub enum LoopStatus {
    /// The playback will stop when there are no more tracks to play.
    None = 0,
    /// The current track will start again from the beginning once it has finished playing.
    Track = 1,
    /// The playback loops through a list of tracks.
    Playlist = 2,
}

/// A data structure describing a playlist.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(crate = "zbus::wire")]
pub struct Playlist {
    /// A unique identifier for the playlist.
    pub id: PlaylistId,
    /// The name of the playlist, typically given by the user.
    pub name: String,
    /// The URI of an (optional) icon.
    pub icon: String,
}

/// A data structure describing a playlist, or nothing.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(crate = "zbus::wire")]
pub struct MaybePlaylist {
    /// Whether this structure refers to a valid playlist.
    pub valid: bool,
    /// The playlist, providing Valid is true.
    pub playlist: Playlist,
}

/// A mapping from metadata attribute names to values.
pub type MetadataMap = std::collections::HashMap<String, zbus::OwnedValue>;

/// Unique playlist identifier.
pub type PlaylistId = zbus::OwnedObjectPath;

/// Provides access to the media player's playlists.
///
/// Since D-Bus does not directly support enums, or a **maybe** type, they are described in this interface.
#[proxy(interface = "com.example.Playlists", assume_defaults = true)]
pub trait Playlists {
    /// ActivatePlaylist method
    ///
    /// Starts playing the given playlist.
    ///
    /// It is up to the media player whether this completely replaces the current tracklist, or whether it is merely inserted into the tracklist and the first track starts.
    ///
    /// # Arguments
    ///
    /// * `playlist_id` - The id of the playlist to activate.
    fn activate_playlist(&self, playlist_id: &PlaylistId) -> zbus::Result<()>;

    /// GetPlaylists method
    ///
    /// Gets a set of playlists.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the first playlist to be fetched (according to the ordering).
    /// * `max_count` - The maximum number of playlists to fetch.
    /// * `order` - The ordering that should be used.
    /// * `reverse_order` - Whether the order should be reversed.
    /// * `playlists` - A list of (at most *max_count*) playlists.
    fn get_playlists(
        &self,
        index: u32,
        max_count: u32,
        order: PlaylistOrdering,
        reverse_order: bool,
    ) -> zbus::Result<Vec<Playlist>>;

    /// GetMetadata method
    ///
    /// Gets the metadata of a playlist.
    ///
    /// # Arguments
    ///
    /// * `mismatch` - A tp:type not matching the signature falls back to the plain type.
    /// * `metadata` - The metadata of the playlist.
    fn get_metadata(&self, mismatch: &str) -> zbus::Result<MetadataMap>;

    /// PlaylistChanged signal
    ///
    /// Indicates that either the Name or Icon attribute of a playlist has changed.
    ///
    /// Client implementations should be aware that this signal may not be implemented.
    ///
    /// Without this signal, media players have no way to notify clients of a change in the attributes of a playlist.
    ///
    /// # Arguments
    ///
    /// * `playlist` - The playlist which details have changed.
    #[zbus(signal)]
    fn playlist_changed(&self, playlist: Playlist) -> zbus::Result<()>;

    /// Orderings property
    ///
    /// The available orderings. At least one must be offered.
    ///
    /// Media players may not have access to all the data required for some orderings.
    #[zbus(property)]
    fn orderings(&self) -> zbus::Result<Vec<PlaylistOrdering>>;

    /// ActivePlaylist property
    ///
    /// The currently-active playlist.
    ///
    /// If there is no currently-active playlist, the structure's Valid field will be false, and the Playlist details are undefined.
    #[zbus(property)]
    fn active_playlist(&self) -> zbus::Result<MaybePlaylist>;

    /// LoopStatus property
    ///
    /// The current loop / repeat status.
    #[zbus(property)]
    fn loop_status(&self) -> zbus::Result<LoopStatus>;
    #[zbus(property)]
    fn set_loop_status(&self, value: LoopStatus) -> zbus::Result<()>;

    /// PlaylistCount property
    ///
    /// The number of playlists available.
    #[zbus(property)]
    fn playlist_count(&self) -> zbus::Result<u32>;

    /// Undocumented property
    #[zbus(property)]
    fn undocumented(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn set_undocumented(&self, value: &str) -> zbus::Result<()>;
}
