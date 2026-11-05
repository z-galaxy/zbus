//! The object server API.

use std::{
    collections::{BTreeMap, HashMap, btree_map, hash_map},
    fmt::Write,
};

#[cfg(feature = "object-manager")]
use crate::{
    Connection, ObjectServer, OwnedVariant,
    fdo::{self, ManagedObjects, ObjectManager},
    object_server::SignalEmitter,
};
use crate::{
    ObjectPath, OwnedObjectPath,
    fdo::{Introspectable, Peer, Properties},
    names::InterfaceName,
};

use super::{ArcInterface, Interface};

#[derive(Default, Debug)]
pub(crate) struct Node {
    path: OwnedObjectPath,
    children: HashMap<String, Node>,
    interfaces: BTreeMap<InterfaceName<'static>, ArcInterface>,
}

/// What is missing for an interface to serve a path: see [`Node::interface_at`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unknown {
    /// There is no object at the path.
    Object,
    /// The object at the path does not have the interface.
    Interface,
}

/// Fallback interfaces by name, as the nodes above a node pass them down to it.
pub(crate) type Fallbacks<'a> = BTreeMap<&'a InterfaceName<'static>, &'a ArcInterface>;

/// A signal that an `ObjectManager` emits about an object it manages.
#[cfg(feature = "object-manager")]
pub(super) enum ManagerSignal {
    /// `InterfacesAdded`, with the interfaces to read the properties of.
    Added {
        manager: OwnedObjectPath,
        path: OwnedObjectPath,
        interfaces: Vec<(InterfaceName<'static>, ArcInterface)>,
    },
    /// `InterfacesRemoved`.
    Removed {
        manager: OwnedObjectPath,
        path: OwnedObjectPath,
        interfaces: Vec<InterfaceName<'static>>,
    },
}

impl Node {
    pub(crate) fn new(path: OwnedObjectPath) -> Self {
        let mut node = Self {
            path,
            ..Default::default()
        };
        // Keep this set in sync with `is_default_interface`.
        assert!(node.add_interface(Peer));
        assert!(node.add_interface(Introspectable));
        assert!(node.add_interface(Properties));

        node
    }

    // Get the child Node at path.
    pub(crate) fn get_child(&self, path: &ObjectPath<'_>) -> Option<&Node> {
        self.walk(path, |_| ())
    }

    /// The interface named `name` that serves the object at `path`.
    ///
    /// That is the interface registered at `path` itself, as a fallback or not, if there is one.
    /// Otherwise, it is the one that the closest node above `path` has registered as a fallback.
    /// A path without a node of its own is an object if a fallback serves it, and it then has the
    /// default interfaces as well.
    pub(crate) fn interface_at(
        &self,
        path: &ObjectPath<'_>,
        name: &InterfaceName<'_>,
    ) -> Result<ArcInterface, Unknown> {
        // Calls are mostly for interfaces registered at the path itself, so look there first and
        // spare them the search for a fallback.
        if let Some(iface) = self
            .get_child(path)
            .and_then(|node| node.interfaces.get(name))
        {
            return Ok(iface.clone());
        }

        let mut fallback = None;
        // Whether any fallback serves `path`, which makes it an object even without a node.
        let mut covered = false;
        let node = self.walk(path, |ancestor| {
            for (fallback_name, iface) in ancestor.fallbacks() {
                covered = true;
                if fallback_name == name {
                    fallback = Some(iface);
                }
            }
        });
        match (fallback, node) {
            (Some(iface), _) => Ok(iface.clone()),
            (None, Some(_)) => Err(Unknown::Interface),
            (None, None) if !covered => Err(Unknown::Object),
            // The default interfaces keep no state of their own, so those of any node do.
            (None, None) => self
                .interfaces
                .get(name)
                .filter(|_| is_default_interface(name))
                .cloned()
                .ok_or(Unknown::Interface),
        }
    }

    /// Introspection XML of the object at `path`, or `None` if there is none.
    ///
    /// The XML describes the interfaces that serve the object, the fallbacks of the nodes above
    /// it included, and its child nodes, recursively.
    pub(crate) async fn introspect(&self, path: &ObjectPath<'_>) -> Option<String> {
        let (node, fallbacks) = self.with_fallbacks(path);
        let mut xml = String::with_capacity(1024);
        match node {
            Some(node) => node.introspect_to_writer(&mut xml, fallbacks).await,
            // An object that only fallbacks serve has no children, and the default interfaces,
            // just like a node that nothing has been registered at yet.
            None if !fallbacks.is_empty() => {
                Node::new(path.clone().into())
                    .introspect_to_writer(&mut xml, fallbacks)
                    .await
            }
            None => return None,
        }

        Some(xml)
    }

    /// The node at `path`, if there is one, along with the fallbacks that the nodes above `path`
    /// pass down to it.
    pub(crate) fn with_fallbacks(&self, path: &ObjectPath<'_>) -> (Option<&Node>, Fallbacks<'_>) {
        // The nodes are visited from the root down, so the fallbacks of the closer ones override
        // those of the same name from further up.
        let mut fallbacks = Fallbacks::new();
        let node = self.walk(path, |ancestor| fallbacks.extend(ancestor.fallbacks()));

        (node, fallbacks)
    }

    /// How many elements of `path`, from the first one on, lead to existing nodes.
    pub(super) fn existing_depth(&self, path: &ObjectPath<'_>) -> usize {
        let mut node = self;
        let mut depth = 0;
        for part in path.split('/').filter(|part| !part.is_empty()) {
            let Some(child) = node.children.get(part) else {
                break;
            };
            node = child;
            depth += 1;
        }

        depth
    }

    /// The signals that the `ObjectManager`s emit as the interface named `name` gets registered
    /// at `path`, `existing_depth` being the [`Node::existing_depth`] of `path` beforehand.
    ///
    /// The object at `path` gets the interface, or appears with all its interfaces if it was
    /// created for it, as do the nodes created on the way to it. A fallback also serves the
    /// objects below `path` that neither a closer fallback nor an interface of their own of the
    /// same name serves. `manager` is the path of the closest node above `path` that serves an
    /// `ObjectManager`.
    #[cfg(feature = "object-manager")]
    pub(super) fn added_signals(
        &self,
        path: &ObjectPath<'_>,
        name: &InterfaceName<'static>,
        existing_depth: usize,
        manager: Option<&OwnedObjectPath>,
    ) -> Vec<ManagerSignal> {
        let mut signals = Vec::new();
        let mut depth = 0;
        let mut fallbacks = Fallbacks::new();
        let node = self
            .walk(path, |ancestor| {
                if let Some(manager) = manager.filter(|_| depth > existing_depth) {
                    signals.extend(ManagerSignal::added(
                        manager,
                        &ancestor.path,
                        ancestor.reported(&fallbacks),
                    ));
                }
                fallbacks.extend(ancestor.fallbacks());
                depth += 1;
            })
            .expect("the interface was registered at an existing node");
        let iface = &node.interfaces[name];
        if let Some(manager) = manager {
            let interfaces = if depth > existing_depth {
                node.reported(&fallbacks)
            } else if is_managed(name) {
                vec![(name, iface)]
            } else {
                vec![]
            };
            signals.extend(ManagerSignal::added(manager, &node.path, interfaces));
        }
        if iface.fallback && is_managed(name) {
            node.for_each_served_by_fallback(name, manager, |served, manager| {
                signals.extend(ManagerSignal::added(
                    manager,
                    &served.path,
                    vec![(name, iface)],
                ));
            });
        }

        signals
    }

    /// The signals that the `ObjectManager`s emit as `removed`, named `name`, gets unregistered
    /// from `path`, after `pruned`, the paths of the nodes removed along with it, are gone.
    ///
    /// The removed nodes disappear with all their interfaces. If the node at `path` stays, it
    /// loses the interface, unless a fallback from above serves it instead. So do the objects
    /// below that the interface served, if it was a fallback. `manager` is the path of the
    /// closest node above `path` that serves an `ObjectManager`.
    #[cfg(feature = "object-manager")]
    pub(super) fn removed_signals(
        &self,
        path: &ObjectPath<'_>,
        name: &InterfaceName<'static>,
        removed: &ArcInterface,
        pruned: &[OwnedObjectPath],
        manager: Option<&OwnedObjectPath>,
    ) -> Vec<ManagerSignal> {
        let mut signals = Vec::new();
        // The removed nodes had no fallbacks of their own, so they inherited these as well.
        let (node, fallbacks) = self.with_fallbacks(path);
        if let Some(manager) = manager {
            for pruned_path in pruned {
                let mut interfaces: Vec<_> = fallbacks
                    .keys()
                    .filter(|fallback| is_managed(fallback))
                    .map(|fallback| (*fallback).clone())
                    .collect();
                if pruned_path.as_str() == path.as_str() && !fallbacks.contains_key(name) {
                    interfaces.push(name.clone());
                }
                signals.extend(ManagerSignal::removed(manager, pruned_path, interfaces));
            }
        }
        let Some(node) = node else {
            return signals;
        };
        let replacement = fallbacks.get(name).filter(|_| is_managed(name));
        if let Some(manager) = manager {
            signals.extend(ManagerSignal::replaced(
                manager,
                &node.path,
                name,
                replacement,
            ));
        }
        if removed.fallback && is_managed(name) {
            node.for_each_served_by_fallback(name, manager, |served, manager| {
                signals.extend(ManagerSignal::replaced(
                    manager,
                    &served.path,
                    name,
                    replacement,
                ));
            });
        }

        signals
    }

    /// Get the child Node at path. Optionally create one if it doesn't exist.
    ///
    /// This also returns the path of the parent node that implements ObjectManager (if any). If
    /// multiple parents implement it (they shouldn't), then the closest one is returned.
    pub(super) fn get_child_mut(
        &mut self,
        path: &ObjectPath<'_>,
        create: bool,
    ) -> (Option<&mut Node>, Option<ObjectPath<'_>>) {
        let mut node = self;
        let mut node_path = String::new();
        let mut obj_manager_path = None;

        for i in path.split('/').skip(1) {
            if i.is_empty() {
                continue;
            }

            if node.serves_object_manager() {
                obj_manager_path = Some((*node.path).clone());
            }

            write!(&mut node_path, "/{i}").unwrap();
            match node.children.entry(i.into()) {
                hash_map::Entry::Vacant(e) => {
                    if create {
                        let path = node_path.as_str().try_into().expect("Invalid Object Path");
                        node = e.insert(Node::new(path));
                    } else {
                        return (None, obj_manager_path);
                    }
                }
                hash_map::Entry::Occupied(e) => node = e.into_mut(),
            }
        }

        (Some(node), obj_manager_path)
    }

    pub(super) fn remove_interface(
        &mut self,
        interface_name: &InterfaceName<'static>,
    ) -> Option<ArcInterface> {
        self.interfaces.remove(interface_name)
    }

    /// Remove the node at `path` if it's no longer needed, along with any ancestors that are
    /// thereby no longer needed either.
    ///
    /// A node is still needed if it has children or non-default interfaces (or is the root
    /// node). Returns the paths of the removed nodes, from the top down, so the node at `path`
    /// comes last if it was removed.
    pub(super) fn remove_node(&mut self, path: &ObjectPath<'_>) -> Vec<OwnedObjectPath> {
        let parts = path
            .split('/')
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>();
        if parts.is_empty() {
            return vec![];
        }

        // First pass: check that the whole path exists and find the deepest ancestor that has to
        // stay: the last one along the path with other children or non-default interfaces of its
        // own. Everything below it only exists to lead to the target node.
        let mut node = &*self;
        let mut keep_depth = 0;
        for (depth, part) in parts.iter().enumerate() {
            let Some(child) = node.children.get(*part) else {
                return vec![];
            };
            if depth + 1 < parts.len()
                && (child.children.len() > 1 || !child.has_default_interfaces_only())
            {
                keep_depth = depth + 1;
            }
            node = child;
        }
        if !node.children.is_empty() || !node.has_default_interfaces_only() {
            // The target node is still needed.
            return vec![];
        }

        // Second pass: unlink the target node and the now-useless part of its ancestor chain in
        // one go, by cutting the tree right below the deepest surviving ancestor.
        let mut node = &mut *self;
        for part in &parts[..keep_depth] {
            // The first pass established that the whole path exists.
            node = node.children.get_mut(*part).unwrap();
        }
        let mut disposal = Vec::from_iter(node.children.remove(parts[keep_depth]));
        // Dispose of the detached subtree iteratively: dropping it in one go would recurse per
        // level, as each node owns its children. As the subtree is the chain of nodes down to the
        // target node, it is disposed of from the top down.
        let mut removed = Vec::with_capacity(parts.len() - keep_depth);
        while let Some(mut node) = disposal.pop() {
            disposal.extend(node.children.drain().map(|(_, child)| child));
            removed.push(node.path);
        }
        removed
    }

    /// Whether the node only has the default interfaces that every node gets on creation.
    ///
    /// Note that this considers `ObjectManager` a non-default interface: it is explicitly
    /// registered by the user, so a node serving one must not be removed behind their back.
    fn has_default_interfaces_only(&self) -> bool {
        self.interfaces.keys().all(is_default_interface)
    }

    /// Whether an interface with the given name is registered at this node.
    pub(super) fn has_interface(&self, name: &InterfaceName<'_>) -> bool {
        self.interfaces.contains_key(name)
    }

    /// The node at `path`, if there is one, after calling `visit` with each node above it, from
    /// this one down.
    ///
    /// If there is no node at `path`, `visit` is called with each node above it that there is.
    fn walk<'a>(
        &'a self,
        path: &ObjectPath<'_>,
        mut visit: impl FnMut(&'a Node),
    ) -> Option<&'a Node> {
        let mut node = self;
        for part in path.split('/').filter(|part| !part.is_empty()) {
            visit(node);
            node = node.children.get(part)?;
        }

        Some(node)
    }

    /// The interfaces registered at this node as fallbacks, by name.
    fn fallbacks(&self) -> impl Iterator<Item = (&InterfaceName<'static>, &ArcInterface)> {
        self.interfaces.iter().filter(|(_, iface)| iface.fallback)
    }

    /// The interfaces that this node reports to an `ObjectManager`, by name, `fallbacks` being
    /// those that it inherits: its own, and the inherited ones it has none of the same name of.
    #[cfg(feature = "object-manager")]
    fn reported<'a>(
        &'a self,
        fallbacks: &Fallbacks<'a>,
    ) -> Vec<(&'a InterfaceName<'static>, &'a ArcInterface)> {
        let inherited = fallbacks
            .iter()
            .filter(|(name, _)| !self.interfaces.contains_key(**name))
            .map(|(name, iface)| (*name, *iface));

        self.interfaces
            .iter()
            .chain(inherited)
            .filter(|(name, _)| is_managed(name))
            .collect()
    }

    /// Call `visit` with each node below this one that this node's fallback named `name` serves,
    /// and that an `ObjectManager` manages, along with the path of the closest node above it that
    /// serves one.
    ///
    /// Those are the nodes that neither a closer fallback nor an interface of their own of that
    /// name serves. `manager` is the path of the closest node above this one that serves an
    /// `ObjectManager`.
    #[cfg(feature = "object-manager")]
    fn for_each_served_by_fallback<'a>(
        &'a self,
        name: &InterfaceName<'_>,
        manager: Option<&'a OwnedObjectPath>,
        mut visit: impl FnMut(&'a Node, &'a OwnedObjectPath),
    ) {
        let manager = self.manager_below(manager);
        let mut nodes: Vec<_> = self
            .children
            .values()
            .map(|child| (child, manager))
            .collect();
        while let Some((node, manager)) = nodes.pop() {
            match node.interfaces.get(name) {
                // A closer fallback serves the node, and those below it.
                Some(iface) if iface.fallback => continue,
                // An interface of its own serves the node, but not those below it.
                Some(_) => (),
                None => {
                    if let Some(manager) = manager {
                        visit(node, manager);
                    }
                }
            }
            let manager = node.manager_below(manager);
            nodes.extend(node.children.values().map(|child| (child, manager)));
        }
    }

    /// The path of the closest `ObjectManager` above the children of this node, `manager` being
    /// the one above this node.
    #[cfg(feature = "object-manager")]
    fn manager_below<'a>(
        &'a self,
        manager: Option<&'a OwnedObjectPath>,
    ) -> Option<&'a OwnedObjectPath> {
        if self.serves_object_manager() {
            Some(&self.path)
        } else {
            manager
        }
    }

    pub(super) fn add_arc_interface(
        &mut self,
        name: InterfaceName<'static>,
        arc_iface: ArcInterface,
    ) -> bool {
        match self.interfaces.entry(name) {
            btree_map::Entry::Vacant(e) => {
                e.insert(arc_iface);
                true
            }
            btree_map::Entry::Occupied(_) => false,
        }
    }

    fn add_interface<I>(&mut self, iface: I) -> bool
    where
        I: Interface,
    {
        self.add_arc_interface(I::name(), ArcInterface::new(iface))
    }

    /// Write the introspection XML of this node to `writer`, `fallbacks` being the fallback
    /// interfaces that the nodes above it pass down to it.
    async fn introspect_to_writer<'a, W: Write + Send>(
        &'a self,
        writer: &mut W,
        fallbacks: Fallbacks<'a>,
    ) {
        enum Fragment<'a> {
            /// Represent an unclosed node tree, could be further splitted into sub-`Fragment`s.
            Node {
                name: &'a str,
                node: &'a Node,
                level: usize,
                /// The index of the fallbacks that the node inherits, in `inheritance`.
                inherits: usize,
            },
            /// Represent a closing `</node>`.
            End { level: usize },
        }

        let mut inheritance = Inheritance::new(fallbacks);
        let mut stack = Vec::new();
        stack.push(Fragment::Node {
            name: "",
            node: self,
            level: 0,
            inherits: 0,
        });

        // This can be seen as traversing the fragment tree in pre-order DFS with formatted XML
        // fragment, splitted `Fragment::Node`s and `Fragment::End` being current node, left
        // subtree and right leaf respectively.
        while let Some(fragment) = stack.pop() {
            match fragment {
                Fragment::Node {
                    name,
                    node,
                    level,
                    inherits,
                } => {
                    stack.push(Fragment::End { level });

                    let passed_down = inheritance.pass_down(node, inherits);
                    for (name, node) in &node.children {
                        stack.push(Fragment::Node {
                            name,
                            node,
                            level: level + 2,
                            inherits: passed_down,
                        })
                    }

                    if level == 0 {
                        writeln!(
                            writer,
                            r#"
<!DOCTYPE node PUBLIC "-//freedesktop//DTD D-BUS Object Introspection 1.0//EN"
 "http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd">
<node>"#
                        )
                        .unwrap();
                    } else {
                        writeln!(
                            writer,
                            "{:indent$}<node name=\"{}\">",
                            "",
                            name,
                            indent = level
                        )
                        .unwrap();
                    }

                    // An inherited fallback doesn't serve a node with an interface of the same
                    // name of its own.
                    let fallbacks = inheritance
                        .get(inherits)
                        .iter()
                        .filter(|(name, _)| !node.interfaces.contains_key(**name))
                        .map(|(_, iface)| *iface);
                    for iface in node.interfaces.values().chain(fallbacks) {
                        iface
                            .instance
                            .read()
                            .await
                            .introspect_to_writer(writer, level + 2);
                    }
                }
                Fragment::End { level } => {
                    writeln!(writer, "{:indent$}</node>", "", indent = level).unwrap();
                }
            }
        }
    }

    /// Whether an `ObjectManager` is registered at this node.
    #[cfg(feature = "object-manager")]
    fn serves_object_manager(&self) -> bool {
        self.interfaces.contains_key(&ObjectManager::name())
    }

    #[cfg(not(feature = "object-manager"))]
    fn serves_object_manager(&self) -> bool {
        false
    }

    /// The objects below this node, with the interfaces that they report to an `ObjectManager`
    /// and their properties, `fallbacks` being the fallbacks that this node inherits.
    #[cfg(feature = "object-manager")]
    pub(crate) async fn get_managed_objects(
        &self,
        fallbacks: Fallbacks<'_>,
        object_server: &ObjectServer,
        connection: &Connection,
    ) -> fdo::Result<ManagedObjects> {
        let mut managed_objects = ManagedObjects::new();
        let mut inheritance = Inheritance::new(fallbacks);
        let passed_down = inheritance.pass_down(self, 0);
        let mut nodes: Vec<_> = self
            .children
            .values()
            .map(|child| (child, passed_down))
            .collect();
        while let Some((node, inherits)) = nodes.pop() {
            let mut interfaces = HashMap::new();
            for (name, iface) in node.reported(inheritance.get(inherits)) {
                let props = properties(iface, &node.path, object_server, connection).await?;
                interfaces.insert(name.clone().into(), props);
            }
            managed_objects.insert(node.path.clone(), interfaces);
            let passed_down = inheritance.pass_down(node, inherits);
            nodes.extend(node.children.values().map(|child| (child, passed_down)));
        }

        Ok(managed_objects)
    }
}

#[cfg(feature = "object-manager")]
impl ManagerSignal {
    /// `InterfacesAdded` for `interfaces` of the object at `path`, unless there are none.
    fn added(
        manager: &OwnedObjectPath,
        path: &OwnedObjectPath,
        interfaces: Vec<(&InterfaceName<'static>, &ArcInterface)>,
    ) -> Option<Self> {
        (!interfaces.is_empty()).then(|| Self::Added {
            manager: manager.clone(),
            path: path.clone(),
            interfaces: interfaces
                .into_iter()
                .map(|(name, iface)| (name.clone(), iface.clone()))
                .collect(),
        })
    }

    /// `InterfacesRemoved` for `interfaces` of the object at `path`, unless there are none.
    fn removed(
        manager: &OwnedObjectPath,
        path: &OwnedObjectPath,
        interfaces: Vec<InterfaceName<'static>>,
    ) -> Option<Self> {
        (!interfaces.is_empty()).then(|| Self::Removed {
            manager: manager.clone(),
            path: path.clone(),
            interfaces,
        })
    }

    /// The signal for the interface named `name` of the object at `path` being served by
    /// `replacement` from now on, or by nothing if it is `None`.
    fn replaced(
        manager: &OwnedObjectPath,
        path: &OwnedObjectPath,
        name: &InterfaceName<'static>,
        replacement: Option<&&ArcInterface>,
    ) -> Option<Self> {
        match replacement {
            Some(iface) => Self::added(manager, path, vec![(name, iface)]),
            None => Self::removed(manager, path, vec![name.clone()]),
        }
    }
}

/// The fallbacks that the nodes of a subtree inherit, shared between the nodes that inherit the
/// same ones.
///
/// The set at index 0 is the one that the top node of the subtree inherits. Each node with
/// fallbacks of its own adds a set for the nodes below it: the one it inherits, with its own
/// fallbacks overriding those of the same name.
struct Inheritance<'a> {
    sets: Vec<Fallbacks<'a>>,
}

impl<'a> Inheritance<'a> {
    fn new(fallbacks: Fallbacks<'a>) -> Self {
        Self {
            sets: vec![fallbacks],
        }
    }

    /// The index of the set that the children of `node` inherit, `inherits` being the index of
    /// the one that `node` inherits.
    fn pass_down(&mut self, node: &'a Node, inherits: usize) -> usize {
        let mut own = node.fallbacks().peekable();
        if own.peek().is_none() {
            return inherits;
        }
        let mut fallbacks = self.sets[inherits].clone();
        fallbacks.extend(own);
        self.sets.push(fallbacks);

        self.sets.len() - 1
    }

    fn get(&self, index: usize) -> &Fallbacks<'a> {
        &self.sets[index]
    }
}

/// The properties of `iface` for the object at `path`.
#[cfg(feature = "object-manager")]
pub(super) async fn properties(
    iface: &ArcInterface,
    path: &ObjectPath<'_>,
    object_server: &ObjectServer,
    connection: &Connection,
) -> fdo::Result<HashMap<String, OwnedVariant>> {
    let emitter = SignalEmitter::new(connection, path.clone())?;
    iface
        .instance
        .read()
        .await
        .get_all(object_server, connection, None, &emitter)
        .await
}

/// Whether `name` is one of the default interfaces added to every node on creation.
fn is_default_interface(name: &InterfaceName<'_>) -> bool {
    *name == Peer::name() || *name == Introspectable::name() || *name == Properties::name()
}

/// Whether an `ObjectManager` reports the interfaces named `name`: it leaves out the default
/// interfaces, and itself.
#[cfg(feature = "object-manager")]
fn is_managed(name: &InterfaceName<'_>) -> bool {
    !is_default_interface(name) && *name != ObjectManager::name()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn new_node_only_has_the_default_interfaces() {
        // Guards the coupling between `Node::new` and `is_default_interface`: a default
        // interface registered by one but unknown to the other would silently break pruning.
        let node = Node::new("/".try_into().unwrap());
        assert!(node.has_default_interfaces_only());
    }

    #[test]
    fn deep_path_removal_needs_no_deep_stack() {
        // Neither the removal walk nor the disposal of the removed nodes may recurse per path
        // component: with this many components on a deliberately small stack, a recursive
        // implementation aborts with a stack overflow.
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let path_str = format!("/{}", ["n"; 2000].join("/"));
                let path = ObjectPath::try_from(path_str.as_str()).unwrap();
                let mut root = Node::new("/".try_into().unwrap());
                root.get_child_mut(&path, true);
                assert_eq!(root.remove_node(&path).len(), 2000);
                assert!(root.children.is_empty());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn fallback_serves_its_path_and_those_below() {
        let mut root = root();
        let fallback = add(&mut root, "/x", Iface, ArcInterface::new_fallback);

        for path in ["/x", "/x/a", "/x/a/b"] {
            assert!(served_by(&root, path, Iface::name(), &fallback), "{path}");
        }
        assert_eq!(unknown(&root, "/y", Iface::name()), Some(Unknown::Object));
        assert_eq!(unknown(&root, "/", Iface::name()), Some(Unknown::Interface));
    }

    #[test]
    fn own_interface_overrides_fallback() {
        let mut root = root();
        let fallback = add(&mut root, "/x", Iface, ArcInterface::new_fallback);
        let own = add(&mut root, "/x/defined", Iface, ArcInterface::new);

        assert!(served_by(&root, "/x/defined", Iface::name(), &own));
        assert!(served_by(&root, "/x/undefined", Iface::name(), &fallback));
        // An interface that isn't a fallback serves none of the paths below its own.
        assert!(served_by(
            &root,
            "/x/defined/below",
            Iface::name(),
            &fallback
        ));
    }

    #[test]
    fn closest_fallback_serves() {
        let mut root = root();
        let outer = add(&mut root, "/x", Iface, ArcInterface::new_fallback);
        let inner = add(&mut root, "/x/a", Iface, ArcInterface::new_fallback);

        assert!(served_by(&root, "/x/a", Iface::name(), &inner));
        assert!(served_by(&root, "/x/a/b", Iface::name(), &inner));
        assert!(served_by(&root, "/x/b", Iface::name(), &outer));
    }

    #[test]
    fn fallback_serves_objects_with_other_interfaces() {
        // Registering an interface at `/x/a/b` creates objects at `/x/a` as well.
        let mut root = root();
        let fallback = add(&mut root, "/x", Iface, ArcInterface::new_fallback);
        let other = add(&mut root, "/x/a/b", Other, ArcInterface::new);

        for path in ["/x/a", "/x/a/b"] {
            assert!(served_by(&root, path, Iface::name(), &fallback), "{path}");
        }
        assert!(served_by(&root, "/x/a/b", Other::name(), &other));
        assert_eq!(
            unknown(&root, "/x/a", Other::name()),
            Some(Unknown::Interface)
        );
    }

    #[test]
    fn non_fallback_serves_no_path_below() {
        let mut root = root();
        add(&mut root, "/x", Iface, ArcInterface::new);
        add(&mut root, "/x/a", Other, ArcInterface::new);

        assert_eq!(
            unknown(&root, "/x/a", Iface::name()),
            Some(Unknown::Interface)
        );
        assert_eq!(unknown(&root, "/x/b", Iface::name()), Some(Unknown::Object));
        assert_eq!(
            unknown(&root, "/x/b", Properties::name()),
            Some(Unknown::Object)
        );
    }

    #[test]
    fn fallback_served_paths_have_the_default_interfaces() {
        let mut root = root();
        add(&mut root, "/x", Iface, ArcInterface::new_fallback);

        for name in [Peer::name(), Introspectable::name(), Properties::name()] {
            assert_eq!(unknown(&root, "/x/a", name.clone()), None, "{name}");
        }
        assert_eq!(
            unknown(&root, "/x/a", Other::name()),
            Some(Unknown::Interface)
        );
        assert_eq!(
            unknown(&root, "/y", Properties::name()),
            Some(Unknown::Object)
        );
    }

    #[test]
    fn root_fallback_serves_all_paths() {
        let mut root = root();
        let fallback = add(&mut root, "/", Iface, ArcInterface::new_fallback);

        for path in ["/", "/x", "/x/a"] {
            assert!(served_by(&root, path, Iface::name(), &fallback), "{path}");
        }
    }

    #[test]
    fn introspection_includes_inherited_fallbacks() {
        let mut root = root();
        add(&mut root, "/x", Iface, ArcInterface::new_fallback);
        add(&mut root, "/x/a/b", Other, ArcInterface::new);
        add(&mut root, "/x/a/b/own", Iface, ArcInterface::new);
        let introspect =
            |path| crate::block_on(root.introspect(&ObjectPath::try_from(path).unwrap()));
        let iface_element = format!("<interface name=\"{}\">", Iface::name());

        // Each object of the subtree lists the interface once, whether it inherits it from `/x`
        // (`/x/a` and `/x/a/b`), has its own (`/x/a/b/own`), or both (`/x`).
        let xml = introspect("/x").unwrap();
        assert_eq!(xml.matches(&iface_element).count(), 4, "{xml}");

        // An object that only the fallback serves has the default interfaces, and no children.
        let xml = introspect("/x/virtual").unwrap();
        assert_eq!(xml.matches(&iface_element).count(), 1, "{xml}");
        for name in [Peer::name(), Introspectable::name(), Properties::name()] {
            assert!(
                xml.contains(&format!("<interface name=\"{name}\">")),
                "{xml}"
            );
        }
        assert!(!xml.contains("<node name="), "{xml}");

        assert_eq!(introspect("/y"), None);
    }

    #[cfg(feature = "object-manager")]
    #[test]
    fn manager_signals_for_objects_below_fallback() {
        let mut root = root();
        add(&mut root, "/m", ObjectManager, ArcInterface::new);

        // The auto-created `/m/x` has no interface to report yet.
        let signals = add_signalled(&mut root, "/m/x/a", Other, ArcInterface::new);
        assert_eq!(signals, [added("/m", "/m/x/a", &[OTHER])]);

        // A fallback is announced for the objects below it, not just its own path.
        let signals = add_signalled(&mut root, "/m/x", Iface, ArcInterface::new_fallback);
        assert_eq!(
            signals,
            [
                added("/m", "/m/x", &[IFACE]),
                added("/m", "/m/x/a", &[IFACE])
            ]
        );

        // New objects come with the fallbacks that they inherit.
        let signals = add_signalled(&mut root, "/m/x/b/c", Other, ArcInterface::new);
        assert_eq!(
            signals,
            [
                added("/m", "/m/x/b", &[IFACE]),
                added("/m", "/m/x/b/c", &[IFACE, OTHER]),
            ]
        );

        // Pruned objects go with all their interfaces.
        let signals = remove_signalled(&mut root, "/m/x/b/c", Other::name());
        assert_eq!(
            signals,
            [
                removed("/m", "/m/x/b", &[IFACE]),
                removed("/m", "/m/x/b/c", &[IFACE, OTHER]),
            ]
        );

        // The objects below a fallback lose it along with its path.
        let signals = remove_signalled(&mut root, "/m/x", Iface::name());
        assert_eq!(
            signals,
            [
                removed("/m", "/m/x", &[IFACE]),
                removed("/m", "/m/x/a", &[IFACE])
            ]
        );
    }

    #[cfg(feature = "object-manager")]
    #[test]
    fn manager_signals_when_another_interface_takes_over() {
        let mut root = root();
        add(&mut root, "/m", ObjectManager, ArcInterface::new);
        let outer = add(&mut root, "/m", Iface, ArcInterface::new_fallback);
        add(&mut root, "/m/x/a", Other, ArcInterface::new);

        // A closer fallback takes over the objects below it...
        let inner = add_signalled(&mut root, "/m/x", Iface, ArcInterface::new_fallback);
        assert_eq!(
            inner,
            [
                added("/m", "/m/x", &[IFACE]),
                added("/m", "/m/x/a", &[IFACE])
            ]
        );

        // ... and hands them back to the outer one when it goes.
        let path = ObjectPath::try_from("/m/x").unwrap();
        let (node, manager) = root.get_child_mut(&path, false);
        let manager = manager.map(OwnedObjectPath::from);
        let removed_iface = node.unwrap().remove_interface(&Iface::name()).unwrap();
        let pruned = root.remove_node(&path);
        let signals = root.removed_signals(
            &path,
            &Iface::name(),
            &removed_iface,
            &pruned,
            manager.as_ref(),
        );
        assert_eq!(
            summary(&signals),
            [
                added("/m", "/m/x", &[IFACE]),
                added("/m", "/m/x/a", &[IFACE])
            ]
        );
        for signal in &signals {
            let ManagerSignal::Added { interfaces, .. } = signal else {
                panic!("not an `InterfacesAdded` signal");
            };
            assert!(Arc::ptr_eq(&interfaces[0].1.instance, &outer.instance));
        }

        // An interface of its own yields the object to the fallback as well, unless the object
        // goes away with it.
        add(&mut root, "/m/x/a", Iface, ArcInterface::new);
        let signals = remove_signalled(&mut root, "/m/x/a", Iface::name());
        assert_eq!(signals, [added("/m", "/m/x/a", &[IFACE])]);
        add(&mut root, "/m/y", Iface, ArcInterface::new);
        let signals = remove_signalled(&mut root, "/m/y", Iface::name());
        assert_eq!(signals, [removed("/m", "/m/y", &[IFACE])]);
    }

    #[cfg(feature = "object-manager")]
    #[test]
    fn manager_signals_come_from_the_closest_manager() {
        let mut root = root();
        add(&mut root, "/m", ObjectManager, ArcInterface::new);
        add(&mut root, "/m/x/n", ObjectManager, ArcInterface::new);
        add(&mut root, "/m/x/n/a", Other, ArcInterface::new);

        let signals = add_signalled(&mut root, "/m/x", Iface, ArcInterface::new_fallback);
        assert_eq!(
            signals,
            [
                added("/m", "/m/x", &[IFACE]),
                added("/m", "/m/x/n", &[IFACE]),
                added("/m/x/n", "/m/x/n/a", &[IFACE]),
            ]
        );
    }

    #[cfg(feature = "object-manager")]
    #[test]
    fn no_manager_signals_without_a_manager() {
        let mut root = root();
        add(&mut root, "/x/a", Other, ArcInterface::new);
        assert!(add_signalled(&mut root, "/x", Iface, ArcInterface::new_fallback).is_empty());
        assert!(remove_signalled(&mut root, "/x/a", Other::name()).is_empty());
    }

    struct Iface;

    #[crate::interface(name = "org.zbus.NodeTest.Iface")]
    impl Iface {
        fn noop(&self) {}
    }

    struct Other;

    #[crate::interface(name = "org.zbus.NodeTest.Other")]
    impl Other {
        fn noop(&self) {}
    }

    fn root() -> Node {
        Node::new("/".try_into().unwrap())
    }

    #[cfg(feature = "object-manager")]
    const IFACE: &str = "org.zbus.NodeTest.Iface";
    #[cfg(feature = "object-manager")]
    const OTHER: &str = "org.zbus.NodeTest.Other";

    /// A signal in comparable form: whether interfaces were added, the paths of the manager and
    /// of the object, and the names of the interfaces, sorted.
    #[cfg(feature = "object-manager")]
    type Summary = (bool, String, String, Vec<String>);

    /// Register `iface` at `path` as `ObjectServer` does, and return the manager signals.
    #[cfg(feature = "object-manager")]
    fn add_signalled<I>(
        root: &mut Node,
        path: &str,
        iface: I,
        wrap: fn(I) -> ArcInterface,
    ) -> Vec<Summary>
    where
        I: Interface,
    {
        let path = ObjectPath::try_from(path).unwrap();
        let existing_depth = root.existing_depth(&path);
        let (node, manager) = root.get_child_mut(&path, true);
        let manager = manager.map(OwnedObjectPath::from);
        assert!(node.unwrap().add_arc_interface(I::name(), wrap(iface)));

        summary(&root.added_signals(&path, &I::name(), existing_depth, manager.as_ref()))
    }

    /// Unregister the interface named `name` from `path` as `ObjectServer` does, and return the
    /// manager signals.
    #[cfg(feature = "object-manager")]
    fn remove_signalled(root: &mut Node, path: &str, name: InterfaceName<'static>) -> Vec<Summary> {
        let path = ObjectPath::try_from(path).unwrap();
        let (node, manager) = root.get_child_mut(&path, false);
        let manager = manager.map(OwnedObjectPath::from);
        let removed = node.unwrap().remove_interface(&name).unwrap();
        let pruned = root.remove_node(&path);

        summary(&root.removed_signals(&path, &name, &removed, &pruned, manager.as_ref()))
    }

    /// `signals` in comparable form, sorted.
    #[cfg(feature = "object-manager")]
    fn summary(signals: &[ManagerSignal]) -> Vec<Summary> {
        let mut summary: Vec<_> = signals
            .iter()
            .map(|signal| {
                let (added, manager, path, mut names) = match signal {
                    ManagerSignal::Added {
                        manager,
                        path,
                        interfaces,
                    } => (
                        true,
                        manager,
                        path,
                        interfaces
                            .iter()
                            .map(|(name, _)| name.to_string())
                            .collect(),
                    ),
                    ManagerSignal::Removed {
                        manager,
                        path,
                        interfaces,
                    } => (
                        false,
                        manager,
                        path,
                        interfaces
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                    ),
                };
                names.sort();
                (added, manager.to_string(), path.to_string(), names)
            })
            .collect();
        summary.sort();

        summary
    }

    #[cfg(feature = "object-manager")]
    fn added(manager: &str, path: &str, names: &[&str]) -> Summary {
        (
            true,
            manager.into(),
            path.into(),
            names.iter().map(ToString::to_string).collect(),
        )
    }

    #[cfg(feature = "object-manager")]
    fn removed(manager: &str, path: &str, names: &[&str]) -> Summary {
        (
            false,
            manager.into(),
            path.into(),
            names.iter().map(ToString::to_string).collect(),
        )
    }

    /// Register `iface` at `path`, as `wrap` wraps it, and return the registration.
    fn add<I>(root: &mut Node, path: &str, iface: I, wrap: fn(I) -> ArcInterface) -> ArcInterface
    where
        I: Interface,
    {
        let path = ObjectPath::try_from(path).unwrap();
        let iface = wrap(iface);
        let (node, _) = root.get_child_mut(&path, true);
        assert!(node.unwrap().add_arc_interface(I::name(), iface.clone()));

        iface
    }

    /// Whether `expected` is the interface named `name` that serves `path`.
    fn served_by(
        root: &Node,
        path: &str,
        name: InterfaceName<'_>,
        expected: &ArcInterface,
    ) -> bool {
        lookup(root, path, name).is_ok_and(|iface| Arc::ptr_eq(&iface.instance, &expected.instance))
    }

    /// What is missing for an interface named `name` to serve `path`, if anything.
    fn unknown(root: &Node, path: &str, name: InterfaceName<'_>) -> Option<Unknown> {
        lookup(root, path, name).err()
    }

    fn lookup(root: &Node, path: &str, name: InterfaceName<'_>) -> Result<ArcInterface, Unknown> {
        root.interface_at(&ObjectPath::try_from(path).unwrap(), &name)
    }
}
