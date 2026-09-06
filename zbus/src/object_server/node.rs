//! The object server API.

use std::{
    collections::{BTreeMap, HashMap, btree_map, hash_map},
    fmt::Write,
};

#[cfg(feature = "object-manager")]
use crate::{
    Connection, ObjectServer, OwnedValue,
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
        let mut node = self;

        for i in path.split('/').skip(1) {
            if i.is_empty() {
                continue;
            }
            node = node.children.get(i)?;
        }

        Some(node)
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

    pub(crate) fn interface_lock(&self, interface_name: InterfaceName<'_>) -> Option<ArcInterface> {
        self.interfaces.get(&interface_name).cloned()
    }

    pub(super) fn remove_interface(&mut self, interface_name: &InterfaceName<'static>) -> bool {
        self.interfaces.remove(interface_name).is_some()
    }

    /// Remove the node at `path` if it's no longer needed, along with any ancestors that are
    /// thereby no longer needed either.
    ///
    /// A node is still needed if it has children or non-default interfaces (or is the root
    /// node). Returns whether the node at `path` was removed.
    pub(super) fn remove_node(&mut self, path: &ObjectPath<'_>) -> bool {
        let parts = path
            .split('/')
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>();
        if parts.is_empty() {
            return false;
        }

        // First pass: check that the whole path exists and find the deepest ancestor that has to
        // stay: the last one along the path with other children or non-default interfaces of its
        // own. Everything below it only exists to lead to the target node.
        let mut node = &*self;
        let mut keep_depth = 0;
        for (depth, part) in parts.iter().enumerate() {
            let Some(child) = node.children.get(*part) else {
                return false;
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
            return false;
        }

        // Second pass: unlink the target node and the now-useless part of its ancestor chain in
        // one go, by cutting the tree right below the deepest surviving ancestor.
        let mut node = &mut *self;
        for part in &parts[..keep_depth] {
            // The first pass established that the whole path exists.
            node = node.children.get_mut(*part).unwrap();
        }
        let mut disposal = Vec::from_iter(node.children.remove(parts[keep_depth]));
        let removed = !disposal.is_empty();
        // Dispose of the detached subtree iteratively: dropping it in one go would recurse per
        // level, as each node owns its children.
        while let Some(mut node) = disposal.pop() {
            disposal.extend(node.children.drain().map(|(_, child)| child));
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

    async fn introspect_to_writer<W: Write + Send>(&self, writer: &mut W) {
        enum Fragment<'a> {
            /// Represent an unclosed node tree, could be further splitted into sub-`Fragment`s.
            Node {
                name: &'a str,
                node: &'a Node,
                level: usize,
            },
            /// Represent a closing `</node>`.
            End { level: usize },
        }

        let mut stack = Vec::new();
        stack.push(Fragment::Node {
            name: "",
            node: self,
            level: 0,
        });

        // This can be seen as traversing the fragment tree in pre-order DFS with formatted XML
        // fragment, splitted `Fragment::Node`s and `Fragment::End` being current node, left
        // subtree and right leaf respectively.
        while let Some(fragment) = stack.pop() {
            match fragment {
                Fragment::Node { name, node, level } => {
                    stack.push(Fragment::End { level });

                    for (name, node) in &node.children {
                        stack.push(Fragment::Node {
                            name,
                            node,
                            level: level + 2,
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

                    for iface in node.interfaces.values() {
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

    pub(crate) async fn introspect(&self) -> String {
        let mut xml = String::with_capacity(1024);

        self.introspect_to_writer(&mut xml).await;

        xml
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

    #[cfg(feature = "object-manager")]
    pub(crate) async fn get_managed_objects(
        &self,
        object_server: &ObjectServer,
        connection: &Connection,
    ) -> fdo::Result<ManagedObjects> {
        let mut managed_objects = ManagedObjects::new();

        // Recursively get all properties of all interfaces of descendants.
        let mut node_list: Vec<_> = self.children.values().collect();
        while let Some(node) = node_list.pop() {
            let mut interfaces = BTreeMap::new();
            for iface_name in node
                .interfaces
                .keys()
                // The default interfaces and `ObjectManager` itself are not managed.
                .filter(|n| !is_default_interface(n) && **n != ObjectManager::name())
            {
                let props = node
                    .get_properties(object_server, connection, iface_name.clone())
                    .await?;
                interfaces.insert(iface_name.clone().into(), props);
            }
            managed_objects.insert(node.path.clone(), interfaces);
            node_list.extend(node.children.values());
        }

        Ok(managed_objects)
    }

    #[cfg(feature = "object-manager")]
    pub(super) async fn get_properties(
        &self,
        object_server: &ObjectServer,
        connection: &Connection,
        interface_name: InterfaceName<'_>,
    ) -> fdo::Result<HashMap<String, OwnedValue>> {
        let emitter = SignalEmitter::new(connection, self.path.clone())?;
        self.interface_lock(interface_name)
            .expect("Interface was added but not found")
            .instance
            .read()
            .await
            .get_all(object_server, connection, None, &emitter)
            .await
    }
}

/// Whether `name` is one of the default interfaces added to every node on creation.
fn is_default_interface(name: &InterfaceName<'_>) -> bool {
    *name == Peer::name() || *name == Introspectable::name() || *name == Properties::name()
}

#[cfg(test)]
mod tests {
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
                assert!(root.remove_node(&path));
                assert!(root.children.is_empty());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
