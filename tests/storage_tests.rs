use bytes::Bytes;
use enr_state::{AVLTreeParams, RedbAVLStorage};
use ergo_avltree_rust::authenticated_tree_ops::AuthenticatedTreeOps;
use ergo_avltree_rust::batch_avl_prover::BatchAVLProver;
use ergo_avltree_rust::batch_node::AVLTree;
use ergo_avltree_rust::operation::{KeyValue, Operation};
use ergo_avltree_rust::versioned_avl_storage::VersionedAVLStorage;
use tempfile::tempdir;

const KEY_LEN: usize = 32;

fn params() -> AVLTreeParams {
    AVLTreeParams {
        key_length: KEY_LEN,
        value_length: None,
    }
}

fn make_key(seed: u8) -> Bytes {
    // Keys must be strictly between negative_infinity [0;32] and
    // positive_infinity [0xFF;32].  Byte 0 = 0x01 guarantees that.
    let mut key = vec![0u8; KEY_LEN];
    key[0] = 0x01;
    key[1] = seed;
    Bytes::from(key)
}

fn make_value(seed: u8, len: usize) -> Bytes {
    Bytes::from(vec![seed; len])
}

/// Create a fresh storage + prover pair with an initial empty-tree commit.
fn setup(keep_versions: u32) -> (RedbAVLStorage, BatchAVLProver, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.redb");
    let mut storage = RedbAVLStorage::open(&path, params(), keep_versions).unwrap();

    let resolver = storage.resolver();
    let tree = AVLTree::new(resolver, KEY_LEN, None);
    let mut prover = BatchAVLProver::new(tree, true);

    // Initial commit with one key — can't have an empty-tree commit because
    // digest() is None on a truly empty tree.
    let key = make_key(0);
    let value = make_value(0, 64);
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: key.clone(),
            value: value.clone(),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();

    // Reset the prover's changed-node tracking for the next batch.
    prover.base.tree.reset();
    prover.base.changed_nodes_buffer.clear();
    prover.base.changed_nodes_buffer_to_check.clear();

    (storage, prover, dir)
}

// ── Basic CRUD ────────────────────────────────────────────────────────

#[test]
fn insert_updates_version() {
    let (storage, _, _dir) = setup(10);
    assert!(storage.version().is_some());
}

#[test]
fn insert_and_lookup() {
    let (mut storage, mut prover, _dir) = setup(10);

    let key = make_key(1);
    let value = make_value(1, 128);
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: key.clone(),
            value: value.clone(),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();

    let found = prover.unauthenticated_lookup(&key);
    assert_eq!(found, Some(value));
}

#[test]
fn remove_key() {
    let (mut storage, mut prover, _dir) = setup(10);

    // Insert
    let key = make_key(2);
    let value = make_value(2, 64);
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: key.clone(),
            value: value.clone(),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();
    prover.base.tree.reset();
    prover.base.changed_nodes_buffer.clear();
    prover.base.changed_nodes_buffer_to_check.clear();

    // Remove
    prover
        .perform_one_operation(&Operation::Remove(key.clone()))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();

    let found = prover.unauthenticated_lookup(&key);
    assert_eq!(found, None);
}

#[test]
fn digest_changes_on_update() {
    let (mut storage, mut prover, _dir) = setup(10);
    let d1 = storage.version().unwrap();

    let key = make_key(3);
    let value = make_value(3, 64);
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: key.clone(),
            value: value.clone(),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();

    let d2 = storage.version().unwrap();
    assert_ne!(d1, d2);
}

// ── Rollback ──────────────────────────────────────────────────────────

#[test]
fn rollback_restores_previous_digest() {
    let (mut storage, mut prover, _dir) = setup(10);
    let d1 = storage.version().unwrap();

    // Insert another key → version D2.
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: make_key(10),
            value: make_value(10, 64),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();
    let d2 = storage.version().unwrap();
    assert_ne!(d1, d2);

    // Rollback to D1.
    let (root, height) = storage.rollback(&d1).unwrap();
    assert_eq!(storage.version().unwrap(), d1);

    // Reconstruct the prover with the rolled-back root.
    prover.base.tree.root = Some(root);
    prover.base.tree.height = height;

    // The digest from the prover should match.
    assert_eq!(prover.digest().unwrap(), d1);
}

#[test]
fn rollback_multi_step() {
    let (mut storage, mut prover, _dir) = setup(10);
    let d1 = storage.version().unwrap();

    // Apply several versions.
    let mut digests = vec![d1.clone()];
    for i in 20..25u8 {
        prover.base.tree.reset();
        prover.base.changed_nodes_buffer.clear();
        prover.base.changed_nodes_buffer_to_check.clear();

        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(i),
                value: make_value(i, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
        digests.push(storage.version().unwrap());
    }

    // Rollback to the second version (d1 + one insert).
    let target = &digests[1];
    let (root, height) = storage.rollback(target).unwrap();
    assert_eq!(storage.version().unwrap(), *target);

    prover.base.tree.root = Some(root);
    prover.base.tree.height = height;
    assert_eq!(prover.digest().unwrap(), *target);
}

#[test]
fn rollback_versions_lists_targets() {
    let (mut storage, mut prover, _dir) = setup(10);

    // One version already (from setup).  Add two more.
    for i in 30..32u8 {
        prover.base.tree.reset();
        prover.base.changed_nodes_buffer.clear();
        prover.base.changed_nodes_buffer_to_check.clear();

        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(i),
                value: make_value(i, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
    }

    // rollback_versions should list 2 targets (everything except current).
    let targets: Vec<_> = storage.rollback_versions().collect();
    assert_eq!(targets.len(), 2);
}

// ── keep_versions ─────────────────────────────────────────────────────

#[test]
fn keep_versions_zero_no_undo() {
    let (mut storage, mut prover, _dir) = setup(0);

    // Add a version.
    prover.base.tree.reset();
    prover.base.changed_nodes_buffer.clear();
    prover.base.changed_nodes_buffer_to_check.clear();

    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: make_key(40),
            value: make_value(40, 64),
        }))
        .unwrap();
    storage.update(&mut prover, vec![]).unwrap();

    // No rollback targets.
    let targets: Vec<_> = storage.rollback_versions().collect();
    assert!(targets.is_empty());
}

#[test]
fn keep_versions_prunes_old() {
    let (mut storage, mut prover, _dir) = setup(2);

    let mut digests = vec![storage.version().unwrap()];
    for i in 50..55u8 {
        prover.base.tree.reset();
        prover.base.changed_nodes_buffer.clear();
        prover.base.changed_nodes_buffer_to_check.clear();

        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(i),
                value: make_value(i, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
        digests.push(storage.version().unwrap());
    }

    // With keep_versions=2, only 2 rollback targets available.
    let targets: Vec<_> = storage.rollback_versions().collect();
    assert_eq!(targets.len(), 2);

    // Current = digests[5].  keep_versions=2 retains digests[4] and digests[3].
    assert_eq!(targets[0], digests[4]);
    assert_eq!(targets[1], digests[3]);
}

// ── Snapshot loading ──────────────────────────────────────────────────

#[test]
fn load_snapshot_sets_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.redb");
    let mut storage = RedbAVLStorage::open(&path, params(), 10).unwrap();

    // Build a small tree to extract its nodes.
    let resolver = storage.resolver();
    let tree = AVLTree::new(resolver, KEY_LEN, None);
    let mut prover = BatchAVLProver::new(tree, true);

    let key = make_key(60);
    let value = make_value(60, 64);
    prover
        .perform_one_operation(&Operation::Insert(KeyValue {
            key: key.clone(),
            value: value.clone(),
        }))
        .unwrap();

    let digest = prover.digest().unwrap();
    let root_label = prover.base.tree.label(&prover.top_node());
    let height = prover.base.tree.height;

    // Pack the root node.
    let packed = prover.base.tree.pack(prover.top_node());
    let nodes = vec![(root_label, packed)];

    // Load via snapshot.
    storage
        .load_snapshot(nodes.into_iter(), root_label, height, digest.clone())
        .unwrap();

    assert_eq!(storage.version().unwrap(), digest);
    assert!(storage.get_node(&root_label).unwrap().is_some());
}

// ── Persistence across reopen ─────────────────────────────────────────

#[test]
fn reopen_preserves_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.redb");

    let original_version;
    {
        let mut storage = RedbAVLStorage::open(&path, params(), 10).unwrap();
        let resolver = storage.resolver();
        let tree = AVLTree::new(resolver, KEY_LEN, None);
        let mut prover = BatchAVLProver::new(tree, true);

        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(70),
                value: make_value(70, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
        original_version = storage.version().unwrap();
    }

    // Reopen.
    let storage = RedbAVLStorage::open(&path, params(), 10).unwrap();
    assert_eq!(storage.version().unwrap(), original_version);
}

#[test]
fn reopen_preserves_rollback_chain() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.redb");

    let d1;
    {
        let mut storage = RedbAVLStorage::open(&path, params(), 10).unwrap();
        let resolver = storage.resolver();
        let tree = AVLTree::new(resolver, KEY_LEN, None);
        let mut prover = BatchAVLProver::new(tree, true);

        // Version 1.
        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(80),
                value: make_value(80, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
        d1 = storage.version().unwrap();

        prover.base.tree.reset();
        prover.base.changed_nodes_buffer.clear();
        prover.base.changed_nodes_buffer_to_check.clear();

        // Version 2.
        prover
            .perform_one_operation(&Operation::Insert(KeyValue {
                key: make_key(81),
                value: make_value(81, 64),
            }))
            .unwrap();
        storage.update(&mut prover, vec![]).unwrap();
    }

    // Reopen and verify we can still rollback.
    let mut storage = RedbAVLStorage::open(&path, params(), 10).unwrap();
    let targets: Vec<_> = storage.rollback_versions().collect();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0], d1);

    // Actually rollback.
    let (root, height) = storage.rollback(&d1).unwrap();
    assert_eq!(storage.version().unwrap(), d1);

    // Verify the root unpacks correctly.
    let resolver = storage.resolver();
    let mut tree = AVLTree::new(resolver, KEY_LEN, None);
    tree.root = Some(root);
    tree.height = height;
    let prover = BatchAVLProver::new(tree, false);
    assert_eq!(prover.digest().unwrap(), d1);
}
