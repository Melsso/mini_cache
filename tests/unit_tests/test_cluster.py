from mini_cache.cluster import ConsistentHashRing


def test_get_node_is_deterministic():
    ring = ConsistentHashRing(["a", "b", "c"])
    node1 = ring.get_node("mykey")
    node2 = ring.get_node("mykey")
    assert node1 == node2


def test_get_node_returns_only_known_nodes():
    ring = ConsistentHashRing(["a", "b", "c"])
    for i in range(200):
        assert ring.get_node(f"key{i}") in {"a", "b", "c"}


def test_single_node_gets_everything():
    ring = ConsistentHashRing(["only"])
    for i in range(50):
        assert ring.get_node(f"key{i}") == "only"


def test_no_nodes_raises():
    ring = ConsistentHashRing([])
    try:
        ring.get_node("foo")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_keys_distribute_across_all_nodes():
    ring = ConsistentHashRing(["a", "b", "c"], virtual_nodes=150)
    counts = {"a": 0, "b": 0, "c": 0}
    for i in range(3000):
        counts[ring.get_node(f"key{i}")] += 1
    for node in counts:
        assert counts[node] > 500


def test_removing_node_only_remaps_its_keys():
    ring = ConsistentHashRing(["a", "b", "c"], virtual_nodes=150)
    before = {f"key{i}": ring.get_node(f"key{i}") for i in range(1000)}

    ring.remove_node("c")

    moved = 0
    for key, old_node in before.items():
        new_node = ring.get_node(key)
        if new_node != old_node:
            moved += 1
            assert old_node == "c"
    assert moved > 0
    assert "c" not in ring.nodes()


def test_adding_node_only_steals_a_fraction_of_keys():
    ring = ConsistentHashRing(["a", "b"], virtual_nodes=150)
    before = {f"key{i}": ring.get_node(f"key{i}") for i in range(1000)}

    ring.add_node("c")

    moved = sum(1 for key, old_node in before.items() if ring.get_node(key) != old_node)
    assert 0 < moved < 1000


def test_nodes_returns_all_added_nodes():
    ring = ConsistentHashRing(["a", "b", "c"])
    assert ring.nodes() == {"a", "b", "c"}
