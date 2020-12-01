class RevisionMap:
    def __init__(self):
        self._down_revisions = {
            "a": ("b", "0"),
            "c": ("a",),
            "d": ("c",),
            "e": ("c",),
            "f": ("e",),
            "g": ("f", "d"),
            "h": ("g",),
            "i": ("g",),
        }

    def down_revisions(self, node):
        if node not in self._down_revisions:
            return tuple()
        return self._down_revisions[node]

    def up_revisions(self, node):
        for up, down in self._down_revisions.items():
            if node in down:
                yield up

    def dependencies(self, nodes, visited):
        for node in nodes:
            if node in visited:
                continue
            yield node
            visited.add(node)
            yield from self.dependencies(self.down_revisions(node), visited)

    def depends_on(self, nodes, visited):
        for node in nodes:
            if node in visited:
                continue
            yield node
            visited.add(node)
            yield from self.depends_on(self.up_revisions(node), visited)

    def topological_sort(self, nodes, reverse=False):
        nodes = list(nodes)
        result = list(
            self._topological_sort_inner(nodes, frozenset(nodes), set())
        )
        if reverse:
            return list(reversed(result))
        return result

    def _topological_sort_inner(self, nodes, target, visited):
        for node in sorted(nodes):
            yield from self._topological_sort_inner(
                self.down_revisions(node), target, visited
            )
            if node not in target:
                continue
            if node in visited:
                continue
            yield node
            visited.add(node)


if __name__ == "__main__":

    # Notes:
    #
    # * Include dependencies (beyond down_revisions) in these traversal algorithms.
    # * Not used in determining 'up' and 'down' for drop/new.
    #

    revision_map = RevisionMap()  # From scripts
    current_heads = {"e"}  # From database
    new_heads = {"i", "g"}  # From API call + current_heads

    # Upgrade process: add minimal nodes in topo
    current_nodes = frozenset(
        revision_map.dependencies(current_heads, visited=set())
    )
    new_nodes = revision_map.dependencies(
        new_heads, visited=set(current_nodes)
    )
    print(f"{current_nodes=}")
    print(f"{revision_map.topological_sort(new_nodes)=}")

    revision_map = RevisionMap()
    current_heads = {"i", "g"}
    drop_revisions = {"f"}

    # Downgrade process: remove all nodes dependent on drop_revisions (inclusive)
    current_nodes = frozenset(
        revision_map.dependencies(current_heads, visited=set())
    )
    drop_nodes = current_nodes.intersection(
        set(revision_map.depends_on(drop_revisions, visited=set()))
    )
    print(f"{current_nodes=}")
    print(f"{revision_map.topological_sort(drop_nodes, reverse=True)=}")
