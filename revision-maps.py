from dataclasses import dataclass
from typing import Dict, FrozenSet, Generator, Iterable, List, Set

import pytest
from alembic.script.revision import Revision


def direct_dependencies(script: Revision) -> Generator[str, None, None]:
    if script.down_revision is None:
        pass
    elif type(script.down_revision) is str:
        yield script.down_revision
    else:
        yield from script.down_revision
    if script.dependencies is not None:
        if type(script.dependencies) is str:
            yield script.dependencies
        else:
            yield from script.dependencies


def all_dependencies(
    revision_map: Dict[str, Revision], script: Revision
) -> Generator[str, None, None]:
    for dependency in direct_dependencies(script):
        yield dependency
        if dependency is not None:
            yield from all_dependencies(revision_map, revision_map[dependency])


def generate_required_nodes(
    revision_map: Dict[str, Revision], scripts: List[Revision]
) -> Generator[str, None, None]:
    # Improve by tracking visited nodes to avoid repeat dives?
    for script in scripts:
        if script.revision is not None:
            yield script.revision
            yield from all_dependencies(revision_map, script)


def required_nodes(
    revision_map: Dict[str, Revision], scripts: List[Revision]
) -> FrozenSet[str]:
    return frozenset(generate_required_nodes(revision_map, scripts))


def topological_sort(
    revision_map: Dict[str, Revision], revisions: FrozenSet[str]
) -> List[str]:
    # TODO use sqlalchemy.util.topological.sort
    # Presumably someone has thought this one through a bit better than I have!

    # subgraph points from revision to its dependencies
    dependencies = {
        revision: {
            dependency
            for dependency in direct_dependencies(revision_map[revision])
            if dependency in revisions
        }
        for revision in revisions
    }
    # Run topological sort.
    visited = {revision: False for revision in revisions}
    return list(topological_sort_inner(visited, dependencies, revisions))


def topological_sort_inner(
    visited: Dict[str, bool],
    dependencies: Dict[str, Set[str]],
    revisions: Iterable[str],
) -> Generator[str, None, None]:
    """ subgraph points from revision to its dependencies """
    for revision in revisions:
        for dependency in dependencies[revision]:
            yield from topological_sort_inner(
                visited, dependencies, [dependency]
            )
        if not visited[revision]:
            yield revision
            visited[revision] = True


@dataclass
class CheckoutMigration:
    downgrade_order: List[Revision]
    upgrade_order: List[Revision]


def checkout_revisions(
    revision_map: Dict[str, Revision],
    current_heads: List[Revision],
    target_heads: List[Revision],
) -> CheckoutMigration:
    # Current and target states, described by full collections of nodes.
    current = required_nodes(revision_map, current_heads)
    target = required_nodes(revision_map, target_heads)

    # drop-add migration path
    return CheckoutMigration(
        downgrade_order=[
            revision_map[revision]
            for revision in reversed(
                topological_sort(revision_map, current - target)
            )
        ],
        upgrade_order=[
            revision_map[revision]
            for revision in topological_sort(revision_map, target - current)
        ],
    )


@pytest.fixture
def revision_map():
    # Equivalent to ScriptDirectory("<dir>").revision_map._revision_map for a loaded
    # migration directory.
    return {
        "types_v2": Revision("types_v2", "types_v1", dependencies="vestas"),
        "types_v1": Revision("types_v1", None),
        "vestas": Revision("vestas", "types_v1"),
        "49d9d1898bf9": Revision("49d9d1898bf9", ("types_v2", "vestas")),
        "a": Revision("a", None),
        "b": Revision("b", "a"),
        "c": Revision("c", "b", dependencies="f"),
        "d": Revision("d", "b", dependencies="e"),
        "g": Revision("g", "b"),
        "h": Revision("h", "d"),
        "e": Revision("e", "g"),
        "f": Revision("f", "g"),
    }


@pytest.mark.parametrize(
    "db_heads, checkout_targets, expect_downgrade_order, expect_upgrade_order",
    [
        (["types_v2"], ["types_v2"], [], []),
        (["vestas"], ["types_v1"], ["vestas"], []),
        (["types_v2"], ["types_v1"], ["types_v2", "vestas"], []),
        (["types_v1"], ["types_v2"], [], ["vestas", "types_v2"]),
        (["c", "f"], ["d", "e"], ["c", "f"], ["e", "d"]),
    ],
)
def test_things(
    revision_map,
    db_heads,
    checkout_targets,
    expect_downgrade_order,
    expect_upgrade_order,
):
    migration = checkout_revisions(
        revision_map=revision_map,
        current_heads=[
            revision_map[rev] for rev in db_heads
        ],  # From migrations table
        target_heads=[
            revision_map[rev] for rev in checkout_targets
        ],  # From `alembic checkout <r1> <r2> ...`
    )
    assert [
        d.revision for d in migration.downgrade_order
    ] == expect_downgrade_order
    assert [
        d.revision for d in migration.upgrade_order
    ] == expect_upgrade_order
