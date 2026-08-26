"""Contract check pinning ``tests/_doubles.FakeCoordinator`` against ``Coordinator``.

If ``server.work_item_coordinator.Coordinator`` gains, renames, or
resignatures a required member without a matching update to
``FakeCoordinator``, these tests fail here -- loudly, at collection of a
single small module -- instead of every test migrated onto the shared double
silently getting a fallback or an ``AttributeError`` deep in a handler.
"""

from _doubles import FakeCoordinator, assert_conforms_to_coordinator, conformance_problems

from server.work_item_coordinator import Coordinator, WorkItemCoordinator


def test_fake_coordinator_conforms_to_coordinator_protocol() -> None:
    assert_conforms_to_coordinator(FakeCoordinator(), label="FakeCoordinator()")


def test_real_work_item_coordinator_conforms_to_coordinator_protocol() -> None:
    """The real production class must also satisfy the boundary it defines.

    This is the other half of the contract check: it is not enough for the
    fake to match the Protocol if the Protocol has drifted from what
    ``WorkItemCoordinator`` actually provides.
    """
    assert_conforms_to_coordinator(WorkItemCoordinator(), label="WorkItemCoordinator()")


def test_conformance_problems_reports_a_missing_required_method() -> None:
    class Incomplete:
        registry = None
        router = None
        config = None
        OWNED_CONFIG_FIELDS: frozenset[str] = frozenset()

    problems = conformance_problems(Incomplete())
    assert any("arbitrate" in problem for problem in problems)


def test_coordinator_protocol_has_the_members_fake_coordinator_implements() -> None:
    """Cheap drift sentinel: enumerate the required-method names once here so a
    Protocol member added without touching this test (or ``FakeCoordinator``)
    is visible in the diff, even before the conformance check above runs.
    """
    required_methods = {
        name
        for name, member in vars(Coordinator).items()
        if not name.startswith("_") and callable(member)
    }
    assert required_methods == {
        "arbitrate",
        "dispatch",
        "start_task",
        "submit",
        "retain_late_task",
        "cancel",
        "shutdown",
        "add_worker_clarification",
    }
