"""Unit tests for the task registry."""
import pytest

from taskq import registry


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch):
    # The registry is module-level global state. Give each test its own so
    # registrations don't leak between tests.
    monkeypatch.setattr(registry, "_REGISTRY", {})


def test_bare_decorator_registers_under_function_name():
    @registry.task
    def my_task():
        return "ran"

    assert registry.get_handler("my_task")() == "ran"


def test_named_decorator_registers_under_custom_name():
    @registry.task(name="emails.welcome")
    def welcome():
        return "sent"

    assert registry.get_handler("emails.welcome")() == "sent"


def test_decorator_returns_the_original_function():
    # The decorated function must stay directly callable, so tasks can be
    # unit-tested and reused as plain functions.
    @registry.task
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


def test_duplicate_name_raises():
    # Silently overwriting would mean the WRONG function runs for a job -
    # a brutal bug to trace, so it must fail loudly at import time.
    @registry.task(name="dupe")
    def first():
        pass

    with pytest.raises(ValueError, match="already registered"):
        @registry.task(name="dupe")
        def second():
            pass


def test_unknown_task_raises_with_a_helpful_message():
    with pytest.raises(KeyError, match="no task registered"):
        registry.get_handler("does_not_exist")


def test_registered_names_are_sorted():
    for name in ("charlie", "alpha", "bravo"):
        registry.task(name=name)(lambda: None)
    assert registry.registered_names() == ["alpha", "bravo", "charlie"]
