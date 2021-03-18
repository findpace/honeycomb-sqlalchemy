# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from unittest.mock import Mock, call, patch

import pytest
from honeycomb_sqlalchemy import SqlalchemyListeners
from sqlalchemy.engine import Engine


@pytest.fixture
def beeline():
    with patch("honeycomb_sqlalchemy.beeline") as patched:
        yield patched


@pytest.fixture
def listeners():
    listeners = SqlalchemyListeners()
    with patch.object(listeners, "reset_state"):
        yield listeners


@pytest.fixture
def now():
    with patch("honeycomb_sqlalchemy.datetime") as patched:

        def fix():
            """ Return the current time and set it as the return value
            of the mock.
            """
            now = datetime.now()
            patched.datetime.now.return_value = now
            return now

        yield fix


class TestInstall:
    @pytest.fixture
    def sqlalchemy_listen(self):
        with patch("honeycomb_sqlalchemy.listen") as patched:
            yield patched

    def test_listeners(self, sqlalchemy_listen):
        listeners = SqlalchemyListeners()
        listeners.install()

        assert listeners.installed
        assert sqlalchemy_listen.call_args_list == [
            call(Engine, "before_cursor_execute", listeners.before_cursor_execute),
            call(Engine, "after_cursor_execute", listeners.after_cursor_execute),
            call(Engine, "handle_error", listeners.handle_error),
        ]

    def test_install_is_idempotent(self, sqlalchemy_listen):
        listeners = SqlalchemyListeners()
        listeners.install()

        assert listeners.installed
        assert sqlalchemy_listen.call_count == 3

        listeners.install()
        assert sqlalchemy_listen.call_count == 3


class TestResetState:
    def test_existing_state(self):
        listeners = SqlalchemyListeners()
        listeners.state.span = Mock()
        listeners.state.query_start_time = Mock()

        listeners.reset_state()
        assert listeners.state.span is None
        assert listeners.state.query_start_time is None

    def test_no_state(self):
        listeners = SqlalchemyListeners()

        listeners.reset_state()
        assert listeners.state.span is None
        assert listeners.state.query_start_time is None


class TestBeforeCursorExecute:
    def test_warn_on_overlapping_events(self, beeline, listeners):

        listeners.state.span = Mock()
        listeners.state.query_start_time = Mock()

        args = [Mock() for _ in range(6)]

        with pytest.warns(UserWarning):
            listeners.before_cursor_execute(*args)

        assert not beeline.start_span.called

    @pytest.mark.parametrize("type_", [list, tuple])
    def test_list_and_tuple_parameters(self, type_, beeline, listeners):

        dt = datetime.now()

        statement = Mock()
        parameters = type_(["string", 123, dt])

        args = [Mock(), Mock(), statement, parameters, Mock(), Mock()]
        listeners.before_cursor_execute(*args)

        assert beeline.start_span.call_args_list == [
            call(
                context={
                    "name": "sqlalchemy_query",
                    "type": "db",
                    "db.query": statement,
                    "db.query_args": ["string", 123, dt.isoformat()],
                }
            )
        ]

    def test_dict_parameters(self, beeline, listeners):

        dt = datetime.now()

        statement = Mock()
        parameters = {"foo": "string", "bar": 123, "baz": dt}

        args = [Mock(), Mock(), statement, parameters, Mock(), Mock()]
        listeners.before_cursor_execute(*args)

        assert beeline.start_span.call_args_list == [
            call(
                context={
                    "name": "sqlalchemy_query",
                    "type": "db",
                    "db.query": statement,
                    "db.query_args": ["foo=string", "bar=123", f"baz={dt.isoformat()}"],
                }
            )
        ]

    def test_iterable_dict_values(self, beeline, listeners):
        """ Regression test for https://github.com/honeycombio/beeline-python/issues/159
        """
        dt = datetime.now()

        statement = Mock()
        parameters = {
            "foo": "string",
            "bar": 123,
            "baz": dt,
            "zap": [1, 2, 3],
        }

        args = [Mock(), Mock(), statement, parameters, Mock(), Mock()]
        listeners.before_cursor_execute(*args)

        assert beeline.start_span.call_args_list == [
            call(
                context={
                    "name": "sqlalchemy_query",
                    "type": "db",
                    "db.query": statement,
                    "db.query_args": [
                        "foo=string",
                        "bar=123",
                        f"baz={dt.isoformat()}",
                        "zap=[1, 2, 3]",
                    ],
                }
            )
        ]


class TestAfterCursorExecute:
    def test_context(self, beeline, listeners, now):

        start_time = now() - timedelta(seconds=1)
        listeners.state.query_start_time = start_time

        args = [Mock() for _ in range(6)]
        cursor = args[1]
        listeners.after_cursor_execute(*args)

        assert beeline.add_context.call_args_list == [
            call(
                {
                    "db.duration": 1000,
                    "db.last_insert_id": cursor.lastrowid,
                    "db.rows_affected": cursor.rowcount,
                }
            )
        ]

    def test_no_previous_start(self, beeline, listeners):
        args = [Mock() for _ in range(6)]
        listeners.after_cursor_execute(*args)

        assert not beeline.add_context.called

    def test_close_span(self, beeline, listeners):

        span = Mock()
        listeners.state.span = span

        args = [Mock() for _ in range(6)]
        listeners.after_cursor_execute(*args)

        assert beeline.finish_span.call_args_list == [call(span)]

    def test_no_open_span(self, beeline, listeners):

        args = [Mock() for _ in range(6)]
        listeners.after_cursor_execute(*args)

        assert not beeline.finish_span.called

    def test_reset_state(self, beeline, listeners):

        args = [Mock() for _ in range(6)]
        listeners.after_cursor_execute(*args)

        assert listeners.reset_state.called


class TestHandleError:
    def test_context_field(self, beeline):
        listeners = SqlalchemyListeners()

        context = Mock()
        listeners.handle_error(context)

        assert beeline.internal.stringify_exception.call_args_list == [
            call(context.original_exception)
        ]
        assert beeline.add_context_field.call_args_list == [
            call("db.error", beeline.internal.stringify_exception.return_value)
        ]

    def test_close_span(self, beeline):
        listeners = SqlalchemyListeners()

        span = Mock()
        listeners.state.span = span

        context = Mock()
        listeners.handle_error(context)

        assert beeline.finish_span.call_args_list == [call(span)]
        assert listeners.state.span is None

    def test_no_open_span(self, beeline):
        listeners = SqlalchemyListeners()

        context = Mock()
        listeners.handle_error(context)

        assert not beeline.finish_span.called

    def test_reset_state(self, beeline, listeners):
        context = Mock()
        listeners.handle_error(context)

        assert listeners.reset_state.called
