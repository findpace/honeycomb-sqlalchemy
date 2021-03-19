# -*- coding: utf-8 -*-
import datetime
import logging
import threading
import warnings

import beeline
from sqlalchemy import event
from sqlalchemy.engine import Engine


log = logging.getLogger(__name__)


class SqlalchemyListeners(object):
    def __init__(self):
        self.state = threading.local()
        self.reset_state()

        self.installed = False

    def install(self):
        if self.installed:
            log.info("sqlalchemy listeners already installed, ignoring")
            return

        self.installed = True

        event.listen(Engine, "before_cursor_execute", self.before_cursor_execute)
        event.listen(Engine, "after_cursor_execute", self.after_cursor_execute)
        event.listen(Engine, "handle_error", self.handle_error)

    def uninstall(self):

        event.remove(Engine, "before_cursor_execute", self.before_cursor_execute)
        event.remove(Engine, "after_cursor_execute", self.after_cursor_execute)
        event.remove(Engine, "handle_error", self.handle_error)

        self.installed = False

    def reset_state(self):
        self.state.span = None
        self.state.query_start_time = None

    def before_cursor_execute(
        self, conn, cursor, statement, parameters, context, executemany
    ):
        span = getattr(self.state, "span", None)
        query_start_time = getattr(self.state, "query_start_time", None)

        if span or query_start_time:
            warnings.warn(
                "The before_cursor_execute event fired multiple times inside the same "
                "thread, without a corresponding after_cursor_execute or handle_error "
                "event."
            )
            return

        params = []

        # the type of parameters passed in varies depending on DB.
        # handle list, dict, and tuple
        if type(parameters) == tuple or type(parameters) == list:
            for param in parameters:
                if type(param) == datetime.datetime:
                    param = param.isoformat()
                params.append(param)
        elif type(parameters) == dict:
            for k, v in parameters.items():
                param = "%s=" % k
                if type(v) == datetime.datetime:
                    v = v.isoformat()
                param += str(v)
                params.append(param)

        self.state.span = beeline.start_span(
            context={
                "name": "sqlalchemy_query",
                "type": "db",
                "db.query": statement,
                "db.query_args": params,
            }
        )
        self.state.query_start_time = datetime.datetime.now()

    def after_cursor_execute(
        self, conn, cursor, statement, parameters, context, executemany
    ):
        if self.state.query_start_time:
            query_duration = datetime.datetime.now() - self.state.query_start_time

            beeline.add_context(
                {
                    "db.duration": query_duration.total_seconds() * 1000,
                    "db.last_insert_id": getattr(cursor, "lastrowid", None),
                    "db.rows_affected": cursor.rowcount,
                }
            )
        if self.state.span:
            beeline.finish_span(self.state.span)
        self.reset_state()

    def handle_error(self, context):

        beeline.add_context_field(
            "db.error", beeline.internal.stringify_exception(context.original_exception)
        )
        if self.state.span:
            beeline.finish_span(self.state.span)
        self.reset_state()


listeners = SqlalchemyListeners()


def install():
    listeners.install()
