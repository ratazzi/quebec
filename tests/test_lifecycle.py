# coding: utf-8
"""
Integration tests for lifecycle hooks.

Tests that lifecycle hooks (on_start, on_stop, etc.) are properly registered.
"""

from sqlalchemy import text


class TestLifecycleHooks:
    """Test lifecycle hook registration."""

    def test_on_start_hook_registration(self, qc):
        """on_start decorator should register a start handler."""
        handler_called = []

        @qc.on_start
        def my_start_handler():
            handler_called.append("start")

        # Manually invoke start handlers to verify registration
        qc.invoke_start_handlers()

        assert "start" in handler_called

    def test_on_stop_hook_registration(self, qc):
        """on_stop decorator should register a stop handler."""
        handler_called = []

        @qc.on_stop
        def my_stop_handler():
            handler_called.append("stop")

        # Manually invoke stop handlers
        qc.invoke_stop_handlers()

        assert "stop" in handler_called

    def test_multiple_start_hooks(self, qc):
        """Multiple on_start hooks should all be called."""
        call_order = []

        @qc.on_start
        def first_handler():
            call_order.append("first")

        @qc.on_start
        def second_handler():
            call_order.append("second")

        qc.invoke_start_handlers()

        assert "first" in call_order
        assert "second" in call_order

    def test_multiple_stop_hooks(self, qc):
        """Multiple on_stop hooks should all be called."""
        call_order = []

        @qc.on_stop
        def first_handler():
            call_order.append("first")

        @qc.on_stop
        def second_handler():
            call_order.append("second")

        qc.invoke_stop_handlers()

        assert "first" in call_order
        assert "second" in call_order

    def test_on_worker_start_hook(self, qc):
        """on_worker_start decorator should register a worker start handler."""

        # Just test that registration doesn't error
        @qc.on_worker_start
        def worker_start_handler():
            pass

        # No error means success

    def test_on_worker_stop_hook(self, qc):
        """on_worker_stop decorator should register a worker stop handler."""

        # Just test that registration doesn't error
        @qc.on_worker_stop
        def worker_stop_handler():
            pass

        # No error means success

    def test_hook_with_arguments(self, qc):
        """Hook handler should be able to receive context."""
        received_args = []

        @qc.on_start
        def handler_with_args(*args, **kwargs):
            received_args.append((args, kwargs))

        qc.invoke_start_handlers()

        # Handler should have been called
        assert len(received_args) == 1


class TestPausingQueues:
    """Test queue pausing functionality."""

    def test_pauses_table_exists(self, qc_with_sqlalchemy):
        """Pauses table should be created."""
        ctx = qc_with_sqlalchemy
        session = ctx["session"]
        prefix = ctx["prefix"]

        # Query should not error if table exists
        result = session.execute(text(f"SELECT COUNT(*) FROM {prefix}_pauses"))
        count = result.scalar()
        assert count == 0  # No paused queues initially

    def test_pauses_table_has_queue_name_column(self, qc_with_sqlalchemy):
        """Pauses table should have queue_name column."""
        ctx = qc_with_sqlalchemy
        session = ctx["session"]
        prefix = ctx["prefix"]

        # Try to select queue_name column
        result = session.execute(
            text(f"SELECT queue_name FROM {prefix}_pauses LIMIT 1")
        )
        # No error means column exists
        result.fetchall()


class TestProcessRegistry:
    """Test process registration functionality."""

    def test_processes_table_exists(self, qc_with_sqlalchemy):
        """Processes table should be created."""
        ctx = qc_with_sqlalchemy
        session = ctx["session"]
        prefix = ctx["prefix"]

        result = session.execute(text(f"SELECT COUNT(*) FROM {prefix}_processes"))
        count = result.scalar()
        assert count == 0  # No processes initially

    def test_processes_table_schema(self, qc_with_sqlalchemy):
        """Processes table should have required columns."""
        ctx = qc_with_sqlalchemy
        session = ctx["session"]
        prefix = ctx["prefix"]

        # Query specific columns to verify schema
        result = session.execute(
            text(f"""
                SELECT kind, last_heartbeat_at, pid, hostname, name
                FROM {prefix}_processes
                LIMIT 1
            """)
        )
        # No error means columns exist
        result.fetchall()
