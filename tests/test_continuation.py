from __future__ import annotations

import quebec


class ContinuationJob(quebec.BaseClass, quebec.Continuable):
    callback_runs = 0

    def perform(self, *args, **kwargs) -> None:
        return None


def test_step_callback_accepts_step_context_argument() -> None:
    job = ContinuationJob()
    seen: list[tuple[object, bool]] = []

    def callback(step) -> None:
        seen.append((step.cursor, step.resumed))

    job.step("with-context", callback, start=5)

    assert seen == [(5, False)]
