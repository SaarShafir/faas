"""Readiness and liveness.

These decide when OpenShift restarts a pod and when it routes to one, so the
tests are mostly about the two ways of getting them wrong: a liveness probe
that kills a pod for being busy, and a readiness probe that reports a wedged
process as fine.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import pytest

from faas_sdk.codec import JsonCodec
from faas_sdk.dlq import DeadLetterQueue
from faas_sdk.health import HealthState, serve
from faas_sdk.results import ResultEmitter
from faas_sdk.runner import FunctionRunner
from faas_sdk.testing import reference_message


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


# -- the state machine -----------------------------------------------------


def test_a_fresh_pod_is_alive_but_not_ready():
    """Alive because the process is running; not ready because it holds no
    partitions yet. Routing to it before an assignment sends work nowhere."""
    state = HealthState(clock=FakeClock())

    assert state.liveness()[0]
    assert not state.readiness()[0]


def test_a_pod_holding_partitions_is_ready():
    state = HealthState(clock=FakeClock())
    state.assigned(12)

    ready, body = state.readiness()
    assert ready
    assert body["partitions"] == 12


def test_losing_every_partition_is_not_ready_but_stays_alive():
    """A rebalance takes partitions away through no fault of the pod. Failing
    liveness here would restart a healthy process mid-rebalance and start
    another rebalance."""
    state = HealthState(clock=FakeClock())
    state.assigned(12)
    state.assigned(0)

    assert not state.readiness()[0]
    assert state.liveness()[0]


def test_a_wedged_poll_loop_eventually_fails_liveness():
    """The case the probe exists for: the process is up, the port answers, and
    nothing is being consumed."""
    clock = FakeClock()
    state = HealthState(stale_after=300.0, clock=clock)
    state.assigned(4)

    clock.advance(299)
    assert state.liveness()[0]

    clock.advance(2)
    alive, body = state.liveness()
    assert not alive
    assert body["seconds_since_poll"] > 300


def test_slow_work_does_not_fail_liveness():
    """A 5-minute file is slow, not wedged. Restarting for it would turn one
    slow call into a redelivered one -- the §5.2 failure by another route."""
    clock = FakeClock()
    state = HealthState(stale_after=600.0, clock=clock)

    for _ in range(10):
        clock.advance(59)
        state.loop_ran()

    assert state.liveness()[0]


def test_a_draining_pod_leaves_the_rotation_but_is_not_killed():
    """SIGTERM means stop taking traffic and finish what is in flight. Failing
    liveness during a drain is exactly the restart the drain exists to avoid."""
    state = HealthState(clock=FakeClock())
    state.assigned(4)
    state.stopping()

    assert not state.readiness()[0]
    assert state.liveness()[0]


# -- wired into the runner -------------------------------------------------


@pytest.fixture
def runner_with_health(config, consumer, producer, object_store, pool, clock, metrics):
    state = HealthState(clock=FakeClock())
    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=JsonCodec(),
        results=ResultEmitter(
            config=config,
            producer=producer,
            object_store=object_store,
            codec=JsonCodec(),
            clock=clock,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        metrics=metrics,
        health=state,
        clock=clock,
    )
    return runner, state


def test_the_runner_becomes_ready_once_it_is_assigned(runner_with_health):
    """librdkafka assigns asynchronously after subscribe, so readiness comes
    from the loop rather than from `start()` returning."""
    runner, state = runner_with_health
    assert not state.readiness()[0]

    runner.start()
    runner.run_once()

    assert state.readiness()[0]


def test_stopping_the_runner_drops_readiness(runner_with_health):
    runner, state = runner_with_health
    runner.start()
    runner.run_once()
    assert state.readiness()[0]

    runner.stop()

    assert not state.readiness()[0]
    assert state.liveness()[0], "a draining pod must not be restarted"


def test_each_loop_iteration_refreshes_liveness(runner_with_health, consumer):
    runner, state = runner_with_health
    runner.start()
    state._clock.advance(200)

    runner.run_once()

    assert state.liveness()[1]["seconds_since_poll"] == 0


def test_a_runner_built_without_health_still_runs(
    config, consumer, producer, object_store, pool, clock
):
    """`health` is optional at the constructor, so nothing that builds a runner
    has to know about probes."""
    runner = FunctionRunner(
        config=config,
        consumer=consumer,
        pool=pool,
        codec=JsonCodec(),
        results=ResultEmitter(
            config=config,
            producer=producer,
            object_store=object_store,
            codec=JsonCodec(),
            clock=clock,
        ),
        dlq=DeadLetterQueue(config=config, producer=producer, clock=clock),
        clock=clock,
    )
    runner.start()
    consumer.feed(reference_message(partition=0, offset=1, call_id="c1"))
    runner.run_once()  # must not raise


# -- the endpoint ----------------------------------------------------------


def test_the_probes_answer_over_http(free_tcp_port):
    state = HealthState(clock=FakeClock())
    server = serve(state, port=free_tcp_port)
    assert server is not None
    port = server.server_address[1]

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as response:
            assert response.status == 200
            assert json.load(response)["alive"] is True

        # Not ready yet: no partitions. 503 is what makes the kubelet take this
        # pod out of the endpoints list rather than restart it.
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5)
        assert excinfo.value.code == 503

        state.assigned(3)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5) as response:
            assert response.status == 200
            assert json.load(response)["partitions"] == 3
    finally:
        server.shutdown()
        server.server_close()


def test_a_port_that_cannot_be_bound_does_not_stop_the_pod(monkeypatch, caplog):
    """A pod without probes is worse off; a pod that will not start processes
    no audio at all."""
    import faas_sdk.health as module

    def explode(*args, **kwargs):
        raise OSError("address already in use")

    monkeypatch.setattr(module, "ThreadingHTTPServer", explode)

    with caplog.at_level("ERROR"):
        assert serve(HealthState(), port=9) is None
    assert "probes are unavailable" in caplog.text


def test_probes_can_be_turned_off(free_tcp_port):
    """FAAS_HEALTH_PORT=0 is how a deployment says "no probe server", so zero
    has to mean off rather than "pick a port"."""
    server = serve(HealthState(), port=free_tcp_port)
    assert server is not None
    server.shutdown()
    server.server_close()

    assert serve(HealthState(), port=0) is None
