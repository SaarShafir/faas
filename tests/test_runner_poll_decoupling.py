"""Poll/work decoupling (spec §5.2).

The failure this prevents: per-file work takes seconds to minutes, a naive loop
blocks between polls, the broker blows past max.poll.interval.ms, evicts the
consumer mid-file, and another consumer reprocesses the same file -- forever.

So: the poll loop must keep polling while work is outstanding, and it must use
pause/resume for backpressure rather than simply not calling poll().
"""

from faas_sdk.testing import reference_message


def test_poll_keeps_running_while_work_is_outstanding(runner, consumer, pool):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))

    runner.run_once()
    assert pool.in_flight() == 1

    # The job is deliberately never completed. The loop must still poll --
    # this is what keeps the heartbeat / poll interval alive.
    for _ in range(5):
        runner.run_once()

    assert consumer.poll_count == 6
    assert pool.in_flight() == 1


def test_pool_saturation_pauses_partitions_instead_of_stalling_the_loop(
    runner, consumer, pool, config
):
    for offset in range(10, 10 + config.in_flight):
        consumer.feed(reference_message(partition=0, offset=offset, call_id=f"c{offset}"))
    consumer.feed(reference_message(partition=0, offset=99, call_id="c99"))

    for _ in range(config.in_flight):
        runner.run_once()

    assert pool.in_flight() == config.in_flight

    polls_before = consumer.poll_count
    runner.run_once()

    # Backpressure is applied by pausing the assignment, not by skipping poll().
    assert consumer.paused == {(consumer.topic, 0)}
    assert consumer.poll_count == polls_before + 1
    assert pool.in_flight() == config.in_flight


def test_partitions_resume_once_the_pool_drains(runner, consumer, pool, config):
    for offset in range(10, 10 + config.in_flight):
        consumer.feed(reference_message(partition=0, offset=offset, call_id=f"c{offset}"))
    for _ in range(config.in_flight):
        runner.run_once()
    runner.run_once()
    assert consumer.paused

    pool.succeed_all()
    runner.run_once()

    assert consumer.paused == set()


def test_a_paused_consumer_yields_no_records(runner, consumer, pool, config):
    for offset in range(10, 10 + config.in_flight + 3):
        consumer.feed(reference_message(partition=0, offset=offset, call_id=f"c{offset}"))

    for _ in range(config.in_flight + 3):
        runner.run_once()

    # Never over-commit the pool, however many times we poll.
    assert pool.in_flight() == config.in_flight


def test_in_flight_depth_is_reported_as_a_metric(runner, consumer, pool, metrics):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    assert metrics.gauge_value("faas.in_flight") == 1

    pool.succeed_all()
    runner.run_once()
    assert metrics.gauge_value("faas.in_flight") == 0
