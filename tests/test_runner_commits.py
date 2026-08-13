"""Commit semantics through the runner (spec §5.3).

test_offsets.py covers the ledger in isolation; this covers the runner actually
wiring completion -> commit, and never committing on dispatch.
"""

from faas_sdk.testing import reference_message


def test_dispatch_alone_never_commits(runner, consumer):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    assert consumer.commits == []


def test_commit_happens_on_completion(runner, consumer, pool):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed_all()
    runner.run_once()

    assert consumer.commits == [[((consumer.topic, 0), 11)]]


def test_out_of_order_completion_commits_the_low_water_mark(runner, consumer, pool):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    consumer.feed(reference_message(partition=0, offset=11, call_id="c11"))
    runner.run_once()
    runner.run_once()

    # The second file finishes first -- the classic out-of-order case.
    pool.succeed("c11")
    runner.run_once()
    assert consumer.commits == []

    pool.succeed("c10")
    runner.run_once()
    assert consumer.commits == [[((consumer.topic, 0), 12)]]


def test_revocation_drains_in_flight_work_and_commits_before_releasing(runner, consumer, pool):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    pool.succeed_all()
    consumer.trigger_revoke([(consumer.topic, 0)])

    assert consumer.commits == [[((consumer.topic, 0), 11)]]
    assert runner.in_flight == 0


def test_revocation_abandons_work_that_does_not_finish_in_the_grace_period(
    runner, consumer, pool, clock
):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()

    clock.auto_advance = True  # let the drain grace period expire
    consumer.trigger_revoke([(consumer.topic, 0)])

    # Nothing completed, so nothing is committed: the new owner reprocesses
    # from 10. At-least-once, as designed.
    assert consumer.commits == []
    assert runner.in_flight == 0


def test_shutdown_commits_what_completed(runner, consumer, pool):
    consumer.feed(reference_message(partition=0, offset=10, call_id="c10"))
    runner.run_once()
    pool.succeed_all()

    runner.close()

    assert consumer.commits == [[((consumer.topic, 0), 11)]]
    assert consumer.closed is True
