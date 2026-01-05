import sys
import time
from pathlib import Path
from concurrent.futures import CancelledError
from threading import Event

import pytest

# 프로젝트 루트 경로 추가 (simple_actor 패키지를 찾기 위해)
sys.path.append(str(Path(__file__).resolve().parents[1]))

from simple_actor.actor_pool import ActorPool


@pytest.fixture
def pool() -> ActorPool:
    p = ActorPool(size=3, name="TestPool")
    assert p.start()
    yield p
    p.stop(drain_and_cancel=True)


def test_pool_submit_returns_future_and_result(pool: ActorPool) -> None:
    fut = pool.submit(lambda a, b: a + b, 10, 20)
    assert fut.result(timeout=1.0) == 30


def test_actor_id_routing(pool: ActorPool) -> None:
    ids = pool.actor_ids
    a1 = ids[1]
    a2 = ids[2]
    f1 = pool.submit(lambda x: x, "a", actor_id=a1)
    f2 = pool.submit(lambda x: x, "b", actor_id=a2)
    assert f1.result(timeout=1.0) == "a"
    assert f2.result(timeout=1.0) == "b"

    histories = pool.histories()
    assert len(histories[a1]) == 1
    assert len(histories[a2]) == 1
    # 나머지 actor는 라우팅되지 않았어야 함
    other = [aid for aid in ids if aid not in (a1, a2)]
    assert len(histories[other[0]]) == 0


def test_stop_drain_and_cancel_cancels_queued_tasks() -> None:
    p = ActorPool(size=2, name="DrainPool")
    assert p.start()

    started = Event()
    try:
        a0 = p.actor_ids[0]
        def long_task() -> str:
            started.set()
            time.sleep(0.4)
            return "done"

        f_running = p.submit(long_task, actor_id=a0)
        assert started.wait(timeout=1.0)

        queued = [p.submit(lambda x: x, i, actor_id=a0) for i in range(5)]

        # 실행 중인 작업은 완료, 대기 중인 작업은 cancel
        assert p.stop(drain_and_cancel=True)

        assert f_running.result(timeout=1.0) == "done"
        for f in queued:
            with pytest.raises(CancelledError):
                f.result(timeout=1.0)
            assert f.cancelled()
    finally:
        p.stop(drain_and_cancel=True)


def test_context_manager_starts_and_stops() -> None:
    with ActorPool(size=1, name="CtxPool") as p:
        assert p.submit(lambda: 123).result(timeout=1.0) == 123


def test_least_loaded_scheduling_prefers_less_busy_actor() -> None:
    p = ActorPool(size=2, name="LeastPool")
    assert p.start()

    started = Event()
    try:
        a0, a1 = p.actor_ids
        def long_task() -> str:
            started.set()
            time.sleep(0.3)
            return "long"

        f0 = p.submit(long_task, actor_id=a0)
        assert started.wait(timeout=1.0)

        marker = "route-me"
        f1 = p.submit(lambda x: x, marker)
        assert f1.result(timeout=1.0) == marker
        assert f0.result(timeout=1.0) == "long"

        histories = p.histories()
        # marker 작업이 actor 1로 갔는지 확인
        in_0 = any(e.args and e.args[0] == marker for e in histories[a0])
        in_1 = any(e.args and e.args[0] == marker for e in histories[a1])
        assert not in_0
        assert in_1
    finally:
        p.stop(drain_and_cancel=True)



def test_work_stealing_moves_tasks_from_busy_actor_fifo_next() -> None:
    p = ActorPool(size=2, name="StealPool")
    assert p.start()

    started = Event()
    try:
        a0, a1 = p.actor_ids
        def long_task() -> str:
            started.set()
            time.sleep(0.4)
            return "long"

        # actor0를 바쁘게 만들고, actor_id 라우팅으로 backlog를 명시적으로 쌓습니다.
        # 정책: actor_id로 라우팅된 작업은 steal 금지
        f_long = p.submit(long_task, actor_id=a0)
        assert started.wait(timeout=1.0)

        # victim(actor0)에 "steal 가능한" backlog를 명시적으로 쌓아두고,
        # idle actor1이 FIFO(next) 순서로 가져가는지 확인합니다.
        # (FIFO 요건: victim이 첫 작업(long_task)을 처리 중이면 thief는 그 다음 작업(1)부터 가져가야 함)
        backlog = [p.submit(lambda x: x, i, actor_id=a0) for i in range(1, 6)]

        assert [f.result(timeout=1.0) for f in backlog] == [1, 2, 3, 4, 5]
        assert f_long.result(timeout=1.0) == "long"

        histories = p.histories()
        actor0_ids = [e.args[0] for e in histories[a0] if e.args and isinstance(e.args[0], int)]
        actor1_ids = [e.args[0] for e in histories[a1] if e.args and isinstance(e.args[0], int)]

        # actor_id로 라우팅된 작업은 steal 금지이므로 actor1은 backlog를 처리하지 않습니다.
        assert actor1_ids == []
        assert actor0_ids[:5] == [1, 2, 3, 4, 5]
    finally:
        p.stop(drain_and_cancel=True)


def test_add_actor_while_started_allows_least_loaded_routing() -> None:
    p = ActorPool(size=1, name="DynAddPool")
    assert p.start()

    started = Event()
    try:
        old_id = p.actor_ids[0]
        new_id = p.add_actor()
        assert new_id != old_id
        assert p.size == 2

        def long_task() -> str:
            started.set()
            time.sleep(0.3)
            return "long"

        f0 = p.submit(long_task, actor_id=old_id)
        assert started.wait(timeout=1.0)

        marker = "dynamic-route"
        f1 = p.submit(lambda x: x, marker)

        assert f1.result(timeout=1.0) == marker
        assert f0.result(timeout=1.0) == "long"

        histories = p.histories()
        in_0 = any(e.args and e.args[0] == marker for e in histories[old_id])
        in_1 = any(e.args and e.args[0] == marker for e in histories[new_id])
        assert not in_0
        assert in_1
    finally:
        p.stop(drain_and_cancel=True)


def test_remove_actor_prevents_actor_id_routing() -> None:
    p = ActorPool(size=2, name="DynRemovePool")
    assert p.start()
    try:
        a0, a1 = p.actor_ids
        assert p.remove_actor(a1, drain_and_cancel=True) in (True, False)
        with pytest.raises(RuntimeError):
            p.submit(lambda: "x", actor_id=a1)
    finally:
        p.stop(drain_and_cancel=True)
