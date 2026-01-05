import pytest

pytest.skip(
    "legacy test: depends on old command-actor API (root-level actor module)",
    allow_module_level=True,
)

import sys
from pathlib import Path

# 프로젝트 루트 추가
sys.path.append(str(Path(__file__).resolve().parents[1]))

import time
import logging
from threading import Lock, Thread
from typing import Any, Tuple

from actor import Actor, ContextProtocol, CommandProtocol, CommandResult

logging.basicConfig(level=logging.INFO)


class URContext(ContextProtocol):
    def __init__(self, robot_ip: str, port: int = 502) -> None:
        self.robot_ip: str = robot_ip
        self.port: int = port


class URPrintCommand(CommandProtocol[URContext]):
    name: str = "URPrintCommand"

    def execute(self, ctx: URContext, *args: Any, **kwargs: Any) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = "URPrintCommand Execute"
        print(msg)
        return CommandResult(success=True, data={"message": msg})

    def undo(self, ctx: URContext, *args: Any, **kwargs: Any) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = "URPrintCommand Undo"
        print(msg)
        return CommandResult(success=True, data={"message": msg})

    def redo(self, ctx: URContext, *args: Any, **kwargs: Any) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = "URPrintCommand Redo"
        print(msg)
        return CommandResult(success=True, data={"message": msg})


class URMoveCommand(CommandProtocol[URContext]):
    name: str = "URMoveCommand"

    def execute(
        self,
        ctx: URContext,
        position: Tuple[float, float, float],
        *args: Any,
        **kwargs: Any,
    ) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = f"URMoveCommand Execute to position {position}"
        print(msg)
        return CommandResult(success=True, data={"message": msg})

    def undo(self, ctx: URContext, *args: Any, **kwargs: Any) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = "URMoveCommand Undo"
        print(msg)
        return CommandResult(success=True, data={"message": msg})

    def redo(
        self,
        ctx: URContext,
        position: Tuple[float, float, float],
        *args: Any,
        **kwargs: Any,
    ) -> CommandResult:
        assert isinstance(ctx, URContext), "URContext required"
        msg = f"URMoveCommand Redo to position {position}"
        print(msg)
        return CommandResult(success=True, data={"message": msg})


class CounterContext(ContextProtocol):
    def __init__(self) -> None:
        self.counter: int = 0
        self._lock = Lock()

    def add_one(self) -> int:
        with self._lock:
            self.counter += 1
            return self.counter


class IncrementCommand(CommandProtocol[CounterContext]):
    name: str = "IncrementCommand"

    def execute(self, ctx: CounterContext, *args: Any, **kwargs: Any) -> CommandResult:
        assert isinstance(ctx, CounterContext), "CounterContext required"
        val = ctx.add_one()
        return CommandResult(success=True, data={"counter": val})

    def undo(self, ctx: CounterContext, *args: Any, **kwargs: Any) -> CommandResult:
        return CommandResult(success=True)

    def redo(self, ctx: CounterContext, *args: Any, **kwargs: Any) -> CommandResult:
        return self.execute(ctx, *args, **kwargs)


def test_multi_actor() -> None:
    # 인스턴스 생성
    actor_a = Actor(name="Actor-A", history_size=5, interval=0.1, backoff=0.05)
    actor_b = Actor(name="Actor-B", history_size=5, interval=0.1, backoff=0.05)

    ctx_a = URContext(robot_ip="192.168.0.10", port=502)
    ctx_b = URContext(robot_ip="192.168.0.11", port=503)

    print_cmd = URPrintCommand()
    move_cmd = URMoveCommand()
    pos_a = (0.1, 0.2, 0.3)
    pos_b = (0.4, 0.5, 0.6)
    
    # 액터 시작

    assert actor_a.start()
    assert actor_b.start()

    # 명령 요청
    try:
        actor_a.request_execute_command(print_cmd, ctx_a)
        actor_a.request_execute_command(move_cmd, ctx_a, pos_a)
        actor_a.request_undo_command(move_cmd, ctx_a)  # undo
        actor_a.request_redo_command(move_cmd, ctx_a, pos_a)  # redo

        actor_b.request_execute_command(move_cmd, ctx_b, pos_b)
        actor_b.request_execute_command(print_cmd, ctx_b)
        actor_b.request_undo_command(print_cmd, ctx_b)  # undo
        actor_b.request_redo_command(print_cmd, ctx_b)  # redo

        time.sleep(2.0)

        assert len(actor_a.history) > 0
        assert len(actor_b.history) > 0
    finally:
        # 액터 정지 (리소스 정리)
        actor_a.stop()
        actor_b.stop()


def test_concurrent_increment() -> None:
    actor = Actor(
        name="Actor-Counter",
        history_size=12000,
        interval=0.0,
        backoff=0.0,
    )
    ctx = CounterContext()
    cmd = IncrementCommand()

    n_threads = 10
    n_increments = 1000

    assert actor.start()

    def worker() -> None:
        for _ in range(n_increments):
            actor.request_execute_command(cmd, ctx)

    threads = [Thread(target=worker) for _ in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    expected = n_threads * n_increments
    deadline = time.perf_counter() + 5.0
    try:
        while time.perf_counter() < deadline and ctx.counter < expected:
            time.sleep(0.01)
        assert ctx.counter == expected, f"counter {ctx.counter} != {expected}"
    finally:
        print(f"Final counter value: {ctx.counter}")
        actor.stop()


if __name__ == "__main__":
    test_multi_actor()
    # test_concurrent_increment()
    print("Tests passed.")
