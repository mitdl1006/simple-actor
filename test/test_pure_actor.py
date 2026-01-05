import sys
import time
from pathlib import Path
from concurrent.futures import CancelledError
from threading import Event
import pytest

# 프로젝트 루트 경로 추가 (simple_actor 패키지를 찾기 위해)
sys.path.append(str(Path(__file__).resolve().parents[1]))

from simple_actor.actor import Actor

@pytest.fixture
def actor():
    """기본 테스트용 Actor fixture"""
    # interval을 짧게 설정하여 테스트 속도 향상
    act = Actor(name="TestActor")
    act.start()
    yield act
    act.stop()

def test_basic_execution(actor):
    """기본적인 함수 실행 및 결과 반환 테스트"""
    def add(a, b):
        return a + b

    future = actor.submit(add, 10, 20)
    result = future.result(timeout=1.0)
    assert result == 30

def test_kwargs_execution(actor):
    """Keyword Arguments 전달 테스트"""
    def greet(name, greeting="Hello"):
        return f"{greeting}, {name}!"

    future = actor.submit(greet, name="World", greeting="Hi")
    result = future.result(timeout=1.0)
    assert result == "Hi, World!"

def test_exception_handling(actor):
    """작업 중 예외 발생 시 Future에 예외가 설정되는지 테스트"""
    def fail():
        raise ValueError("Intentional Failure")

    future = actor.submit(fail)
    
    # result() 호출 시 예외 발생 확인
    with pytest.raises(ValueError):
        future.result(timeout=1.0)
    
    # exception() 메서드로 예외 객체 확인
    exc = future.exception(timeout=1.0)
    assert isinstance(exc, ValueError)

def test_history_recording(actor):
    """작업 완료 후 히스토리에 기록되는지 테스트"""
    def echo(msg):
        return msg

    msg = "History Check"
    future = actor.submit(echo, msg)
    future.result()

    # 히스토리 확인
    history = actor.history
    assert len(history) > 0
    
    # 마지막 항목 확인
    last_entry = history[-1]
    assert last_entry.args[0] == msg
    assert last_entry.ok
    assert last_entry.result == msg

def test_sequential_execution(actor):
    """작업이 순차적으로 실행되는지 테스트 (Actor 모델 특성)"""
    execution_order = []

    def task(idx):
        # 작업 시간 시뮬레이션 (순서가 꼬일 가능성을 배제하기 위해 sleep)
        time.sleep(0.01) 
        execution_order.append(idx)
        return idx

    futures = []
    # 여러 작업을 빠르게 제출
    for i in range(5):
        futures.append(actor.submit(task, i))

    # 모든 결과 대기
    results = [f.result() for f in futures]

    # 결과 값 확인
    assert results == [0, 1, 2, 3, 4]
    # 실행 순서 확인 (0부터 4까지 순서대로 실행되어야 함)
    assert execution_order == [0, 1, 2, 3, 4]

def test_cancel_before_execution(actor):
    """실행 전 취소 테스트"""
    # 1. 긴 작업 제출하여 Actor 점유 (0.1초 동안)
    actor.submit(time.sleep, 0.1)
    
    # 2. 대기열에 들어갈 작업 제출
    future = actor.submit(lambda: "Should not run")
    
    # 3. 즉시 취소 (아직 앞선 작업이 실행 중이므로 큐에 대기 중인 상태)
    cancelled = future.cancel()
    
    # 4. 검증
    assert cancelled, "작업이 취소되어야 합니다."
    assert future.cancelled()
    with pytest.raises(CancelledError):
        future.result()

def test_stop_drain_and_cancel():
    """stop(drain_and_cancel=True) 시 대기 중인 작업 취소 테스트"""
    actor = Actor(name="DrainActor")
    actor.start()

    started = Event()

    try:
        def long_task():
            started.set()  # "실행 시작" 동기화 포인트
            time.sleep(0.5)
            return "done"

        f_running = actor.submit(long_task)

        # long_task가 실제로 워커에서 실행을 시작할 때까지 대기(레이스 제거)
        assert started.wait(timeout=1.0), "long_task가 실행 시작하지 못했습니다(스케줄링 레이스)."

        futures = []
        for i in range(5):
            futures.append(actor.submit(lambda x: x, i))

        actor.stop(drain_and_cancel=True)

        assert f_running.result() == "done"

        for f in futures:
            with pytest.raises(CancelledError):
                f.result()
            assert f.cancelled()

    finally:
        actor.stop()

def test_restart():
    """재시작 테스트"""
    actor = Actor(name="RestartActor")
    actor.start()
    try:
        assert actor.submit(lambda: 1).result() == 1
        actor.restart()
        assert actor.submit(lambda: 2).result() == 2
    finally:
        actor.stop()
