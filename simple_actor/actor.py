import logging
from dataclasses import dataclass, field
from threading import Thread, Lock, Condition
from datetime import datetime
from collections import deque
from concurrent.futures import Future, CancelledError
from typing import Any, Callable, Deque, Dict, Tuple, Optional, Union


_log = logging.getLogger(__name__)

# 모듈 내부에서 유일 객체로 정의(권장)
_STOP_SENTINEL: object = object()


@dataclass
class ActorJob:
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    future: Future
    stealable: bool = True
    enqueued_at: datetime = field(default_factory=datetime.now)


@dataclass
class ActorHistoryEntry:
    at: datetime
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    ok: bool
    result: Any = None
    error: Optional[BaseException] = None


class _Invoker:
    def __init__(self, *, name: str = "Invoker") -> None:
        self._name = name  # Actor 이름

    def invoke(self, func: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        fname: str = getattr(func, "__name__", func.__class__.__name__)
        _log.info(f"Invoker: [{self._name}] {fname} 실행 중...")
        result = func(*args, **kwargs)
        _log.info(f"Invoker: [{self._name}] {fname} 실행 완료 - 결과: {result}")
        return result


class Actor:
    def __init__(
        self,
        *,
        name: str = "Actor",
        history_size: int = 100,
        join_timeout: float = 1.0,
    ) -> None:
        # Actor는 "순수 callable 실행"만 담당: 내부 큐에는 (ActorJob | STOP_SENTINEL)
        # work stealing을 위해 deque + Condition을 사용합니다.
        self._queue: Deque[Union[ActorJob, object]] = deque()
        self._queue_lock: Lock = Lock()
        self._queue_cv: Condition = Condition(self._queue_lock)  # 큐 락과 동일한 락 사용

        self._thread: Optional[Thread] = None
        self._started: bool = False  # 액터의 시작/정지 상태
        self._invoker: _Invoker = _Invoker(name=name)
        self._join_timeout: float = join_timeout
        self.name: str = name

        self._steal_fn: Optional[Callable[[], Optional[ActorJob]]] = None
        # "현재 실행 중" 상태는 queue lock과 동일한 락으로 보호합니다.
        self._is_busy: bool = False  # 작업 실행 중 상태 플래그

        self._history_size: int = history_size
        # 히스토리는 "최근 N개"의 순서형 로그이므로 key 기반 dict보다 deque가 적합합니다.
        # (datetime key 충돌/덮어쓰기 문제도 원천적으로 제거)
        self._history: Deque[ActorHistoryEntry] = deque(maxlen=history_size)
        self._history_lock: Lock = Lock()
        self._lifecycle_lock: Lock = Lock()

    @property
    def history(self) -> Tuple[ActorHistoryEntry, ...]:
        with self._history_lock:
            # 불변 시퀀스로 반환(외부에서 append/clear 등 변경 불가)
            # 튜플은 슬라이싱도 지원합니다.
            return tuple(self._history)

    @property
    def started(self) -> bool:
        with self._lifecycle_lock:
            return self._started

    def load(self) -> int:
        """대기 작업 수 + 실행 중(0/1) 기반의 간단한 부하 지표."""
        with self._queue_cv:
            return len(self._queue) + (1 if self._is_busy else 0)

    def wake(self) -> None:
        """대기 중인 워커를 깨웁니다(work stealing 시 유휴 Actor를 깨우기 용도)."""
        with self._queue_cv:
            self._queue_cv.notify()

    def set_steal_fn(self, steal_fn: Optional[Callable[[], Optional[ActorJob]]]) -> None:
        """다른 Actor의 큐에서 작업을 훔쳐오는 콜백을 설정합니다."""
        with self._lifecycle_lock:
            self._steal_fn = steal_fn

    def steal_next(self) -> Optional[ActorJob]:
        """FIFO를 지키며 다음 작업(큐의 front)을 1개 훔칩니다.

        규칙:
        - victim이 "이미 실행 중"(is_busy=True)일 때만 steal 허용
          (첫 작업을 victim이 처리 중인 상황에서, thief는 두 번째 작업부터 가져가게 됨)
        - FIFO 보장을 위해 queue front가 stealable이 아닐 경우, 뒤쪽을 건너뛰어 훔치지 않음
        """
        with self._queue_cv:
            if not self._is_busy:  # victim이 실행 중이 아니면 훔치지 않음
                return None
            if not self._queue:  # 큐가 비었으면 훔치지 않음
                return None
            item = self._queue[0]  # 큐에서 제거 되지 않은 상태로 확인
            if item is _STOP_SENTINEL:  # STOP 센티넬이면 훔치지 않음
                return None
            job: ActorJob = item  # type: ignore[assignment]
            if not job.stealable:  # stealable이 아니면 훔치지 않음
                return None
            self._queue.popleft()  # FIFO 보장하며 제거
            return job

    def submit(self, func: Callable[..., Any], *args, stealable: bool = True, **kwargs) -> Future:
        with self._lifecycle_lock:
            if not self._started:
                raise RuntimeError("Actor가 정지 상태입니다. start() 후 submit() 하세요.")
            if not (isinstance(self._thread, Thread) and self._thread.is_alive()):
                raise RuntimeError("Actor가 시작되지 않았습니다. start() 후 submit() 하세요.")

            fut: Future = Future()
            with self._queue_cv:
                self._queue.append(ActorJob(func=func, args=args, kwargs=kwargs, future=fut, stealable=stealable))
                self._queue_cv.notify()
            return fut

    def _record_history(
        self,
        *,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        ok: bool,
        result: Any = None,
        error: Optional[BaseException] = None,
    ) -> None:
        with self._history_lock:
            now = datetime.now()
            self._history.append(ActorHistoryEntry(
                at=now, args=args, kwargs=kwargs, ok=ok, result=result, error=error
            ))

    def _run(self) -> None:
        job: Optional[ActorJob] = None
        while True:
            try:
                # 1. 내 큐 확인 (Lock 보유)
                with self._queue_cv:
                    if self._queue:
                        item = self._queue.popleft()
                        if item is _STOP_SENTINEL:  # 종료 신호
                            return  # 워커 종료
                        job = item  # type: ignore[assignment]
                        self._is_busy = True
                    else:
                        job = None

                # 2. 큐가 비었으면 스틸링 시도 (Lock 해제 상태에서 수행 -> 데드락 방지)
                if job is None and self._steal_fn is not None:
                    try:
                        job = self._steal_fn()
                    except Exception:
                        pass  # 스틸링 실패/오류는 무시

                    if job is not None:
                        with self._queue_cv:
                            self._is_busy = True

                # 3. 여전히 할 일이 없으면 대기 (Lock 보유)
                if job is None:
                    with self._queue_cv:
                        # 대기 직전 큐 다시 확인 (스틸링 시도 중 새 작업이 들어왔을 수 있음)
                        if not self._queue:
                            self._queue_cv.wait()
                        continue

                # 4. 작업 실행 (이 시점에는 job이 반드시 존재)

                if not job.future.set_running_or_notify_cancel():
                    self._record_history(
                        args=job.args,
                        kwargs=job.kwargs,
                        ok=False,
                        result=None,
                        error=CancelledError(),
                    )
                    with self._queue_cv:
                        self._is_busy = False
                    continue

                try:
                    try:
                        result = self._invoker.invoke(func=job.func, args=job.args, kwargs=job.kwargs)
                    except Exception as exc:
                        job.future.set_exception(exc)
                        self._record_history(args=job.args, kwargs=job.kwargs, ok=False, result=None, error=exc)
                    else:
                        job.future.set_result(result)
                        self._record_history(args=job.args, kwargs=job.kwargs, ok=True, result=result, error=None)
                finally:
                    with self._queue_cv:
                        self._is_busy = False

            except Exception as exc:
                _log.exception(f"Actor-{self.name}: 메인 작업 루프 내부 예외 발생 - {exc}")
                # 예외가 나도 워커는 계속 유지(다음 작업 대기)
                continue

    def start(self) -> bool:
        with self._lifecycle_lock:
            if isinstance(self._thread, Thread) and self._thread.is_alive():
                _log.warning(f"{self.__class__.__name__}: 시작 실패 - 이미 실행 중입니다.")
                return False
            try:
                _log.info(f"{self.__class__.__name__}: 시작 중...")
                self._thread = Thread(target=self._run, daemon=False)
                self._started = True
                self._thread.start()
                _log.info(f"{self.__class__.__name__}: 시작 완료, 현재 실행 중입니다.")
                return True
            except Exception as exc:
                _log.warning(f"{self.__class__.__name__}: 시작 실패 - {exc}")
                self._started = False
                return False

    def stop(self, *, drain_and_cancel: bool = True) -> bool:
        with self._lifecycle_lock:
            """
            drain_and_cancel=True:
              - stop 시점에 큐에 대기 중인 작업들을 드레인하면서 Future.cancel() 처리
              - 실행 중인 작업은 강제 중단하지 않음(완료/예외로 Future 종료)
              - 워커를 깨우기 위해 STOP 센티넬을 1개 넣음
            """
            if not isinstance(self._thread, Thread):
                _log.warning(f"{self.__class__.__name__}: 정지 실패 - 스레드가 존재하지 않습니다.")
                return False

            try:
                _log.info(f"{self.__class__.__name__}: 정지 중...")
                self._started = False

                # 대기 중인 작업 취소
                if drain_and_cancel:
                    with self._queue_lock:
                        drained: list[Union[ActorJob, object]] = list(self._queue)
                        self._queue.clear()
                    for item in drained:
                        if item is _STOP_SENTINEL:
                            continue  # 이미 종료 센티넬이므로 무시
                        job: ActorJob = item  # type: ignore[assignment]
                        job.future.cancel()
                        self._record_history(
                            args=job.args,
                            kwargs=job.kwargs,
                            ok=False,
                            result=None,
                            error=CancelledError(),
                        )

                # 워커 종료를 위한 센티넬 투입 + notify
                with self._queue_cv:
                    self._queue.append(_STOP_SENTINEL)
                    self._queue_cv.notify_all()

                self._thread.join(timeout=self._join_timeout)
                if self._thread.is_alive():
                    _log.warning(f"{self.__class__.__name__}: 정지 실패 - 스레드가 아직 살아있습니다.")
                    return False

                self._thread = None
                _log.info(f"{self.__class__.__name__}: 정지 완료, 현재 정지 상태입니다.")
                return True
            except Exception as exc:
                _log.warning(f"{self.__class__.__name__}: 정지 실패 - {exc}")
                return False

    def restart(self) -> None:
        if not self.stop():
            _log.warning(f"{self.__class__.__name__}: 재시작 중지 실패 - 정지 실패")
        _log.info(f"{self.__class__.__name__}: 재시작 중...")
        if not self.start():
            _log.warning(f"{self.__class__.__name__}: 재시작 실패 - 시작 실패")
        _log.info(f"{self.__class__.__name__}: 재시작 완료.")
