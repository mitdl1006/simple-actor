import sys

sys.path.append("./")
import time
import logging
from dataclasses import dataclass, field
from functools import partial
from threading import Thread, Lock, Event
from queue import Queue, Empty
from datetime import datetime
from collections import OrderedDict
from typing import (
    Any,
    TypeVar,
    Protocol,
    Callable,
    Dict,
    Optional,
    runtime_checkable,
    Generic,
)


_log = logging.getLogger(__name__)


@dataclass
class CommandResult:
    # 작업 수행 경과 시간, 타임스탬프 등의 정보 추가
    success: bool
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[Exception] = field(default=None)


@runtime_checkable
class ContextProtocol(Protocol): ...  # 컨텍스트 제약 최소화..


TCtx = TypeVar("TCtx", bound=ContextProtocol, contravariant=True)


@runtime_checkable
class CommandProtocol(Protocol, Generic[TCtx]):
    name: str

    def execute(self, ctx: TCtx, *args, **kwargs) -> CommandResult: ...
    def redo(self, ctx: TCtx, *args, **kwargs) -> CommandResult: ...
    def undo(self, ctx: TCtx, *args, **kwargs) -> CommandResult: ...


class _Invoker:
    def __init__(self, *, name: str = "Invoker") -> None:
        self._lock = Lock()
        self._name = name  # Actor 이름

    def invoke(self, partial_func: Callable[[], CommandResult]) -> CommandResult:
        with self._lock:
            try:
                _log.info(f"Invoker: [{self._name}] 명령 실행 중...")
                result = partial_func()
            except Exception as exc:
                _log.error(f"Invoker: [{self._name}] 명령 실행 중 예외 발생 - {exc}")
                return CommandResult(success=False, error=exc)  # 예외 정보 포함
            else:
                _log.info(f"Invoker: [{self._name}] 명령 실행 완료 - 결과: {result}")
                return result
            # finally:
            #     ...


class Actor:
    def __init__(
        self,
        *,
        name: str = "Actor",
        history_size: int = 100,
        interval: float = 0.5,
        backoff: float = 0.1,
        join_timeout: float = 1.0,
    ) -> None:
        self._cmd_queue: Queue[Callable[[], CommandResult]] = Queue(maxsize=-1)
        self._thread: Optional[Thread] = None
        self._stop_flag: Event = Event()
        self._stop_flag.set()
        self._invoker: _Invoker = _Invoker(name=name)
        self._interval: float = interval
        self._backoff: float = backoff
        self._join_timeout: float = join_timeout
        self.name: str = name
        self._history_size: int = history_size
        self._history: OrderedDict[datetime, CommandResult] = OrderedDict()
        self._history_lock: Lock = Lock()  # 추가

    @property
    def history(self) -> OrderedDict[datetime, CommandResult]:
        with self._history_lock:
            return self._history.copy()

    def _run(self) -> None:
        while not self._stop_flag.is_set():
            func: Callable[[], CommandResult]
            sleep_time = self._backoff  # 기본 대기 시간
            try:
                started_time = time.perf_counter()
                try:
                    func = self._cmd_queue.get(timeout=self._backoff)
                except Empty:
                    pass
                else:
                    command_result = self._invoker.invoke(partial_func=func)
                    with self._history_lock:  # 히스토리 접근 시 잠금
                        self._history[datetime.now()] = command_result
                        if len(self._history) > self._history_size:
                            self._history.popitem(last=False)
                    elapsed_time = time.perf_counter() - started_time
                    sleep_time = max(
                        self._backoff, self._interval - elapsed_time
                    )  # 조정된 대기 시간
                finally:
                    self._stop_flag.wait(sleep_time)
            except Exception as exc:
                _log.exception(f"Actor: 루프 예외 발생, 계속 실행합니다. - 예외: {exc}")
                continue

    def start(self) -> bool:
        # Guard
        if isinstance(self._thread, Thread) and self._thread.is_alive():
            _log.warning(f"{self.__class__.__name__}: 시작 실패 - 이미 실행 중입니다.")
            return False
        try:
            _log.info(f"{self.__class__.__name__}: 시작 중...")
            self._thread = Thread(
                target=self._run, daemon=False
            )  # 데몬 스레드 아님, 리소스 정리 필요
            self._stop_flag.clear()  # 플래그 초기화
            self._thread.start()  # 스레드 시작
            _log.info(f"{self.__class__.__name__}: 시작 완료, 현재 실행 중입니다.")
            return True
        except Exception as exc:
            _log.warning(f"{self.__class__.__name__}: 시작 실패 - {exc}")
            # start에 실패하면 stop 이벤트 세팅해 안정화
            self._stop_flag.set()
            return False

    def stop(self) -> bool:
        # Guard
        if not isinstance(self._thread, Thread):
            _log.warning(
                f"{self.__class__.__name__}: 정지 실패 - 스레드가 존재하지 않습니다."
            )
            return False
        try:
            _log.info(f"{self.__class__.__name__}: 정지 중...")
            self._stop_flag.set()  # 정지 플래그 세팅
            self._thread.join(timeout=self._join_timeout)  # 스레드 종료 대기
            if self._thread.is_alive():  # 스레드가 여전히 살아 있다면
                _log.warning(
                    f"{self.__class__.__name__}: 정지 실패 - 스레드가 아직 살아있습니다."
                )
                return False
            self._thread = None  # 스레드 참조 해제 (가비지 컬렉션 대상)
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

    def request_command(
        self,
        cmd: CommandProtocol[TCtx],
        ctx: TCtx,
        *args,
        **kwargs,
    ) -> None:
        self._cmd_queue.put(partial(cmd.execute, ctx, *args, **kwargs))
