from threading import Thread, Lock, Event
import time
from queue import Queue, Empty
from pathlib import Path
from typing import Any, Protocol, Callable, List, Tuple, Dict, Optional
import logging
from functools import partial

logger = logging.getLogger(__name__)


class BaseCommandProtocol(Protocol):
    name: str

    def execute(self, context: BaseActorProtocol, *args, **kwargs) -> Any: ...


class _Invoker:
    def __init__(self) -> None:
        self._lock = Lock()

    def invoke(self, *, cmd: BaseCommandProtocol, context: BaseActorProtocol) -> Any:
        with self._lock:
            try:
                logger.info(f"Invoker: [{cmd.name}] 명령 실행 중...")
                result = cmd.execute(context)
                logger.info(f"Invoker: [{cmd.name}] 명령 실행 완료 - 결과: {result}")
                return result
            except Exception as exc:
                logger.exception(
                    f"Invoker: [{cmd.name}] 명령 실행 중 예외 발생 - {exc}"
                )
                raise exc
            else:
                ...
            finally:
                ...


class BaseActorProtocol(Protocol): ...


class ThreadManager:
    def __init__(
        self,
        stop_event: Event,
        *,
        actor_name: str,
        func_factory: Dict[str, Callable],
        interval: float = 0.5,
        backoff: float = 0.1,
        join_timeout: float = 1.0,
    ) -> None:
        self._actor_name: str = actor_name
        self._interval: float = interval
        self._backoff: float = backoff
        self._join_timeout: float = join_timeout
        self._stop_event = stop_event
        self._lock = Lock()
        self._func_factory: Dict[str, Callable] = func_factory
        self.under_management: Dict[str, Thread] = dict()
        self.manage_thread: Optional[Thread] = None

    def start(self) -> None:
        # 기존에 set된 상태가 있으면 초기화
        self._stop_event.clear()
        if isinstance(self.manage_thread, Thread) and self.manage_thread.is_alive():
            logger.info(f"{self._actor_name}/ThreadManager: 이미 관리 스레드 실행 중.")
            return
        # 관리 스레드 시작
        t = self.manage_thread = Thread(
            target=self._manage,
            name="thread-manager",
            daemon=False,
        )
        t.start()

    def stop(self) -> None:
        # 종료 신호 발신
        self._stop_event.set()

        # 관리 스레드 종료 대기
        if isinstance(self.manage_thread, Thread) and self.manage_thread.is_alive():
            self.manage_thread.join(timeout=self._join_timeout)

    def _func_wrapper(self, name: str, f: Callable):
        try:
            f()
        except Exception:
            # 스택 트레이스 포함하여 로깅
            logger.exception(
                f"{self._actor_name}/ThreadManager: [{name}] 스레드 예외 발생"
            )

    def _ensure_thread(self, name: str) -> None:
        with self._lock:
            t = self.under_management.get(name)
            if (isinstance(t, Thread) and not t.is_alive()) or t is None:
                f = self._func_factory.get(name)
                if not callable(f):  # None도 포함하나, 더 엄격하게 검사
                    logger.error(
                        f"{self._actor_name}/ThreadManager: [{name}]에 해당하는 실행 가능한 함수가 없습니다. 스레드 생성에 실패했습니다."
                    )
                    return
                logger.warning(
                    f"{self._actor_name}/ThreadManager: [{name}] 스레드를 실행합니다..."
                )
                t = Thread(
                    target=partial(self._func_wrapper, name, f),
                    name=f"{name}-thread",
                    daemon=False,
                )
                t.start()
                logger.info(
                    f"{self._actor_name}/ThreadManager: [{name}] 스레드가 실행되었습니다."
                )
                self.under_management[name] = t  # 업데이트

    def _manage(self) -> None:
        t: Thread

        logger.info(f"{self._actor_name}/ThreadManager: 관리 시작")

        # 주기적으로 스레드 상태 점검 및 재시작
        while not self._stop_event.is_set():
            start_time = time.perf_counter()

            for name in self._func_factory.keys():
                try:
                    self._ensure_thread(name)
                except Exception as exc:
                    logger.warning(
                        f"{self._actor_name}/ThreadManager: [{name}] 스레드 관리 중 예외 발생 - {exc}"
                    )

            elapsed = time.perf_counter() - start_time
            sleep_time = max(self._backoff, self._interval - elapsed)

            self._stop_event.wait(sleep_time)  # 즉시 중단 가능

        # ============ 종료 절차 ============

        # 모든 관리하던 스레드 join (타임아웃 적용)
        for name, t in list(self.under_management.items()):
            if isinstance(t, Thread) and t.is_alive():
                logger.info(
                    f"{self._actor_name}/ThreadManager: [{name}] 스레드 종료 대기..."
                )
                t.join(timeout=self._join_timeout)
                if t.is_alive():
                    logger.warning(
                        f"{self._actor_name}/ThreadManager: [{name}] 스레드가 아직 살아있습니다."
                    )
            self.under_management.pop(name, None)  # 안전하게 제거

        logger.info(f"{self._actor_name}/ThreadManager: 스레드 관리 종료")


class BaseActor(BaseActorProtocol):
    def __init__(
        self,
        *,
        interval: float = 0.5,
        backoff: float = 0.1,
        join_timeout: float = 1.0,
        log_file_path: Path = Path("base_log.log"),
    ) -> None:
        # 초기화
        self._interval: float = interval
        self._backoff: float = backoff
        self._join_timeout: float = join_timeout
        self._message_queue: Queue = Queue(-1)  # 큐 사이즈 무한
        self._lock = Lock()
        self._invoker = _Invoker()
        # 종료/중단 시그널
        self._stop_event = Event()

        self._thread_manager: ThreadManager = ThreadManager(
            stop_event=self._stop_event,
            actor_name=self.__class__.__name__,
            func_factory={
                "collecting": self._collecting,
                "processing": self._processing,
            },  # 추가 가능...
        )

    def start(self) -> bool:
        try:
            logger.info(f"{self.__class__.__name__}: 시작 중...")
            self._thread_manager.start()  # stop_flag 클리어 포함
            logger.info(f"{self.__class__.__name__}: 시작 완료, 현재 실행 중입니다.")
            return True
        except Exception as exc:
            logger.warning(f"{self.__class__.__name__}: 시작 실패 - {exc}")
            # start에 실패하면 stop 이벤트 세팅해 안정화
            self._stop_event.set()
            return False

    def stop(self) -> bool:
        try:
            logger.info(f"{self.__class__.__name__}: 정지 중...")
            self._thread_manager.stop()  # stop_flag 세팅 포함
            logger.info(f"{self.__class__.__name__}: 정지 완료, 현재 정지 상태입니다.")
            return True
        except Exception as exc:
            logger.warning(f"{self.__class__.__name__}: 정지 실패 - {exc}")
            return False

    def execute_command(self, cmd: BaseCommandProtocol) -> Any:
        """함수 콜을 통한 즉시 명령 실행"""
        return _Invoker().invoke(cmd=cmd, context=self)

    def schedule_command(self, cmd: BaseCommandProtocol) -> Any:
        """명령 실행 예약"""
        return self._message_queue.put(cmd)

    def _collecting(self, *args, **kwargs) -> None:
        """장치 상태값 수집 루프 (사용자 구현)"""

    def _processing(self, *args, **kwargs) -> None:
        """처리 작업 루프"""
        cmd: BaseCommandProtocol

        logger.info(f"{self.__class__.__name__}: 동기 작업 시작")

        # 주기적으로 스레드 상태 점검 및 재시작
        while not self._stop_event.is_set():
            start_time = time.perf_counter()

            # 처리 작업 구현 부분
            try:
                cmd = self._message_queue.get(timeout=self._backoff)
            except Empty:  # 큐가 비어 있음.
                # 명령이 없으면 패스
                pass
            else:
                # 명령이 있으면 처리 (한번에 하나씩)
                self._invoker.invoke(cmd=cmd, context=self)
            finally:
                elapsed = time.perf_counter() - start_time
                sleep_time = max(self._backoff, self._interval - elapsed)

                self._stop_event.wait(sleep_time)  # 즉시 중단 가능

        logger.info(f"{self.__class__.__name__}: 동기 작업 종료")


class SampleCommand(BaseCommandProtocol):
    name = "SampleCommand"

    def execute(self, context: BaseActorProtocol, *args, **kwargs) -> Any:
        print("SampleCommand executed")
        return "Command Result"


class SampleActor(BaseActor):
    def _collecting(self, *args, **kwargs) -> None:
        while not self._stop_event.is_set():
            print("Collecting data...")
            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    actor = SampleActor()
    actor.start()

    time.sleep(1)

    cmd = SampleCommand()
    result = actor.execute_command(cmd)
    print(f"Command Result: {result}")
    threads = []
    cnt = 10
    for _ in range(cnt):
        threads.append(Thread(target=lambda: actor.execute_command(SampleCommand())))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    time.sleep(1)

    cmd = SampleCommand()
    result = actor.execute_command(cmd)
    print(f"Command Result: {result}")

    time.sleep(1)

    actor.stop()
    print("Actor stopped.")
