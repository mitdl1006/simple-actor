from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

from .actor import Actor
from .actor import ActorHistoryEntry

from .scheduling import (
    LeastLoadedRouting,
    PlacementStrategy,
    MostLoadedWorkStealer,
    WorkStealingStrategy,
)


@dataclass
class _ActorSlot:
    actor_id: int
    actor: Actor


class ActorPool:
    def __init__(
        self,
        size: int,
        *,
        name: str = "ActorPool",
        actor_history_size: int = 100,
        actor_join_timeout: float = 1.0,
        router_factory: Optional[
            Callable[[Callable[[], List[int]], Callable[[int], Actor]], PlacementStrategy]
        ] = None,
        stealer_factory: Optional[
            Callable[
                [Callable[[], List[int]], Callable[[int], Actor], Callable[[], bool]],
                WorkStealingStrategy,
            ]
        ] = None,
    ) -> None:
        if size <= 0:
            raise ValueError("size must be > 0")

        self._lifecycle_lock: Lock = Lock()
        self._name: str = name
        self._actor_history_size: int = actor_history_size
        self._actor_join_timeout: float = actor_join_timeout
        self._slots: List[Optional[_ActorSlot]] = []
        self._actors_by_id: Dict[int, Actor] = {}
        self._next_actor_id: int = 0
        self._started_event: Event = Event()

        if router_factory is None:
            router_factory = lambda list_ids, get_actor: LeastLoadedRouting(
                list_actor_ids=list_ids,
                get_actor=get_actor,
            )
        if stealer_factory is None:
            stealer_factory = lambda list_ids, get_actor, is_started: MostLoadedWorkStealer(
                list_actor_ids=list_ids,
                get_actor=get_actor,
                is_started=is_started,
            )

        for _ in range(size):
            self._create_slot(reuse_free_slot=False)

        self._router = router_factory(self._list_actor_ids, self._get_actor)
        self._stealer = stealer_factory(
            self._list_actor_ids,
            self._get_actor,
            self._started_event.is_set,
        )
        self._refresh_steal_fns()

    def _allocate_actor_id(self) -> int:
        actor_id = self._next_actor_id
        self._next_actor_id += 1
        return actor_id

    def _list_actor_ids(self) -> List[int]:
        return [slot.actor_id for slot in self._slots if slot is not None]

    def _get_actor(self, actor_id: int) -> Actor:
        return self._actors_by_id[actor_id]

    def _refresh_steal_fns(self) -> None:
        for slot in self._slots:
            if slot is None:
                continue
            actor_id = slot.actor_id
            slot.actor.set_steal_fn(
                lambda thief_id=actor_id: self._stealer.try_steal_for(thief_id)
            )

    def _create_slot(self, *, reuse_free_slot: bool) -> int:
        actor_id = self._allocate_actor_id()
        actor = Actor(
            name=f"{self._name}-{actor_id}",
            history_size=self._actor_history_size,
            join_timeout=self._actor_join_timeout,
        )
        self._actors_by_id[actor_id] = actor

        if reuse_free_slot:
            for i, slot in enumerate(self._slots):
                if slot is None:
                    self._slots[i] = _ActorSlot(actor_id=actor_id, actor=actor)
                    return actor_id

        self._slots.append(_ActorSlot(actor_id=actor_id, actor=actor))
        return actor_id

    @property
    def size(self) -> int:
        return len(self._list_actor_ids())

    @property
    def actor_ids(self) -> Tuple[int, ...]:
        return tuple(self._list_actor_ids())

    @property
    def started(self) -> bool:
        return self._started_event.is_set()
    
    @property
    def name(self) -> str:
        return self._name

    def start(self) -> bool:
        """모든 Actor를 시작합니다. 하나라도 실패하면 False를 반환합니다."""
        with self._lifecycle_lock:
            if self._started_event.is_set():
                return False

            ok = True
            for slot in self._slots:
                if slot is None:
                    continue
                ok = slot.actor.start() and ok
            if ok:
                self._started_event.set()
            else:
                self._started_event.clear()
            return ok

    def stop(self, *, drain_and_cancel: bool = True) -> bool:
        """모든 Actor를 정지합니다."""
        with self._lifecycle_lock:
            if not self._started_event.is_set():
                # 이미 정지 상태로 간주
                return True

            # stop 중에는 steal 시도를 막기 위해 즉시 내려둡니다.
            self._started_event.clear()

            ok = True
            for slot in self._slots:
                if slot is None:
                    continue
                ok = slot.actor.stop(drain_and_cancel=drain_and_cancel) and ok  # 모든 actor가 성공해야 ok
            return ok

    def add_actor(self, *, name: Optional[str] = None) -> int:
        """풀에 Actor를 동적으로 추가하고, 새 actor_id를 반환합니다.

        - retire로 빈 슬롯이 있으면 그 슬롯을 재사용합니다.
        - retire된 액터의 리소스는 remove_actor()에서 stop+join 후 참조 제거로 해제됩니다.
        """
        with self._lifecycle_lock:
            actor_id = self._create_slot(reuse_free_slot=True)
            actor = self._actors_by_id[actor_id]
            if name is not None:
                actor.name = name

            if self._started_event.is_set():
                actor.start()
            self._refresh_steal_fns()
            return actor_id

    def remove_actor(self, actor_id: int, *, drain_and_cancel: bool = True) -> bool:
        """actor_id를 retire(중지 + 슬롯 비움 + 참조 제거)합니다."""
        with self._lifecycle_lock:
            actor = self._actors_by_id.get(actor_id)
            if actor is None:
                return False

            ok = actor.stop(drain_and_cancel=drain_and_cancel)

            for i, slot in enumerate(self._slots):
                if slot is not None and slot.actor_id == actor_id:
                    self._slots[i] = None
                    break
            self._actors_by_id.pop(actor_id, None)
            self._refresh_steal_fns()
            return ok

    def restart(self) -> bool:
        # stop()/start()가 각각 lifecycle_lock을 잡으므로
        # restart에서 다시 lock을 잡으면 비재진입 Lock 특성상 데드락이 납니다.
        self.stop(drain_and_cancel=False)
        return self.start()

    def submit(
        self,
        func: Callable[..., Any],
        /,
        *args: Any,
        actor_id: Optional[int] = None,
        **kwargs: Any,
    ) -> Future:
        """작업을 pool에 제출하고 Future를 반환합니다.

        라우팅 우선순위:
        1) actor_id (명시 지정)
        2) least-loaded (대기+실행 부하가 가장 작은 actor)
        """

        with self._lifecycle_lock:
            if not self._started_event.is_set():
                raise RuntimeError("ActorPool is not started. Call start() first.")
            decision = self._router.decide(actor_id=actor_id)
            actor = self._get_actor(decision.actor_id)

        fut = actor.submit(func, *args, stealable=decision.stealable, **kwargs)

        # 유휴 Actor들이 steal 시도를 할 수 있도록 깨웁니다.
        for slot in self._slots:
            if slot is None:
                continue
            slot.actor.wake()

        return fut

    def histories(self) -> Dict[int, Tuple[ActorHistoryEntry, ...]]:
        """각 Actor의 history 스냅샷을 actor_id 기준으로 반환합니다."""
        return {actor_id: actor.history for actor_id, actor in self._actors_by_id.items()}

    def __enter__(self) -> "ActorPool":
        if not self.start():
            raise RuntimeError("Failed to start ActorPool")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop(drain_and_cancel=True)
