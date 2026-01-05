from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, List, Optional, Protocol

from .actor import Actor, ActorJob


@dataclass(frozen=True)
class PlacementDecision:
    actor_id: int  # 작업을 배치할 Actor ID
    stealable: bool  # 이 작업이 스틸 가능한지 여부


class PlacementStrategy(Protocol):
    def decide(self, *, actor_id: Optional[int]) -> PlacementDecision: ...


class WorkStealingStrategy(Protocol):
    def try_steal_for(self, thief_id: int) -> Optional[ActorJob]: ...


@dataclass(frozen=True)
class LeastLoadedRouting(PlacementStrategy):
    """라우팅 결정.

    - actor_id가 있으면 그 actor로
    - 없으면 least-loaded로

    steal 정책:
    - actor_id로 라우팅된 작업은 steal 금지
    - actor_id가 None인 경우만 steal 가능
    """

    list_actor_ids: Callable[[], List[int]]  # 사용 가능한 actor ID 목록 반환
    get_actor: Callable[[int], Actor]  # actor ID로 Actor 인스턴스 반환

    def _pick_least_loaded(self, actor_ids: List[int]) -> int:
        if not actor_ids:
            raise RuntimeError("No available actors")
        return min(actor_ids, key=lambda aid: self.get_actor(aid).load())

    def decide(self, *, actor_id: Optional[int]) -> PlacementDecision:
        actor_ids = self.list_actor_ids()
        if actor_id is not None:
            if actor_id not in actor_ids:
                raise RuntimeError("actor_id is not available")
            return PlacementDecision(actor_id=actor_id, stealable=False)

        return PlacementDecision(actor_id=self._pick_least_loaded(actor_ids), stealable=True)


class MostLoadedWorkStealer(WorkStealingStrategy):
    """스틸링 결정.

    - thief가 유휴일 때, 가장 바쁜 victim에서 FIFO(next)로 1개 steal 시도
    - 동시에 여러 thief가 victim 선정을 돌 때 과도한 경합을 줄이기 위해 lock으로 보호
    """

    def __init__(
        self,
        list_actor_ids: Callable[[], List[int]],
        get_actor: Callable[[int], Actor],
        *,
        is_started: Callable[[], bool],
    ) -> None:
        self._list_actor_ids = list_actor_ids
        self._get_actor = get_actor
        self._is_started = is_started
        self._steal_lock: Lock = Lock()

    def try_steal_for(self, thief_id: int) -> Optional[ActorJob]:
        if not self._is_started():
            return None

        actor_ids = self._list_actor_ids()
        if thief_id not in actor_ids:
            return None

        with self._steal_lock:
            candidates = [
                (aid, self._get_actor(aid).load())
                for aid in actor_ids
                if aid != thief_id
            ]

        for victim, _load in sorted(candidates, key=lambda x: x[1], reverse=True):
            job = self._get_actor(victim).steal_next()
            if job is not None:
                return job
        return None
