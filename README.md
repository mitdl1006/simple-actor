# simple-actor

**파이썬 표준 라이브러리**만으로 구현한 **경량 Actor 모델** 및 **Work Stealing Actor Pool**입니다.
복잡한 동시성 제어(Lock, Semaphore 등) 없이, **"메시지 전달(Message Passing)"**과 **"단일 스레드 순차 실행"**을 통해 안전하게 병렬 처리를 수행하는 방법을 학습하기 위한 교육용 프로젝트입니다.

## 🎯 프로젝트 목표
이 프로젝트는 다음의 동시성 프로그래밍 개념들을 코드로 직접 구현하고 이해하는 것을 목표로 합니다.

1.  **Actor Model**: 상태(State)를 공유하지 않고, 각자 전용 스레드를 가진 Actor들이 메시지(작업)를 주고받으며 협력합니다.
2.  **Future Pattern**: 비동기 작업의 결과를 `concurrent.futures.Future` 객체를 통해 표준적인 방식으로 조회합니다.
3.  **Work Stealing**: 일부 워커(Actor)에 작업이 몰릴 때, 유휴 상태인 워커가 작업을 훔쳐와 처리함으로써 전체 처리량을 높입니다.

## 📂 패키지 구조 (`simple_actor/`)

핵심 코드는 `simple_actor` 패키지 내에 있습니다.

- **`actor.py`**: `Actor` 클래스.
    - 스레드(Thread)와 작업 큐(Deque)를 소유합니다.
    - `submit()`으로 들어온 함수를 순서대로 실행합니다.
    - 실행 이력(History)을 저장합니다.
- **`actor_pool.py`**: `ActorPool` 클래스.
    - 여러 `Actor`를 생성하고 관리합니다.
    - **Least Loaded Routing**: 작업을 가장 한가한 Actor에게 배정합니다.
    - **Work Stealing**: 놀고 있는 Actor가 바쁜 Actor의 작업을 가져옵니다.
- **`scheduling.py`**: 라우팅 및 스틸링 전략(Strategy) 구현체.

## 🚀 사용 방법

### 1. 단일 Actor 사용하기
Actor는 내부적으로 작업을 직렬화(Serialize)하여 실행하므로, 함수 내에서 `Lock` 없이도 스레드 안전(Thread-safe)하게 상태를 변경할 수 있습니다.

```python
from simple_actor.actor import Actor

# 1. Actor 생성 및 시작
actor = Actor(name="MyActor")
actor.start()

# 2. 작업 정의 (공유 자원 접근 시에도 Lock 불필요)
counter = 0
def increment(amount):
    global counter
    counter += amount
    return counter

# 3. 작업 제출 (Future 반환)
future = actor.submit(increment, 10)

# 4. 결과 확인
print(f"Result: {future.result()}")  # 10

# 5. 종료
actor.stop()
```

### 2. Actor Pool 사용하기 (병렬 처리)
`ActorPool`을 사용하면 여러 Actor에게 작업을 분산시켜 병렬로 처리할 수 있습니다.

```python
import time
from simple_actor.actor_pool import ActorPool

def heavy_task(name):
    time.sleep(0.1)
    return f"Hello, {name}!"

# Context Manager로 Pool 관리 (자동 start/stop)
with ActorPool(size=3) as pool:
    futures = []
    for i in range(5):
        # 자동으로 부하가 적은 Actor에게 배정됨
        f = pool.submit(heavy_task, f"User-{i}")
        futures.append(f)
    
    # 결과 출력
    for f in futures:
        print(f.result())
```

## 🧠 핵심 구현 상세 (교육용 노트)

### Actor의 동작 원리
- **Queue & Thread**: 각 Actor는 `collections.deque`와 `threading.Condition`을 사용하여 효율적인 생산자-소비자 패턴을 구현했습니다.
- **순차 실행**: 큐에서 작업을 하나씩 꺼내 실행하므로, Actor 내부 로직은 단일 스레드 환경처럼 작성할 수 있습니다. (GIL의 이점을 활용하거나, I/O 바운드 작업에서 효율적)

### Work Stealing 알고리즘
단순히 작업을 나누어 주는 것(Routing)만으로는 실행 시간이 긴 작업 때문에 특정 Actor만 바빠지는 불균형(Skew)을 해결하기 어렵습니다.
- **Routing**: 작업 제출 시점에는 `LeastLoadedRouting` 전략을 사용하여 대기열이 가장 짧은 Actor를 선택합니다.
- **Stealing**: Actor가 자신의 큐를 모두 비우면(`idle`), `MostLoadedWorkStealer` 전략을 통해 가장 바쁜 동료 Actor의 큐 앞쪽(Stealable한 작업)을 가져와 실행합니다.

## 🧪 테스트 및 검증
`pytest`를 통해 동시성 동작을 검증할 수 있습니다.

```bash
# 전체 테스트 실행
pytest test/

# 주요 테스트 파일
# test/test_pure_actor.py : 단일 Actor 기능 검증
# test/test_actor_pool.py : Pool 라우팅 및 Work Stealing 검증
```

## 📝 요구 사항
- Python 3.10+
- 표준 라이브러리만 사용 (외부 의존성 없음)