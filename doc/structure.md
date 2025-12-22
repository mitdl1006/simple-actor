## simple-actor 코드 구조 (actor.py)

### 1) 정적 구조(클래스/필드 중심): `Actor` 내부 인스턴스가 한눈에 보이도록
```mermaid
---
config:
  theme: neutral
  look: neo
  layout: elk
---
classDiagram
direction LR

class CommandResult {
  +success
  +error
}

class CommandProtocol {
  +name
  +execute()
  +undo()
  +redo()
}

class _Invoker {
  +invoke()
}

class Actor {
  +name
  -_cmd_queue
  -_thread
  -_invoker
  -_stop_flag
  -_history
  -_history_lock

  +start()
  +stop()
  +restart()
  +request_command()
  -_run()
}

%% 내부 구성(Actor가 "가지고 있음")
Actor *-- "1" _Invoker : _invoker
Actor *-- "1" Queue : _cmd_queue
Actor *-- "0..1" Thread : _thread

%% 명령 실행/결과 흐름(핵심만)
Actor ..> CommandProtocol : enqueue
CommandProtocol ..> CommandResult : returns
_Invoker ..> CommandResult : returns
```

---

### 2) “큐에 데이터를 넣는 경로” + 실행 스레드가 소비하는 위치(메서드 콜 관점)
> “엔트리 타는 순서(전체 시나리오)”가 아니라, **어떤 메서드가 어떤 내부 인스턴스를 건드리는지**를 표현합니다.

```mermaid
---
config:
  theme: neutral
  look: neo
  layout: elk
  flowchart:
---
flowchart LR
  subgraph Actor["Actor 인스턴스 내부"]
    Q["_cmd_queue (Queue)"]
    I["_invoker : _Invoker"]
    H["_history (field)"]
    L["_history_lock (field)"]
    E["_stop_flag (field)"]
    T["_thread : Thread (target=_run)"]
  end

  Caller["외부 호출자"] -->|"request_command(...)"| RC["request_command()"]
  RC -- "wrap cmd -> job" --> J["job (Callable)\npartial(cmd.execute, ctx, ...)"]
  J -- "enqueue (Queue.put)" --> Q

  S["start()"] -- "spawn worker" --> T
  T -->|"target=_run()"| P["_run() loop"]
  Q -- "dequeue (Queue.get)" --> J
  P -- "invoke(job)" --> I
  I -- "CommandResult" --> P
  P -- "with _history_lock: append" --> L
  L --> H

  X["stop()"] -- "set _stop_flag" --> E
  E -- "loop checks flag" --> P
```
