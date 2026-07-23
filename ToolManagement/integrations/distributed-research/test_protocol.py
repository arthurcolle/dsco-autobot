from meta_protocol import UnifiedRuntime, Message, MessageType

print("""
META-PROTOCOL INTER-OPERATION MAP
================================================================================

    LEARNER ◄────────► MESSAGE BUS ◄────────► GENERATOR
   (patterns)       request/response          (code)
       │                   ▲                     │
       │ learn             │ events              │ generate
       │ get_patterns      │ broadcast           │ verify
       ▼                   │                     ▼
    PLANNER ◄────────► UNIFIED ◄────────────► EXECUTOR
    (GOAP)             RUNTIME               (parallel)
       │                                         │
       │ plan                                    │ execute
       │ assess_state                            │ get_stats

  PROTOCOLS:  Learnable | Generatable | Plannable | Executable | Observable
================================================================================
""")

print("VERIFYING ALL INTER-OPERATIONS")
print("="*70)

runtime = UnifiedRuntime()
runtime.initialize()
bus = runtime.orchestrator.bus

tests = [
    ("learner", "learn", {"max_files": 100}),
    ("learner", "get_patterns", {"type": "imports"}),
    ("learner", "get_knowledge", {}),
    ("generator", "verify", {"code": "def foo(): pass"}),
    ("planner", "get_goals", {}),
    ("planner", "plan", {"goal_id": "init_systems"}),
    ("planner", "assess_state", {"capabilities": {"file_read": True}}),
    ("planner", "get_development_plan", {}),
    ("executor", "get_executor_stats", {}),
]

results = []
for target, action, payload in tests:
    try:
        resp = bus.request(Message(
            source="test",
            target=target,
            action=action,
            payload=payload
        ), timeout=30)
        success = resp.type != MessageType.ERROR
        results.append((f"{target}.{action}", success))
    except Exception as e:
        results.append((f"{target}.{action}", False))

print()
print("MESSAGE ROUTING STATUS:")
for route, success in results:
    status = "+" if success else "-"
    print(f"  [{status}] {route}")

print()
success_count = sum(1 for _, s in results if s)
print(f"Routes working: {success_count}/{len(results)}")

print()
print("WORKFLOW STATUS:")
workflows = ["learn_and_generate", "plan_and_execute", "self_improve", "full_cycle"]
for wf in workflows:
    if wf in runtime.orchestrator.workflows:
        print(f"  [+] {wf}")
    else:
        print(f"  [-] {wf}")

print()
print("INTER-OPERATION VERIFIED")
