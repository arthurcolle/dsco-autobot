#!/usr/bin/env python3
"""Mitogen parallel execution test"""
import mitogen.master
import mitogen.select
import os
import time


def heavy_work(n):
    """Simulate CPU work"""
    import time
    import os
    time.sleep(0.3)
    return {'pid': os.getpid(), 'n': n, 'result': sum(range(n * 100000))}


def get_info():
    """Get system info from remote context"""
    import platform
    import os
    return {
        'pid': os.getpid(),
        'hostname': platform.node(),
        'python': platform.python_version(),
    }


def main():
    broker = mitogen.master.Broker()
    router = mitogen.master.Router(broker)

    # Spawn workers via fork
    num_workers = 4
    workers = [router.fork() for _ in range(num_workers)]
    print(f"Spawned {num_workers} forked workers:")
    for w in workers:
        info = w.call(get_info)
        print(f"  PID {info['pid']}: {w}")

    # Parallel execution demo
    print("\n--- Parallel Work ---")
    start = time.time()

    calls = [w.call_async(heavy_work, i + 1) for i, w in enumerate(workers)]

    for msg in mitogen.select.Select(calls):
        r = msg.unpickle()
        print(f"  Worker {r['pid']}: n={r['n']}, result={r['result']:,}")

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.2f}s (parallel)")
    print(f"Sequential estimate: ~{num_workers * 0.3:.1f}s")

    # Cleanup
    broker.shutdown()
    print("\n✓ Mitogen test complete!")


def ssh_demo(hostname, username='pi'):
    """Demo SSH connection to remote host"""
    broker = mitogen.master.Broker()
    router = mitogen.master.Router(broker)

    print(f"Connecting to {username}@{hostname}...")
    try:
        remote = router.ssh(
            hostname=hostname,
            username=username,
            check_host_keys='ignore',
        )
        print(f"Connected: {remote}")

        info = remote.call(get_info)
        print(f"Remote system:")
        print(f"  hostname: {info['hostname']}")
        print(f"  python: {info['python']}")
        print(f"  pid: {info['pid']}")

        # Run command on remote
        import subprocess
        uptime = remote.call(subprocess.check_output, ['uptime'], text=True)
        print(f"  uptime: {uptime.strip()}")

    except Exception as e:
        print(f"Failed: {e}")
    finally:
        broker.shutdown()


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        # SSH mode: python mitogen_test.py hostname [username]
        hostname = sys.argv[1]
        username = sys.argv[2] if len(sys.argv) > 2 else 'pi'
        ssh_demo(hostname, username)
    else:
        main()
