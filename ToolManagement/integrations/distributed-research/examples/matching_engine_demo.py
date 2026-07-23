"""
Matching Engine Demo - Demonstrates autonomous job routing to 2300 domain agents.

This example shows how to:
1. Submit jobs to the matching engine
2. Automatically route to optimal workers
3. Track job execution
4. Retrieve results
"""

import asyncio
import httpx
import time


BASE_URL = "http://127.0.0.1:2040"


async def submit_job(client: httpx.AsyncClient, job_data: dict) -> dict:
    """Submit a job to the matching engine."""
    response = await client.post(f"{BASE_URL}/v1/matching/jobs", json=job_data)
    response.raise_for_status()
    return response.json()


async def get_job_status(client: httpx.AsyncClient, job_id: str) -> dict:
    """Get job status."""
    response = await client.get(f"{BASE_URL}/v1/matching/jobs/{job_id}")
    response.raise_for_status()
    return response.json()


async def get_job_result(client: httpx.AsyncClient, job_id: str, wait: bool = True) -> dict:
    """Get job result, optionally waiting for completion."""
    params = {"wait": "true"} if wait else {}
    response = await client.get(f"{BASE_URL}/v1/matching/jobs/{job_id}/result", params=params)
    response.raise_for_status()
    return response.json()


async def get_system_stats(client: httpx.AsyncClient) -> dict:
    """Get system statistics."""
    response = await client.get(f"{BASE_URL}/v1/matching/stats")
    response.raise_for_status()
    return response.json()


async def list_workers(client: httpx.AsyncClient, domain: str = None, limit: int = 10) -> list:
    """List available workers."""
    params = {"limit": limit}
    if domain:
        params["domain"] = domain
    response = await client.get(f"{BASE_URL}/v1/matching/workers", params=params)
    response.raise_for_status()
    return response.json()


async def demo_financial_analysis():
    """Demo: Financial analysis job routed to finance domain agent."""
    print("\n" + "="*70)
    print("DEMO 1: Financial Analysis")
    print("="*70)

    async with httpx.AsyncClient(timeout=60.0) as client:
        job = {
            "description": "Analyze the Q4 2024 earnings report for Tesla and provide key insights on revenue growth, profit margins, and future outlook",
            "job_type": "research",
            "priority": "high",
            "requirements": {
                "min_quality_score": 0.85,
                "max_cost_usd": 2.0,
                "preferred_domains": ["finance"],
                "required_capabilities": ["research", "analysis"]
            }
        }

        print("\n1. Submitting job...")
        print(f"   Description: {job['description'][:80]}...")
        submission = await submit_job(client, job)
        job_id = submission["job_id"]
        print(f"   ✓ Job submitted: {job_id}")
        print(f"   ✓ Matched to: {submission['matched_workers']}")
        print(f"   ✓ Estimated cost: ${submission['estimated_cost_usd']}")
        print(f"   ✓ Estimated duration: {submission['estimated_duration_seconds']}s")

        print("\n2. Polling for completion...")
        for i in range(10):
            status = await get_job_status(client, job_id)
            print(f"   [{i+1}] Status: {status['status']} (Progress: {status['progress_pct']}%)")

            if status['status'] in ['completed', 'failed']:
                break

            await asyncio.sleep(1)

        if status['status'] == 'completed':
            print("\n3. Retrieving result...")
            result = await get_job_result(client, job_id, wait=False)
            print(f"   ✓ Completed by worker: {result['worker_id']}")
            print(f"   ✓ Quality score: {result['quality_score']}")
            print(f"   ✓ Actual cost: ${result['actual_cost_usd']}")
            print(f"   ✓ Duration: {result['actual_duration_seconds']}s")
            print(f"\n   Output preview:")
            print(f"   {str(result['output'])[:200]}...")


async def demo_multi_domain_batch():
    """Demo: Submit multiple jobs across different domains simultaneously."""
    print("\n" + "="*70)
    print("DEMO 2: Multi-Domain Batch Processing")
    print("="*70)

    jobs = [
        {
            "description": "Write a comprehensive guide on deep learning architectures for computer vision",
            "job_type": "content_creation",
            "priority": "normal",
            "requirements": {"preferred_domains": ["technology", "data_science"]}
        },
        {
            "description": "Analyze recent Supreme Court decisions on privacy law",
            "job_type": "research",
            "priority": "normal",
            "requirements": {"preferred_domains": ["law"]}
        },
        {
            "description": "Review latest research on mRNA vaccine technology",
            "job_type": "research",
            "priority": "high",
            "requirements": {"preferred_domains": ["medicine", "science"]}
        },
        {
            "description": "Create a marketing strategy for a B2B SaaS startup targeting enterprise clients",
            "job_type": "analysis",
            "priority": "normal",
            "requirements": {"preferred_domains": ["marketing", "business"]}
        },
        {
            "description": "Generate Python code for a distributed task queue with priority scheduling",
            "job_type": "code_generation",
            "priority": "urgent",
            "requirements": {
                "preferred_domains": ["technology"],
                "required_capabilities": ["code_execution"],
                "computer_use_required": True
            }
        }
    ]

    async with httpx.AsyncClient(timeout=60.0) as client:
        print(f"\n1. Submitting {len(jobs)} jobs in parallel...")

        # Submit all jobs concurrently
        tasks = [submit_job(client, job) for job in jobs]
        submissions = await asyncio.gather(*tasks)

        print(f"\n   ✓ All {len(submissions)} jobs submitted")
        print("\n   Job Summary:")
        for i, sub in enumerate(submissions, 1):
            print(f"   [{i}] {sub['job_id']}")
            print(f"       Domain: {jobs[i-1]['requirements'].get('preferred_domains', ['general'])[0]}")
            print(f"       Worker: {sub['matched_workers'][0] if sub.get('matched_workers') else 'N/A'}")
            print(f"       Cost: ${sub.get('estimated_cost_usd', 0)}")

        total_cost = sum(s.get('estimated_cost_usd', 0) for s in submissions)
        print(f"\n   Total estimated cost: ${total_cost:.2f}")


async def demo_worker_discovery():
    """Demo: Discover and explore available workers."""
    print("\n" + "="*70)
    print("DEMO 3: Worker Discovery & Capabilities")
    print("="*70)

    async with httpx.AsyncClient(timeout=60.0) as client:
        # Get workers by domain
        domains = ["finance", "technology", "medicine", "law"]

        for domain in domains:
            workers = await list_workers(client, domain=domain, limit=5)
            print(f"\n{domain.upper()} Specialists ({len(workers)} shown):")

            for worker in workers[:3]:
                print(f"\n  • {worker['name']}")
                print(f"    ID: {worker['agent_id']}")
                print(f"    Domain: {worker['domain']}")
                print(f"    Quality: {worker['avg_quality_score']:.2f}")
                print(f"    Cost: ${worker['cost_per_task_usd']}")
                print(f"    Capabilities: {', '.join(c['name'] for c in worker['capabilities'][:3])}")
                print(f"    Current Load: {worker['current_load']}/{worker['max_concurrent_tasks']}")


async def demo_system_monitoring():
    """Demo: Monitor system performance and analytics."""
    print("\n" + "="*70)
    print("DEMO 4: System Monitoring & Analytics")
    print("="*70)

    async with httpx.AsyncClient(timeout=60.0) as client:
        stats = await get_system_stats(client)

        print("\n📊 System Statistics")
        print(f"   Total Workers: {stats['total_workers_available'] + stats['total_workers_busy']}")
        print(f"   Available: {stats['total_workers_available']}")
        print(f"   Busy: {stats['total_workers_busy']}")

        print(f"\n📋 Job Statistics")
        print(f"   Pending: {stats['total_jobs_pending']}")
        print(f"   In Progress: {stats['total_jobs_in_progress']}")
        print(f"   Completed Today: {stats['total_jobs_completed_today']}")

        print(f"\n⚡ Performance Metrics")
        print(f"   Avg Job Duration: {stats['avg_job_duration_seconds']:.1f}s")
        print(f"   Avg Match Time: {stats['avg_match_time_ms']:.1f}ms")

        print(f"\n💰 Cost Analytics")
        print(f"   Total Cost Today: ${stats['total_cost_today_usd']:.2f}")


async def main():
    """Run all demos."""
    print("\n" + "🚀" * 35)
    print("   MATCHING ENGINE DEMONSTRATION")
    print("   2300 Specialized AI Domain Agents")
    print("   Autonomous Job Routing at Scale")
    print("🚀" * 35)

    # Check if server is running
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{BASE_URL}/health")
            response.raise_for_status()
            print("\n✓ Server is running")
    except Exception as e:
        print(f"\n✗ Error: Server not running at {BASE_URL}")
        print(f"  Please start the server first: python -m parallel_websearch_server.main")
        return

    # Run demos
    try:
        await demo_worker_discovery()
        await demo_financial_analysis()
        await demo_multi_domain_batch()

        # Wait a bit for jobs to process
        print("\n\n⏳ Waiting for jobs to process...")
        await asyncio.sleep(3)

        await demo_system_monitoring()

    except Exception as e:
        print(f"\n✗ Error during demo: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*70)
    print("DEMO COMPLETE")
    print("="*70)
    print("\nNext Steps:")
    print("  • View API docs: http://127.0.0.1:2040/docs")
    print("  • Explore workers: GET /v1/matching/workers")
    print("  • Submit jobs: POST /v1/matching/jobs")
    print("  • Monitor stats: GET /v1/matching/stats")
    print("\n")


if __name__ == "__main__":
    asyncio.run(main())
