#!/usr/bin/env python3
"""
Test script for the self-hosted Neo4j Modal deployment.

Usage:
    python test_neo4j_deployment.py https://your-url.modal.run
"""

import sys
import requests
import json


def test_health(base_url: str):
    """Test health endpoint."""
    print("Testing /health endpoint...")
    response = requests.get(f"{base_url}/health")
    response.raise_for_status()
    print(f"✓ Health check passed: {response.json()}")
    return True


def test_probe(base_url: str):
    """Test Neo4j connection probe."""
    print("\nTesting /probe endpoint...")
    response = requests.get(f"{base_url}/probe")
    response.raise_for_status()
    result = response.json()
    print(f"✓ Neo4j probe passed: {result}")
    return True


def test_create_nodes(base_url: str):
    """Test creating nodes."""
    print("\nTesting node creation...")

    # Create some test nodes
    query = """
    CREATE (alice:Person {name: 'Alice', age: 30})
    CREATE (bob:Person {name: 'Bob', age: 25})
    CREATE (charlie:Person {name: 'Charlie', age: 35})
    CREATE (alice)-[:KNOWS]->(bob)
    CREATE (bob)-[:KNOWS]->(charlie)
    RETURN alice, bob, charlie
    """

    response = requests.post(
        f"{base_url}/query",
        json={"query": query}
    )
    response.raise_for_status()
    results = response.json()["results"]
    print(f"✓ Created {len(results)} nodes with relationships")
    return True


def test_query_nodes(base_url: str):
    """Test querying nodes."""
    print("\nTesting node queries...")

    query = "MATCH (p:Person) RETURN p.name as name, p.age as age ORDER BY p.age"

    response = requests.post(
        f"{base_url}/query",
        json={"query": query}
    )
    response.raise_for_status()
    results = response.json()["results"]

    print(f"✓ Found {len(results)} people:")
    for person in results:
        print(f"  - {person['name']}, age {person['age']}")
    return True


def test_parameterized_query(base_url: str):
    """Test parameterized queries."""
    print("\nTesting parameterized queries...")

    query = "MATCH (p:Person {name: $name}) RETURN p.name as name, p.age as age"
    params = {"name": "Alice"}

    response = requests.post(
        f"{base_url}/query",
        json={"query": query, "parameters": params}
    )
    response.raise_for_status()
    results = response.json()["results"]

    print(f"✓ Found person with parameterized query: {results}")
    return True


def test_relationship_query(base_url: str):
    """Test relationship queries."""
    print("\nTesting relationship queries...")

    query = """
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)
    RETURN p1.name as person1, p2.name as person2
    """

    response = requests.post(
        f"{base_url}/query",
        json={"query": query}
    )
    response.raise_for_status()
    results = response.json()["results"]

    print(f"✓ Found {len(results)} relationships:")
    for rel in results:
        print(f"  - {rel['person1']} knows {rel['person2']}")
    return True


def test_cleanup(base_url: str):
    """Clean up test data."""
    print("\nCleaning up test data...")

    query = "MATCH (n:Person) DETACH DELETE n"

    response = requests.post(
        f"{base_url}/query",
        json={"query": query}
    )
    response.raise_for_status()
    print("✓ Test data cleaned up")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_neo4j_deployment.py <modal-url>")
        print("\nExample:")
        print("  python test_neo4j_deployment.py https://yourname--neo4j-self-hosted-neo4jservice-serve.modal.run")
        sys.exit(1)

    base_url = sys.argv[1].rstrip('/')

    print(f"Testing Neo4j deployment at: {base_url}")
    print("=" * 60)

    try:
        tests = [
            test_health,
            test_probe,
            test_create_nodes,
            test_query_nodes,
            test_parameterized_query,
            test_relationship_query,
            test_cleanup,
        ]

        for test_func in tests:
            test_func(base_url)

        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("\nYour Neo4j deployment is working correctly.")

    except requests.exceptions.RequestException as e:
        print(f"\n✗ Test failed with error: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
