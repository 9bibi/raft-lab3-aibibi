#!/usr/bin/env python3
import requests
import sys
import json

def send_command(node_address, command):
    """Send a command to a Raft node"""
    try:
        response = requests.post(
            f"http://{node_address}/command",
            json={'command': command},
            timeout=5
        )
        result = response.json()
        print(f"Response: {json.dumps(result, indent=2)}")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_status(node_address):
    """Get status of a Raft node"""
    try:
        response = requests.get(
            f"http://{node_address}/status",
            timeout=5
        )
        result = response.json()
        print(f"\n{'='*50}")
        print(f"Node: {result['node_id']}")
        print(f"State: {result['state']}")
        print(f"Term: {result['term']}")
        print(f"Commit Index: {result['commit_index']}")
        print(f"Log entries: {len(result['log'])}")
        for i, entry in enumerate(result['log']):
            committed = " [COMMITTED]" if i <= result['commit_index'] else ""
            print(f"  [{i}] term={entry['term']}, cmd={entry['command']}{committed}")
        print(f"{'='*50}\n")
        return result
    except Exception as e:
        print(f"Error getting status: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python3 client.py <node_address:port> command <command>")
        print("  python3 client.py <node_address:port> status")
        print("\nExamples:")
        print("  python3 client.py 10.0.1.10:8000 command 'SET x=5'")
        print("  python3 client.py 10.0.1.10:8000 status")
        sys.exit(1)
    
    node_address = sys.argv[1]
    action = sys.argv[2]
    
    if action == "command":
        if len(sys.argv) < 4:
            print("Error: command text required")
            sys.exit(1)
        command = sys.argv[3]
        send_command(node_address, command)
    elif action == "status":
        get_status(node_address)
    else:
        print(f"Unknown action: {action}")