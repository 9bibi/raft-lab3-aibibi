# Raft Lite Implementation - Lab 3

## Setup
- 3 EC2 instances (Ubuntu 22.04, t2.micro)
- Python 3 with Flask and requests

## Run Instructions

### Install dependencies:
```bash
pip3 install flask requests
```

### Test with client:
```bash
python3 client.py <leader-ip>:8000 command "SET x=5"
python3 client.py <node-ip>:8000 status
```

## Files
- node.py - Raft node implementation
- client.py - Client for sending commands