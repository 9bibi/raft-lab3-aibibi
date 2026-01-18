#!/usr/bin/env python3
import argparse
import threading
import time
import random
import requests
from flask import Flask, request, jsonify
from datetime import datetime

class RaftNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of (peer_id, peer_address:port)
        
        # Raft state
        self.state = "Follower"  # Follower, Candidate, Leader
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of (term, command)
        self.commit_index = -1
        
        # Leader state
        self.next_index = {}
        self.match_index = {}
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5, 10)  # 5-10 seconds
        self.heartbeat_interval = 2  # 2 seconds
        
        # Flask app
        self.app = Flask(__name__)
        self.setup_routes()
        
        # Locks
        self.lock = threading.Lock()
        
    def log_message(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [Node {self.node_id}] {message}")
    
    def setup_routes(self):
        @self.app.route('/request_vote', methods=['POST'])
        def request_vote():
            data = request.json
            response = self.handle_request_vote(data)
            return jsonify(response)
        
        @self.app.route('/append_entries', methods=['POST'])
        def append_entries():
            data = request.json
            response = self.handle_append_entries(data)
            return jsonify(response)
        
        @self.app.route('/command', methods=['POST'])
        def command():
            data = request.json
            response = self.handle_client_command(data)
            return jsonify(response)
        
        @self.app.route('/status', methods=['GET'])
        def status():
            return jsonify({
                'node_id': self.node_id,
                'state': self.state,
                'term': self.current_term,
                'log': self.log,
                'commit_index': self.commit_index
            })
    
    def handle_request_vote(self, data):
        with self.lock:
            term = data['term']
            candidate_id = data['candidate_id']
            
            self.log_message(f"Received RequestVote from {candidate_id} (term={term})")
            
            # Update term if necessary
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "Follower"
            
            # Grant vote if haven't voted or already voted for this candidate
            vote_granted = False
            if term >= self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                self.log_message(f"Granted vote to {candidate_id} (term={term})")
            else:
                self.log_message(f"Denied vote to {candidate_id} (term={term})")
            
            return {
                'term': self.current_term,
                'vote_granted': vote_granted
            }
    
    def handle_append_entries(self, data):
        with self.lock:
            term = data['term']
            leader_id = data['leader_id']
            entries = data.get('entries', [])
            leader_commit = data.get('leader_commit', -1)
            
            # Update term if necessary
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "Follower"
            
            # Reset election timeout (received heartbeat)
            self.last_heartbeat = time.time()
            
            success = False
            if term >= self.current_term:
                self.state = "Follower"
                success = True
                
                # Append entries if any
                if entries:
                    for entry in entries:
                        self.log.append(entry)
                        self.log_message(f"Appended entry: {entry}")
                
                # Update commit index
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.log) - 1)
                    self.log_message(f"Updated commit_index to {self.commit_index}")
            
            return {
                'term': self.current_term,
                'success': success
            }
    
    def handle_client_command(self, data):
        with self.lock:
            if self.state != "Leader":
                return {
                    'success': False,
                    'message': f'Not leader. Current state: {self.state}'
                }
            
            command = data['command']
            self.log_message(f"Received client command: {command}")
            
            # Append to own log
            entry = {'term': self.current_term, 'command': command}
            self.log.append(entry)
            log_index = len(self.log) - 1
            self.log_message(f"Appended to log (index={log_index}, term={self.current_term})")
            
            return {
                'success': True,
                'message': 'Command appended',
                'log_index': log_index
            }
    
    def start_election(self):
        with self.lock:
            self.state = "Candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            votes_received = 1  # Vote for self
            
            self.log_message(f"Timeout → Candidate (term {self.current_term})")
            
            term = self.current_term
        
        # Request votes from peers
        for peer_id, peer_addr in self.peers:
            try:
                response = requests.post(
                    f"http://{peer_addr}/request_vote",
                    json={'term': term, 'candidate_id': self.node_id},
                    timeout=2
                )
                if response.json().get('vote_granted'):
                    votes_received += 1
                    self.log_message(f"Received vote from {peer_id}")
            except:
                pass
        
        # Check if won election
        majority = (len(self.peers) + 1) // 2 + 1
        with self.lock:
            if self.state == "Candidate" and votes_received >= majority:
                self.state = "Leader"
                self.log_message(f"Received {votes_received} votes → Leader")
                
                # Initialize leader state
                for peer_id, _ in self.peers:
                    self.next_index[peer_id] = len(self.log)
                    self.match_index[peer_id] = -1
    
    def send_heartbeats(self):
        with self.lock:
            if self.state != "Leader":
                return
            term = self.current_term
            commit_index = self.commit_index
        
        # Send AppendEntries to all peers
        for peer_id, peer_addr in self.peers:
            try:
                # Get entries to send
                with self.lock:
                    next_idx = self.next_index.get(peer_id, len(self.log))
                    entries = self.log[next_idx:] if next_idx < len(self.log) else []
                
                response = requests.post(
                    f"http://{peer_addr}/append_entries",
                    json={
                        'term': term,
                        'leader_id': self.node_id,
                        'entries': entries,
                        'leader_commit': commit_index
                    },
                    timeout=2
                )
                
                if response.json().get('success') and entries:
                    with self.lock:
                        self.next_index[peer_id] = len(self.log)
                        self.match_index[peer_id] = len(self.log) - 1
                        self.log_message(f"Received AppendSuccess from {peer_id}")
                        
            except:
                pass
        
        # Update commit index based on majority
        self.update_commit_index()
    
    def update_commit_index(self):
        with self.lock:
            if self.state != "Leader":
                return
            
            # Find highest index replicated on majority
            for i in range(len(self.log) - 1, self.commit_index, -1):
                replicas = 1  # Leader has it
                for peer_id in self.match_index:
                    if self.match_index[peer_id] >= i:
                        replicas += 1
                
                majority = (len(self.peers) + 1) // 2 + 1
                if replicas >= majority and self.log[i]['term'] == self.current_term:
                    self.commit_index = i
                    self.log_message(f"Entry committed (index={i})")
                    break
    
    def election_timer(self):
        while True:
            time.sleep(1)
            
            with self.lock:
                if self.state == "Leader":
                    continue
                
                elapsed = time.time() - self.last_heartbeat
                if elapsed > self.election_timeout:
                    # Reset timeout
                    self.election_timeout = random.uniform(5, 10)
            
            # Start election (outside lock)
            if elapsed > self.election_timeout:
                self.start_election()
    
    def heartbeat_timer(self):
        while True:
            time.sleep(self.heartbeat_interval)
            self.send_heartbeats()
    
    def run(self):
        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_timer, daemon=True).start()
        
        self.log_message(f"Started on port {self.port}")
        self.log_message(f"Peers: {[p[0] for p in self.peers]}")
        
        # Run Flask app
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, help='Node ID (A, B, C)')
    parser.add_argument('--port', type=int, required=True, help='Port number')
    parser.add_argument('--peers', required=True, help='Comma-separated peer addresses (id:host:port)')
    
    args = parser.parse_args()
    
    # Parse peers: "A:10.0.1.10:8000,C:10.0.1.12:8002"
    peers = []
    for peer in args.peers.split(','):
        parts = peer.split(':')
        peer_id = parts[0]
        peer_host = parts[1]
        peer_port = parts[2]
        peers.append((peer_id, f"{peer_host}:{peer_port}"))
    
    node = RaftNode(args.id, args.port, peers)
    node.run()