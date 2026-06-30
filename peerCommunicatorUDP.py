# peerCommunicatorUDP.py
import threading
import random
import time
import pickle
import sys
from socket import *
from requests import get

import grpc
import name_service_pb2
import name_service_pb2_grpc
from constMP import NAME_SERVICE_ADDR, NAME_SERVICE_PORT, N

class CRDTDocument:
    def __init__(self):
        # Lista de elementos: {"pos": float, "char": str, "ts": float, "user": int}
        self.elements = []

    def generate_structured_float(self, index_inteiro):
        if not self.elements:
            return 0.5
        if index_inteiro <= 0:
            return self.elements[0]["pos"] / 2.0
        if index_inteiro >= len(self.elements):
            return self.elements[-1]["pos"] + 0.5
        
        pos_anterior = self.elements[index_inteiro - 1]["pos"]
        pos_atual = self.elements[index_inteiro]["pos"]
        return (pos_anterior + pos_atual) / 2.0

    def apply_operation(self, op):
        print(op)
        if op["type"] == "INSERT":
            if not any(el["pos"] == op["pos"] for el in self.elements):
                self.elements.append({
                    "pos": op["pos"], "char": op["char"], "ts": op["timestamp"], "user": op["user"]
                })
                # Ordenação matemática comutativa. Desempata pelo TS do criador se as posições forem idênticas
                self.elements.sort(key=lambda x: (x["pos"], x["ts"]))
        elif op["type"] == "DELETE":
            self.elements = [x for x in self.elements if x["pos"] != op["pos"]]

    def get_log_state(self):
        return list(self.elements)


class PeerNode:
    def __init__(self, peer_name, udp_port, tcp_port):
        self.my_name = peer_name
        self.my_id = None
        self.num_msgs = 0
        self.peers = [] 
        self.handshake_count = 0
        self.lock = threading.Lock()
        
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.doc = CRDTDocument()

        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.recv_socket.bind(('0.0.0.0', self.udp_port))

        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', self.tcp_port))
        self.server_sock.listen(1)

        self.channel = grpc.insecure_channel(f'{NAME_SERVICE_ADDR}:{NAME_SERVICE_PORT}')
        self.ns_stub = name_service_pb2_grpc.NameDirectoryServiceStub(self.channel)

    def get_public_ip(self):
        try:
            return get('https://api.ipify.org').content.decode('utf8')
        except Exception:
            return '127.0.0.1'

    def register_and_advertise(self):
        ip_addr = self.get_public_ip()
        bind_req = name_service_pb2.BindRequest(
            name=self.my_name, address=name_service_pb2.Address(ip=ip_addr, port=self.tcp_port)
        )
        res1 = self.ns_stub.Bind(bind_req)
        if res1.success:
            self.ns_stub.RegisterType(name_service_pb2.RegisterTypeRequest(name=self.my_name, type="peer"))
            print(f"[{self.my_name}] Registado com sucesso via gRPC.")
        else:
            print(f"Erro ao registar {self.my_name}: {res1.error_message}")

    def update_peer_list_via_directory(self):
        response = self.ns_stub.Discover(name_service_pb2.DiscoverRequest(type="peer"))
        self.peers = []
        for proc in response.processes:
            if proc.name != self.my_name:
                # Mapeamento para testes locais: assume-se uma porta UDP com base no registo TCP remoto
                remote_udp = proc.address.port + 1000
                self.peers.append((proc.address.ip, remote_udp))

    def wait_for_start_signal(self):
        print(f'[{self.my_name}] A escutar sinal TCP de início na porta {self.tcp_port}...')
        conn, addr = self.server_sock.accept()
        msg = pickle.loads(conn.recv(1024))
        self.my_id = msg[0]
        self.num_msgs = msg[1]
        
        conn.send(pickle.dumps(f'Peer {self.my_name} ativo.'))
        conn.close()
        return self.my_id, self.num_msgs

    def broadcast_handshake(self):
        for ip, udp_p in self.peers:
            self.send_socket.sendto(pickle.dumps(('READY', self.my_id)), (ip, udp_p))

    def broadcast_messages(self):
        while True:
            with self.lock:
                if self.handshake_count >= N - 1: break
            time.sleep(0.1)

        chars = "abcdefghijklmnopqrstuvwxyz "
        for _ in range(self.num_msgs):
            # Aumentando o tempo mínimo de sleep (ex: de 0.1 a 0.3 segundos) 
            # para dar vazão estável ao UDP no localhost
            time.sleep(random.uniform(0.1, 0.3)) 
            with self.lock:
                tipo_op = random.choices(["INSERT", "DELETE"], weights=[0.8, 0.2])[0]
                
                if tipo_op == "INSERT" or not self.doc.elements:
                    pos_base = random.randint(1, 1000) / 1000.0
                    float_pos = pos_base + (self.my_id * 0.00001)
                    
                    op = {
                        "user": self.my_id,
                        "type": "INSERT",
                        "char": random.choice(chars),
                        "pos": float_pos,
                        "timestamp": time.time()
                    }
                else:
                    alvo = random.choice(self.doc.elements)
                    op = {
                        "user": self.my_id,
                        "type": "DELETE",
                        "char": alvo["char"],
                        "pos": alvo["pos"],
                        "timestamp": time.time()
                    }
                self.doc.apply_operation(op)

            msg_pack = pickle.dumps(op)
            for ip, udp_p in self.peers:
                self.send_socket.sendto(msg_pack, (ip, udp_p))

        for ip, udp_p in self.peers:
            self.send_socket.sendto(pickle.dumps({"type": "STOP"}), (ip, udp_p))

    def run(self):
        self.register_and_advertise()
        while True:
            self.wait_for_start_signal()
            if self.num_msgs == 0:
                self.ns_stub.Unbind(name_service_pb2.UnbindRequest(name=self.my_name))
                break
            self.handshake_count = 0
            self.doc = CRDTDocument()
            self.update_peer_list_via_directory()
            
            handler = MsgHandler(self)
            handler.start()
            self.broadcast_handshake()
            self.broadcast_messages()
            handler.join()


class MsgHandler(threading.Thread):
    def __init__(self, peer_node):
        super().__init__()
        self.node = peer_node

    def run(self):
        while True:
            with self.node.lock:
                if self.node.handshake_count >= N-1: break
            data = self.node.recv_socket.recv(1024)
            msg = pickle.loads(data)
            if isinstance(msg, tuple) and msg[0] == 'READY':
                with self.node.lock: self.node.handshake_count += 1

        stop_count = 0
        while stop_count < N-1:
            data = self.node.recv_socket.recv(1024)
            msg = pickle.loads(data)
            if isinstance(msg, dict) and msg.get("type") == "STOP":
                stop_count += 1
            else:
                with self.node.lock:
                    self.node.doc.apply_operation(msg)
        time.sleep(1)
        self.report_to_server()

    def report_to_server(self):
        try:
            res = self.node.ns_stub.Lookup(name_service_pb2.LookupRequest(name="ComparisonServer"))
            if res.success:
                final_state = self.node.doc.get_log_state()
                texto_referencia = "".join([el["char"] for el in final_state])
                print(texto_referencia)
                with socket(AF_INET, SOCK_STREAM) as client_sock:
                    client_sock.connect((res.address.ip, res.address.port))
                    client_sock.sendall(pickle.dumps(final_state))
        except Exception as e:
            print(f"Erro ao enviar estado para o servidor: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python peerCommunicatorUDP.py <NomePeer> <PortaUDP>")
        sys.exit(1)
    PeerNode(sys.argv[1], int(sys.argv[2])+1000, int(sys.argv[2])).run()