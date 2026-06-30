# comparisonServer.py
import socket
import pickle
import grpc
import name_service_pb2
import name_service_pb2_grpc
from constMP import NAME_SERVICE_ADDR, NAME_SERVICE_PORT, N

class ComparisonServer:
    def __init__(self, my_port=50678):
        self.port = my_port
        self.server_sock = None
        self.channel = grpc.insecure_channel(f'{NAME_SERVICE_ADDR}:{NAME_SERVICE_PORT}')
        self.ns_stub = name_service_pb2_grpc.NameDirectoryServiceStub(self.channel)

    def __enter__(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', self.port))
        self.server_sock.listen(6)
        
        local_ip = self._get_local_ip()
        req = name_service_pb2.BindRequest(
            name="ComparisonServer", address=name_service_pb2.Address(ip=local_ip, port=self.port)
        )
        self.ns_stub.Bind(req)
        print(f"Comparison Server registado em {local_ip}:{self.port}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.server_sock: self.server_sock.close()
        self.ns_stub.Unbind(name_service_pb2.UnbindRequest(name="ComparisonServer"))

    def _get_local_ip(self):
        return NAME_SERVICE_ADDR

    def _get_peers_via_directory(self):
        res = self.ns_stub.Discover(name_service_pb2.DiscoverRequest(type="peer"))
        return [(p.name, p.address.ip, p.address.port) for p in res.processes]

    def start_peers(self, peer_list, n_msgs):
        for idx, (name, ip, tcp_port) in enumerate(peer_list):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, tcp_port))
                    s.send(pickle.dumps((idx, n_msgs)))
                    s.recv(512)
            except Exception as e: print(f"Falha ao iniciar {name} -> {e}")

    def wait_for_logs_and_compare(self):
        all_peer_docs = []
        print(f"A aguardar convergência dos {N} peers...")

        while len(all_peer_docs) < N:
            conn, addr = self.server_sock.accept()
            with conn:
                data = bytearray()
                while True:
                    packet = conn.recv(65536)
                    if not packet: break
                    data.extend(packet)
                all_peer_docs.append(pickle.loads(data))
                print(f"Estado CRDT recebido ({len(all_peer_docs)}/{N})")

        self.compare_final_documents(all_peer_docs)

    def compare_final_documents(self, all_peer_docs):
        divergencias = 0
        doc_referencia = all_peer_docs[0]
        texto_referencia = "".join([el["char"] for el in doc_referencia])
        
        for p in range(1, len(all_peer_docs)):
            doc_analisado = all_peer_docs[p]
            texto_analisado = "".join([el["char"] for el in doc_analisado])
            if len(doc_analisado) != len(doc_referencia) or texto_analisado != texto_referencia:
                divergencias += 1

        print(f"\n--- Resultado da Consistência Forte (CRDT) ---")
        if divergencias == 0:
            print("SUCESSO: Todos os editores convergiram para o MESMO texto final!")
            print(f"Texto Consolidado: \"{texto_referencia}\"")
        else:
            print(f"DIVERGÊNCIA: {divergencias} nós possuem estados de texto diferentes.")
        print(f"----------------------------------------------\n")

    def run(self):
        while True:
            try:
                n_msgs = int(input('Operações por peer (0 para sair) => '))
            except ValueError: continue

            peer_list = self._get_peers_via_directory()
            if n_msgs == 0:
                self.start_peers(peer_list, 0)
                break

            self.start_peers(peer_list, n_msgs)
            self.wait_for_logs_and_compare()

if __name__ == "__main__":
    with ComparisonServer() as server:
        server.run()