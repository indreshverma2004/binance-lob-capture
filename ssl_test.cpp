#include <winsock2.h>
#include <ws2tcpip.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <iostream>
int main(){
  WSADATA wsaData; WSAStartup(MAKEWORD(2,2), &wsaData);
  SSL_library_init();
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) { std::cerr << "ctx fail\n"; return 1; }
  if (SSL_CTX_load_verify_file(ctx, "C:/msys64/usr/ssl/certs/ca-bundle.crt") != 1) { std::cerr << "ca load fail\n"; ERR_print_errors_fp(stderr); return 2; }
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
  const SSL_METHOD* method = TLS_client_method();
  SSL* ssl = SSL_new(ctx);
  int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(443); hostent* h = gethostbyname("stream.binance.com"); if (!h) return 3; memcpy(&addr.sin_addr, h->h_addr, h->h_length);
  if (connect(sock, (sockaddr*)&addr, sizeof(addr)) != 0) { std::cerr << "connect fail" << std::endl; return 4; }
  SSL_set_fd(ssl, sock);
  if (SSL_connect(ssl) <= 0) { std::cerr << "ssl connect fail\n"; ERR_print_errors_fp(stderr); return 5; }
  std::cout << "ssl connect ok\n";
  SSL_shutdown(ssl); SSL_free(ssl); closesocket(sock); WSACleanup(); return 0;
}
