#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "path/to/httplib.h"

// HTTP
httplib::Client cli("http://yhirose.github.io");

// HTTPS
httplib::Client cli("https://yhirose.github.io");

auto res = cli.Get("/hi");
res->status;
res->body;
