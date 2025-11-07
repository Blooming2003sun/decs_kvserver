#include "civetweb.h"
#include <pqxx/pqxx>
#include <iostream>
#include <string>
#include <memory>

using namespace std;

class KVHandler {
public:
    KVHandler(pqxx::connection &conn) : db(conn) {}

    static int handle(struct mg_connection *conn, void *cbdata) {
        KVHandler *self = static_cast<KVHandler *>(cbdata);
        const struct mg_request_info *ri = mg_get_request_info(conn);

        string method = ri->request_method;
        string uri = ri->local_uri;
        string key = uri.substr(strlen("/kv/")); // Extract key from /kv/<key>

        if (method == "PUT") return self->handlePut(conn, key);
        if (method == "GET") return self->handleGet(conn, key);
        if (method == "DELETE") return self->handleDelete(conn, key);

        mg_send_http_error(conn, 405, "Method Not Allowed");
        return 405;
    }

private:
    pqxx::connection &db;

    int handlePut(struct mg_connection *conn, const string &key) {
        char buf[4096];
        int len = mg_read(conn, buf, sizeof(buf));
        string value(buf, len);

        try {
            pqxx::work txn(db);
            txn.exec_params(
                "INSERT INTO kv_store (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value);
            txn.commit();
        } catch (const std::exception &e) {
            mg_send_http_error(conn, 500, e.what());
            return 500;
        }

        mg_send_http_ok(conn, "text/plain", 0);
        return 200;
    }

    int handleGet(struct mg_connection *conn, const string &key) {
        try {
            pqxx::read_transaction txn(db);
            pqxx::result r = txn.exec_params("SELECT value FROM kv_store WHERE key=$1", key);

            if (r.empty()) {
                mg_send_http_error(conn, 404, "Key not found");
                return 404;
            }

            string value = r[0][0].as<string>();
            mg_send_http_ok(conn, "text/plain", value.size());
            mg_write(conn, value.data(), value.size());
            return 200;
        } catch (const std::exception &e) {
            mg_send_http_error(conn, 500, e.what());
            return 500;
        }
    }

    int handleDelete(struct mg_connection *conn, const string &key) {
        try {
            pqxx::work txn(db);
            txn.exec_params("DELETE FROM kv_store WHERE key=$1", key);
            txn.commit();
        } catch (const std::exception &e) {
            mg_send_http_error(conn, 500, e.what());
            return 500;
        }

        mg_send_http_ok(conn, "text/plain", 0);
        return 200;
    }
};

int main() {
    const char *options[] = {"listening_ports", "8080", NULL};

    // Connect to PostgreSQL
    try {
        pqxx::connection db("postgresql://user:password@localhost/kvdb");
        if (!db.is_open()) {
            cerr << "Cannot open database\n";
            return 1;
        }

        cout << "Connected to database: " << db.dbname() << endl;

        mg_init_library(0);
        struct mg_context *ctx = mg_start(NULL, 0, options);

        // Register handler for /kv/*
        KVHandler handler(db);
        mg_set_request_handler(ctx, "/kv/", KVHandler::handle, &handler);

        cout << "Server started at http://localhost:8080\n";
        cout << "Press Enter to quit.\n";
        getchar();

        mg_stop(ctx);
        mg_exit_library();

    } catch (const std::exception &e) {
        cerr << "Database error: " << e.what() << endl;
        return 1;
    }

    return 0;
}

