#include <iostream>
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <memory>
#include <sstream>
#include <thread>
#include<list>

#include <libpq-fe.h> //-> for db
#include "CivetServer.h"
#include <nlohmann/json.hpp> //fot json

#include <unordered_map> // --- CACHE ---
#include <mutex>         // --- CACHE ---
//#include <scoped_lock>   // --- CACHE ---

// ---CACHE---
    // Define the max number of items for the LRU cache
    const size_t CACHE_MAX_ITEMS = 10000;
    // ---END CACHE---

using json = nlohmann::json;

// ---------- Simple Postgres connection pool ----------
class PGPool
{
private:
    std::string conninfo_;
    std::queue<PGconn *> conns_;
    std::mutex m_;
    std::condition_variable cv_;

public:
    PGPool(const std::string &conninfo, int pool_size = 8)
        : conninfo_(conninfo)
    {
        for (int i = 0; i < pool_size; ++i)
        {
            PGconn *c = PQconnectdb(conninfo_.c_str());
            if (PQstatus(c) != CONNECTION_OK)
            {
                std::cerr << "Postgres connect failed: " << PQerrorMessage(c) << std::endl;
                PQfinish(c);
                throw std::runtime_error("Failed to connect to Postgres");
            }

            // Only the first connection creates the table
            if (i == 0)
            {
                const char *create =
                    "CREATE TABLE IF NOT EXISTS kv_store ("
                    "k TEXT PRIMARY KEY,"
                    "v TEXT,"
                    "updated_at TIMESTAMP DEFAULT now()"
                    ")";
                PGresult *r = PQexec(c, create);
                if (PQresultStatus(r) != PGRES_COMMAND_OK)
                {
                    std::string msg = PQerrorMessage(c);
                    PQclear(r);
                    PQfinish(c);
                    throw std::runtime_error("Failed to create table: " + msg);
                }
                PQclear(r);
            }

            // Prepare statements for this connection
            PQclear(PQprepare(c, "kv_get", "SELECT v FROM kv_store WHERE k=$1", 1, nullptr));
            PQclear(PQprepare(c, "kv_put",
                              "INSERT INTO kv_store(k,v) VALUES($1,$2) "
                              "ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=now()",
                              2, nullptr));
            PQclear(PQprepare(c, "kv_del", "DELETE FROM kv_store WHERE k=$1", 1, nullptr));

            // Push the now fully-initialized connection into the pool
            conns_.push(c);
        }
    }

    // Destructor
    ~PGPool()
    {
        std::lock_guard<std::mutex> lk(m_);
        while (!conns_.empty())
        {
            PQfinish(conns_.front());
            conns_.pop();
        }
    }

    PGconn *acquire()
    {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]
                 { return !conns_.empty(); });
        PGconn *c = conns_.front();
        conns_.pop();
        return c;
    }

    void release(PGconn *c)
    {
        std::lock_guard<std::mutex> lk(m_);
        conns_.push(c);
        cv_.notify_one();
    }
};

// ---------- Helper: URL decode ----------
static std::string url_decode(const std::string &s)
{
    std::string ret;
    char ch;
    int i, ii;
    for (i = 0; i < (int)s.length(); i++)
    {
        if (s[i] == '%')
        {
            sscanf(s.substr(i + 1, 2).c_str(), "%x", &ii);
            ch = static_cast<char>(ii);
            ret += ch;
            i = i + 2;
        }
        else if (s[i] == '+')
        {
            ret += ' ';
        }
        else
        {
            ret += s[i];
        }
    }
    return ret;
}

// ---------- Helper: send JSON ----------
static void send_json(struct mg_connection *conn, int status, const json &j)
{
    std::string body = j.dump();
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status << " \r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n"
        << body;
    std::string s = oss.str();
    mg_write(conn, s.c_str(), (int)s.size());
}

// ---------- LRU CACHE CLASS ----------
/**
 * @brief A thread-safe, fixed-size LRU (Least Recently Used) cache.
 *
 * It combines an std::unordered_map for O(1) lookups and an std::list
 * to maintain the usage order (MRU -> LRU).
 */
class LRUCache {
private:
    // The list stores {key, value} pairs.
    // Front of the list is MRU (Most Recently Used).
    // Back of the list is LRU (Least Recently Used).
    std::list<std::pair<std::string, std::string>> lru_list_;

    // The map stores the key and an *iterator* to its position in the list.
    std::unordered_map<std::string, std::list<std::pair<std::string, std::string>>::iterator> cache_map_;

    size_t max_size_;
    std::mutex cache_mutex_;

public:
    LRUCache(size_t max_size) : max_size_(max_size) {
        // Ensure cache size is at least 1
        if (max_size_ == 0) {
            max_size_ = 1;
        }
    }

    /**
     * @brief Puts a key-value pair into the cache.
     * If the key exists, its value is updated, and it becomes the MRU.
     * If the key is new and the cache is full, the LRU item is evicted.
     */
    void put(const std::string& key, const std::string& value) {
        std::scoped_lock lock(cache_mutex_);

        auto it = cache_map_.find(key);

        // Case 1: Key already in cache. Update value and move to front (MRU).
        if (it != cache_map_.end()) {
            // Update the value in the list
            it->second->second = value;
            // Move the existing list node to the front
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        // Case 2: Key is new.
        // Check if cache is full *before* inserting.
        if (cache_map_.size() >= max_size_) {
            // Evict the LRU item (at the back of the list)
            auto& lru_item = lru_list_.back();
            cache_map_.erase(lru_item.first);
            lru_list_.pop_back();
        }

        // Add the new item to the front (MRU)
        lru_list_.push_front({key, value});
        // Store an iterator to the new item in the map
        cache_map_[key] = lru_list_.begin();
    }

    /**
     * @brief Gets a value by key.
     * If the key is found (HIT), it becomes the MRU.
     * @param key The key to find.
     * @param value_out A reference to store the found value.
     * @return true if found (HIT), false if not found (MISS).
     */
    bool get(const std::string& key, std::string& value_out) {
        std::scoped_lock lock(cache_mutex_);

        auto it = cache_map_.find(key);

        // Case 1: Key not found (MISS)
        if (it == cache_map_.end()) {
            return false;
        }

        // Case 2: Key found (HIT)
        // Move the accessed item to the front (MRU)
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        // Copy the value to the output parameter
        value_out = it->second->second;
        return true;
    }

    /**
     * @brief Erases a key from the cache.
     */
    void erase(const std::string& key) {
        std::scoped_lock lock(cache_mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            lru_list_.erase(it->second);
            cache_map_.erase(it);
        }
    }
};
// ---------- END LRU CACHE CLASS ----------

// ---------- KVHandler ----------
class KVHandler : public CivetHandler
{
private:
    PGPool &pool_;
    // --- CACHE ---
    // Our in-memory cache and the mutex to protect it.
    LRUCache cache_;
    // --- END CACHE ---

    /**
     * @brief --- CACHE ---
     * Loads all existing data from the database into the in-memory cache.
     * This is called a "cache warm-up".
     */
    void warmUpCache(size_t limit)
    {
        std::cout << "Warming up cache from database..." << std::endl;
        PGconn *pg = pool_.acquire();
        std::string query = "SELECT k, v FROM kv_store ORDER BY updated_at DESC LIMIT " + std::to_string(limit);
        PGresult *res = PQexec(pg, query.c_str());

        if (PQresultStatus(res) == PGRES_TUPLES_OK)
        {
            int rows = PQntuples(res);
            //lock inside cache_.put()

            for (int i = 0; i < rows; i++)
            {
                char *key = PQgetvalue(res, i, 0);
                char *val = PQgetvalue(res, i, 1);
                if (key && val)
                {
                    // This will fill the cache up to its max size
                    // and automatically apply the eviction policy
                    cache_.put( std::string(key) , std::string(val));
                }
            }
            std::cout << "Cache warm-up complete. Loaded " << rows << " items.\n";
        }
        else
        {
            std::cerr << "Cache warm-up failed: " << PQerrorMessage(pg) << std::endl;
        }
        PQclear(res);
        pool_.release(pg);
    }

    bool doGet(struct mg_connection *conn, const std::string &key)
    {
        // --- CACHE ---
        // 1. Check cache first
        std::string value;
        if (cache_.get(key, value)) {
            // CACHE HIT!
            send_json(conn, 200, json{
                                    {"key", key},
                                    {"value", value},
                                    {"cache", "HIT"}});
            return true;
        }
        // CACHE MISS.
        // --- END CACHE ---

        PGconn *pg = pool_.acquire();
        const char *paramValues[1] = {key.c_str()};
        PGresult *res = PQexecPrepared(pg, "kv_get", 1, paramValues, nullptr, nullptr, 0);

        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            std::string err = PQerrorMessage(pg);
            PQclear(res);
            pool_.release(pg);
            send_json(conn, 500, json{{"error", "db_error"}, {"message", err}});
            return true;
        }

        if (PQntuples(res) == 0)
        {
            PQclear(res);
            pool_.release(pg);
            send_json(conn, 404, json{{"error", "not_found"}, {"cache", "MISS"}});
            return true;
        }

        char *val = PQgetvalue(res, 0, 0);
        int len = PQgetlength(res, 0, 0);
        std::string db_value(val, len);
        PQclear(res);
        pool_.release(pg);

        // --- CACHE ---
        // 3. Store the retrieved value in the cache
        cache_.put(key, db_value);
        // --- END CACHE ---

        send_json(conn, 200, json{
                                {"key", key},
                                {"value", db_value},
                                {"cache", "MISS"}});
        return true;
    }

    bool doPut(struct mg_connection *conn, const std::string &key, const struct mg_request_info *ri)
    {
        long long len = ri->content_length;
        std::string body;
        if (len > 0)
        {
            body.resize(len);
            long long r = mg_read(conn, (void *)body.data(), len);
            body.resize((size_t)r);
        }

        // If client sends JSON { "value": "..." }
        std::string value = body;
        try
        {
            json j = json::parse(body);
            if (j.contains("value"))
                value = j["value"].get<std::string>();
        }
        catch (...)
        {
            // fallback: treat as raw string
        }

        PGconn *pg = pool_.acquire();
        const char *paramValues[2] = {key.c_str(), value.c_str()};
        PGresult *res = PQexecPrepared(pg, "kv_put", 2, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            std::string err = PQerrorMessage(pg);
            PQclear(res);
            pool_.release(pg);
            send_json(conn, 500, json{{"error", "db_error"}, {"message", err}});
            return true;
        }
        PQclear(res);
        pool_.release(pg);

        // --- CACHE ---
        // DB write was successful, now update the cache.
        cache_.put(key, value);
        // --- END CACHE ---
        send_json(conn, 200, json{{"status", "ok"}, {"key", key}, {"value", value}});
        return true;
    }

    bool doDelete(struct mg_connection *conn, const std::string &key)
    {
        PGconn *pg = pool_.acquire();
        const char *paramValues[1] = {key.c_str()};
        PGresult *res = PQexecPrepared(pg, "kv_del", 1, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            std::string err = PQerrorMessage(pg);
            PQclear(res);
            pool_.release(pg);
            send_json(conn, 500, json{{"error", "db_error"}, {"message", err}});
            return true;
        }
        PQclear(res);
        pool_.release(pg);

         // --- CACHE ---
        // DB delete was successful, now remove from cache.
        cache_.erase(key);
        // --- END CACHE ---
        send_json(conn, 200, json{{"status", "deleted"}, {"key", key}});
        return true;
    }

public:
    KVHandler(PGPool &pool, size_t cache_size) 
        : pool_(pool), cache_(cache_size)
    {
        // --- CACHE ---
        warmUpCache(CACHE_MAX_ITEMS);
        // --- END CACHE ---
    }

    bool handleGet(CivetServer *server, struct mg_connection *conn) override
    {
        const auto *ri = mg_get_request_info(conn);
        std::string uri = ri->local_uri ? ri->local_uri : "";

        if (uri.rfind("/kv/", 0) != 0)
        {
            send_json(conn, 404, json{{"error", "not_found"}});
            return true;
        }
        std::string key = url_decode(uri.substr(4));
        return doGet(conn, key);
    }

    bool handlePut(CivetServer *server, struct mg_connection *conn) override
    {
        const auto *ri = mg_get_request_info(conn);
        std::string uri = ri->local_uri ? ri->local_uri : "";

        if (uri.rfind("/kv/", 0) != 0)
        {
            send_json(conn, 404, json{{"error", "not_found"}});
            return true;
        }
        std::string key = url_decode(uri.substr(4));
        return doPut(conn, key, ri);
    }

    bool handleDelete(CivetServer *server, struct mg_connection *conn) override
    {
        const auto *ri = mg_get_request_info(conn);
        std::string uri = ri->local_uri ? ri->local_uri : "";

        if (uri.rfind("/kv/", 0) != 0)
        {
            send_json(conn, 404, json{{"error", "not_found"}});
            return true;
        }
        std::string key = url_decode(uri.substr(4));
        return doDelete(conn, key);
    }

    // bool handle(CivetServer *server, struct mg_connection *conn) override {
    //     const auto *ri = mg_get_request_info(conn);
    //     std::string method = ri->request_method ? ri->request_method : "";
    //     std::string uri = ri->local_uri ? ri->local_uri : "";

    //     if (uri.rfind("/kv/", 0) != 0) {
    //         send_json(conn, 404, json{{"error", "not_found"}});
    //         return true;
    //     }
    //     std::string key = url_decode(uri.substr(4));

    //     if (method == "GET")       return doGet(conn, key);
    //     else if (method == "PUT")  return doPut(conn, key, ri);
    //     else if (method == "DELETE") return doDelete(conn, key);
    //     else {
    //         send_json(conn, 405, json{{"error", "method_not_allowed"}});
    //         return true;
    //     }
    // }
};

// ---------- main ----------
int main(int argc, char **argv)
{
    const std::string conninfo =
        argc > 1 ? argv[1]
                 : "host=localhost port=5432 dbname=kvdb user=kvuser password=kvpass";
    try
    {
        PGPool pool(conninfo, 4);

        const char *options[] = {
            "document_root", ".", "listening_ports", "8080", nullptr};
        CivetServer server(options);
        // --- CACHE ---
        // Pass the cache size to the handler
        KVHandler handler(pool,CACHE_MAX_ITEMS);
        // --- END CACHE ---
        server.addHandler("/kv", handler);

        std::cout << "KV Server listening on http://0.0.0.0:8080\n";
        while (true)
            std::this_thread::sleep_for(std::chrono::seconds(60));
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Fatal: " << ex.what() << std::endl;
        return 1;
    }
}
