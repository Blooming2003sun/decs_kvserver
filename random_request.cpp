#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <curl/curl.h>

std::string random_string(int length) {
    std::string s;
    std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < length; ++i) {
        s += chars[rand() % chars.size()];
    }
    return s;
}

size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

int main() {
    srand(time(0));
    const std::string BASE_URL = "http://localhost:8080/kv";

    CURL* curl;
    CURLcode res;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    curl = curl_easy_init();
    if(!curl) return 1;

    std::vector<std::string> methods = {"GET"};//, "PUT", "POST", "DELETE"};

    while (true) {
        std::string method = methods[rand() % methods.size()];
        std::string key = "key" + std::to_string(rand());
        std::string url = BASE_URL + "/" + key;
        std::string data = "{\"value\":\"" + random_string(10) + "\"}";
        std::string response;

        curl_easy_reset(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        if (method == "PUT" || method == "POST") {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
            struct curl_slist* headers = nullptr;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
        } else if (method == "DELETE") {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        } else {
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        }

        res = curl_easy_perform(curl);
        if(res != CURLE_OK)
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
        else
            std::cout << method << " " << url << " → " << response << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(5));// 500  || 50 
    }

    curl_easy_cleanup(curl);
    curl_global_cleanup();
    return 0;
}
