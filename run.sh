$ #!/bin/bash

SERVER="http://localhost:8080/kv"

for i in $(seq 1 10000); do
    key="key$i"
    value="value$i"
    curl -s -X PUT "$SERVER/$key" \
         -H "Content-Type: application/json" \
         -d "{\"value\":\"$value\"}" \
         >/dev/null
    if (( i % 1000 == 0 )); then
        echo "$i keys inserted..."
    fi
done

echo "Done inserting 10000 key-value pairs!"
