# ws_distro – Tick Distribution Server (Boost.Beast)

A minimal WebSocket broadcast server with an HTTP publish endpoint.

- WebSocket endpoint: `ws://<host>:<port>/ws`
- HTTP publish endpoint: `POST http://<host>:<port>/publish` (body broadcast to all WS clients)
- Health check: `GET /health`

## Build

Prereqs:
- CMake >= 3.15
- A C++17 compiler (clang >= 10 or gcc >= 9)
- Boost >= 1.75 with headers and libboost_system installed

Steps:
```
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

The binary will be at `build/ws_distro`.

## Run

Environment variables:
- `WS_DISTRO_HOST` (default: `0.0.0.0`)
- `WS_DISTRO_PORT` (default: `8080`)
- `WS_DISTRO_MAX_THREADS` (default: hardware_concurrency or 1)

Example:
```
WS_DISTRO_PORT=8080 WS_DISTRO_MAX_THREADS=4 ./ws_distro
```

## Try it

1) Start the server:
```
./ws_distro
```

2) Connect a WebSocket client (e.g., websocat or wscat):
```
# Using websocat
websocat ws://127.0.0.1:8080/ws

# Or using wscat (npm i -g wscat)
wscat -c ws://127.0.0.1:8080/ws
```

3) Publish a tick to all clients:
```
curl -X POST \
  -H 'Content-Type: application/json' \
  --data '{"symbol":"JUP/USDC","price":1.2345,"ts":1690000000}' \
  http://127.0.0.1:8080/publish
```

All connected WS clients should receive the JSON body.

## Notes
- Sharded load balancing: sessions are assigned to one of N shards (N = `WS_DISTRO_MAX_THREADS`) based on current shard load. Broadcasts are dispatched to each shard's strand, allowing parallel broadcasts across threads while serializing per-shard access.
- Writes are synchronous within each shard's strand. For very high throughput or slow clients, consider per-session async write queues.
- Add simple auth on `/publish` if exposing beyond localhost.
- You can run multiple instances behind a load balancer if you shard publishers.
