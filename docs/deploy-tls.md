# TLS termination in front of the OMS

The OMS serves plain HTTP/WS on `OMS_HTTP_PORT` (default 8080) and plain gRPC
on `OMS_GRPC_PORT` (default 9090). It is designed to sit behind a
TLS-terminating reverse proxy on the same box (bare-metal single-box
deployment); do not expose the plain ports publicly.

Checklist for a public deployment:

1. Bind the OMS ports to loopback (firewall or bind address) and publish only
   the proxy's 443.
2. Run the OMS with a real auth mode (`OMS_AUTH_MODE=api-key` or `jwt` — never
   `dev`), with secrets injected via `OMS_JWT_SECRET_FILE` /
   `OMS_API_KEYS_FILE` / `OMS_POSTGRES_PASSWORD_FILE`.
3. Set `OMS_CORS_ORIGINS` to the exact UI origin(s), e.g.
   `https://trade.example.com`. The default emits no CORS headers at all.
4. Keep `/api/v1/health` reachable from localhost only (it is unauthenticated
   by design — probe it from the box, don't route it through the proxy).
5. The audit log (`OMS_AUDIT_LOG`, default `oms-audit.log` in the working
   directory) should live on persistent disk and be rotated/shipped like any
   security log.

## Caddy (simplest)

```caddy
oms.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

Caddy provisions and renews certificates automatically and proxies WebSocket
upgrades (`/ws/v1`) out of the box. gRPC needs an h2-capable block:

```caddy
oms-grpc.example.com {
    reverse_proxy h2c://127.0.0.1:9090
}
```

## nginx

```nginx
server {
    listen 443 ssl http2;
    server_name oms.example.com;

    ssl_certificate     /etc/letsencrypt/live/oms.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/oms.example.com/privkey.pem;

    # REST
    location /api/v1/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        client_max_body_size 1m;   # matches the OMS aggregator limit
    }

    # WebSocket (order/execution/balance push)
    location /ws/v1 {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300s;
    }

    # Never proxy /api/v1/health publicly — it is unauthenticated by design.
    location = /api/v1/health { return 404; }
}

# gRPC (separate server block, needs grpc_pass)
server {
    listen 9443 ssl http2;
    server_name oms.example.com;

    ssl_certificate     /etc/letsencrypt/live/oms.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/oms.example.com/privkey.pem;

    location / {
        grpc_pass grpc://127.0.0.1:9090;
    }
}
```

## Admin gateway

The admin gateway (port 8082) binds loopback by default and refuses a
non-loopback bind without `ADMIN_AUTH_TOKEN`. Prefer NOT exposing it at all —
reach it over SSH/VPN. If it must be public, front it the same way and keep
the token required.
