# Accessing Grafana

## URL
**http://localhost:8080/grafana/**

## Important Notes
1. **Two layers of authentication**:
   - First: Nginx HTTP Basic Auth
   - Second: Grafana login

2. **Browser Access**:
   - Navigate to http://localhost:8080/grafana/
   - Browser will prompt for HTTP Basic Auth credentials
   - Then you'll see Grafana login page
## Updating HTTP Basic Auth Password

```bash
cd /opt/vlc/compose
echo "admin:$(openssl passwd -apr1 YOUR_NEW_PASSWORD)" > .htpasswd
docker exec compose-reverse_proxy-1 nginx -s reload
```
