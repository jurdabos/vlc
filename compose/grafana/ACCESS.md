# Accessing Grafana

## URL
**http://localhost:8080/grafana/**

## Credentials

### Nginx Reverse Proxy (HTTP Basic Auth)
- **Username**: `admin`
- **Password**: `vlcpass`

### Grafana Application
- **Username**: `admin`
- **Password**: `admin` (change on first login)

## Important Notes
1. **Two layers of authentication**:
   - First: Nginx HTTP Basic Auth (admin/vlcpass)
   - Second: Grafana login (admin/admin)

2. **Browser Access**:
   - Navigate to http://localhost:8080/grafana/
   - Browser will prompt for HTTP Basic Auth credentials
   - Enter: admin / vlcpass
   - Then you'll see Grafana login page
   - Enter: admin / admin

3. **Why localhost:8080 not localhost:3000?**
   - Grafana runs on port 3000 inside Docker
   - Access is via nginx reverse proxy on port 8080
   - The proxy provides:
     * HTTP Basic Auth security layer
     * Path-based routing (/grafana/ and /kafka-ui/)
     * Future: SSL termination

## Troubleshooting

### "Connection refused" on localhost:3000
✅ **Expected** - Port 3000 is not exposed. Use localhost:8080/grafana/ instead.

### "401 Unauthorized"
- Check you're using correct HTTP Basic Auth credentials: admin/vlcpass
- Browser should save these after first successful entry

### Getting redirected incorrectly
- Make sure to include trailing slash: /grafana/ not /grafana
- Clear browser cache if needed

## Updating HTTP Basic Auth Password

```bash
cd /opt/vlc/compose
echo "admin:$(openssl passwd -apr1 YOUR_NEW_PASSWORD)" > .htpasswd
docker exec compose-reverse_proxy-1 nginx -s reload
```
