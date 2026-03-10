# UFW + Docker Setup

Docker inserts its own iptables rules that are evaluated **before** UFW, so `ufw allow/deny` has no effect on published container ports by default.

To make Docker ports controllable via UFW, use one of these approaches.

---

## Option 1: ufw-docker (recommended)

[ufw-docker](https://github.com/chaifeng/ufw-docker) integrates UFW with Docker so your normal `ufw allow` commands apply to container ports.

```bash
# Install (Debian/Ubuntu)
sudo wget -O /usr/local/bin/ufw-docker https://github.com/chaifeng/ufw-docker/raw/master/ufw-docker
sudo chmod +x /usr/local/bin/ufw-docker

# Configure UFW
sudo ufw-docker install

# Reload UFW
sudo systemctl reload ufw
```

Then use UFW as usual:

```bash
# Allow PostgreSQL from anywhere
sudo ufw allow 5432/tcp

# Allow RustFS S3 only from your LAN
sudo ufw allow from 192.168.1.0/24 to any port 9000 proto tcp

# Allow RustFS Console only from localhost (if accessing via SSH tunnel)
sudo ufw allow from 127.0.0.1 to any port 9001 proto tcp

sudo ufw reload
```

---

## Option 2: Manual DOCKER-USER rules

Add rules to the `DOCKER-USER` chain, which Docker processes before its own rules.

Edit `/etc/ufw/after.rules` and append:

```bash
# BEGIN UFW + Docker
*filter
:DOCKER-USER - [0:0]

# Allow established connections (required for responses)
-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT

# Allow localhost
-A DOCKER-USER -i lo -j ACCEPT

# Allow specific ports (adjust as needed)
-A DOCKER-USER -p tcp -m tcp --dport 5432 -j ACCEPT
-A DOCKER-USER -p tcp -m tcp --dport 6379 -j ACCEPT
-A DOCKER-USER -p tcp -m tcp --dport 9000 -j ACCEPT
-A DOCKER-USER -p tcp -m tcp --dport 9001 -j ACCEPT

# Drop everything else destined for Docker
-A DOCKER-USER -j DROP
COMMIT
# END UFW + Docker
```

Replace the port rules with your own. To restrict by source IP:

```bash
# Only allow PostgreSQL from LAN
-A DOCKER-USER -p tcp -m tcp -s 192.168.1.0/24 --dport 5432 -j ACCEPT
```

Then reload:

```bash
sudo ufw reload
```

---

## Option 3: Bind to localhost only

If you never need external access, bind ports to `127.0.0.1` in `docker-compose.yml`:

```yaml
ports:
  - "127.0.0.1:5432:5432"
```

Nothing listens on external interfaces, so UFW is irrelevant. PeerTube on the host can still reach services via localhost.
