# Install (Debian/Ubuntu)
sudo wget -O /usr/local/bin/ufw-docker https://github.com/chaifeng/ufw-docker/raw/master/ufw-docker
sudo chmod +x /usr/local/bin/ufw-docker

# Configure UFW
sudo ufw-docker install

# Reload UFW
sudo systemctl reload ufw