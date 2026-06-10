# Prepare EC2 on Amazon Linux 2023

Use these commands on a fresh Amazon Linux 2023 EC2 instance to install Python, Git, and `mongosh`.

## 1) Update the system

```bash
sudo dnf update -y
```

## 2) Install Python and Git

```bash
sudo dnf install -y python3 python3-pip git
python3 --version
pip3 --version
git --version
```

## 3) Install mongosh

Amazon Linux 2023 is RHEL 9 compatible, so use the MongoDB RHEL 9 repository.

```bash
sudo tee /etc/yum.repos.d/mongodb-org-8.0.repo > /dev/null <<'EOF'
[mongodb-org-8.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/9/mongodb-org/8.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://pgp.mongodb.com/server-8.0.asc
EOF

sudo dnf install -y mongodb-mongosh
mongosh --version
```

## 4) Optional: create a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
```

## 5) Optional: clone this repository

```bash
git clone https://github.com/kSinglaVikas/stockticker.git 
cd stockticker
mkdir logs
pip install -r requirements.txt
```