#!/bin/bash
# Premium EP1 Linux (Debian 11 Bullseye): ODBC Driver 18 설치
# 컨테이너 재시작 시마다 실행됨

if ! odbcinst -q -d 2>/dev/null | grep -q "ODBC Driver 18"; then
    echo "[startup.sh] Installing ODBC Driver 18 for SQL Server..."
    apt-get update -qq
    ACCEPT_EULA=Y apt-get install -y -qq gnupg2 curl apt-transport-https
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
    echo "deb https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list
    apt-get update -qq
    ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18
    echo "[startup.sh] ODBC Driver 18 installed: $(odbcinst -q -d)"
else
    echo "[startup.sh] ODBC Driver 18 already installed."
fi
