#!/bin/bash
# Premium EP1 Linux 환경: ODBC Driver 18 설치 (컨테이너 시작 시 실행)
# Flex Consumption은 기본 포함이나 Premium은 미포함

if ! odbcinst -q -d 2>/dev/null | grep -q "ODBC Driver 18"; then
    echo "Installing ODBC Driver 18 for SQL Server..."
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list
    apt-get update
    ACCEPT_EULA=Y apt-get install -y msodbcsql18
    echo "ODBC Driver 18 installed."
else
    echo "ODBC Driver 18 already installed."
fi
