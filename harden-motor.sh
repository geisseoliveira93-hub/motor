#!/usr/bin/env bash
# ============================================================
# Blindagem do servidor (roda UMA vez, no VPS, como root).
#  - Atualizações de segurança automáticas do Ubuntu (item 11)
#  - gpg instalado (pros backups criptografados - item 10)
#  - Firewall UFW: deixa SÓ SSH(22) + HTTP(80) + HTTPS(443)
# Obs: as portas do Docker (8080/8081) já foram fechadas pra
# internet no docker-compose (ligadas só em 127.0.0.1), porque
# o Docker "fura" o UFW. Aqui o UFW protege o resto do servidor.
# ============================================================
set -e

echo "==> 1) Atualizando a lista de pacotes..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -y

echo "==> 2) Instalando atualizacoes de seguranca automaticas + gpg..."
apt-get install -y unattended-upgrades gnupg >/dev/null
# Liga o unattended-upgrades (aplica correcoes de seguranca sozinho)
echo 'APT::Periodic::Update-Package-Lists "1";' >/etc/apt/apt.conf.d/20auto-upgrades
echo 'APT::Periodic::Unattended-Upgrade "1";'   >>/etc/apt/apt.conf.d/20auto-upgrades
systemctl enable --now unattended-upgrades 2>/dev/null || true
echo "   OK (o servidor passa a se atualizar sozinho em seguranca)."

echo "==> 3) Configurando o firewall (UFW)..."
if ! command -v ufw >/dev/null 2>&1; then apt-get install -y ufw >/dev/null; fi
ufw --force reset >/dev/null 2>&1 || true
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp     comment 'SSH'
ufw allow 80/tcp     comment 'HTTP (Caddy/SSL)'
ufw allow 443/tcp    comment 'HTTPS (Caddy)'
ufw --force enable
echo "   Firewall ligado. Entradas liberadas: 22, 80, 443."

echo
echo "==================== BLINDAGEM PRONTA ===================="
echo "Status do firewall:"
ufw status verbose | sed 's/^/   /'
echo
echo "Teste: de FORA, http://179.236.233.223:8081 e :8080 NAO devem mais abrir."
echo "O painel do Evolution agora e so por https://evolution.melodiaseriffs.com.br"
echo "=========================================================="
