#!/usr/bin/env bash
# ============================================================
# Backup do banco do motor (contatos/tags/CRM) + .env.
# Roda em /opt/motor. Guarda em /opt/motor/backups e mantém
# os últimos 14 backups. Pode agendar no cron (1x por dia).
# ============================================================
set -e
cd /opt/motor
mkdir -p backups
STAMP=$(date +%Y%m%d-%H%M)

# Copia o banco de dentro do contêiner do engine
if docker compose cp engine:/data/engine.db "backups/engine-$STAMP.db" 2>/dev/null; then
  :
else
  # fallback: direto do volume
  VOL=$(docker volume inspect -f '{{ .Mountpoint }}' motor_enginedata 2>/dev/null || true)
  [ -n "$VOL" ] && cp "$VOL/engine.db" "backups/engine-$STAMP.db"
fi

# Guarda uma cópia do .env também (segredos) — permissão restrita
cp -f .env "backups/env-$STAMP.bak" 2>/dev/null || true
chmod 600 backups/env-*.bak 2>/dev/null || true

# Mantém só os 14 backups mais recentes de cada tipo
ls -1t backups/engine-*.db 2>/dev/null | tail -n +15 | xargs -r rm -f
ls -1t backups/env-*.bak  2>/dev/null | tail -n +15 | xargs -r rm -f

echo "Backup feito em /opt/motor/backups/engine-$STAMP.db"
ls -lh backups/ | tail -5

# Pra agendar todo dia às 3h da manhã, rode UMA vez:
#   (crontab -l 2>/dev/null; echo "0 3 * * * cd /opt/motor && bash backup-motor.sh >> /opt/motor/backups/backup.log 2>&1") | crontab -
