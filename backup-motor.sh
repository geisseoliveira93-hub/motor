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

# ---- SEGURANCA (item 10): se BACKUP_PASSPHRASE estiver no .env, CRIPTOGRAFA os backups ----
# Põe no .env uma linha:  BACKUP_PASSPHRASE=uma-senha-forte-sua
# Pra abrir um backup depois:
#   gpg --decrypt --batch --passphrase 'SUA_SENHA' backups/engine-XXXX.db.gpg > engine.db
PASS=$(grep -oP '(?<=^BACKUP_PASSPHRASE=).*' .env 2>/dev/null || true)
if [ -n "$PASS" ] && command -v gpg >/dev/null 2>&1; then
  for f in "backups/engine-$STAMP.db" "backups/env-$STAMP.bak"; do
    [ -f "$f" ] || continue
    if gpg --batch --yes --passphrase "$PASS" -c --cipher-algo AES256 -o "$f.gpg" "$f" 2>/dev/null; then
      rm -f "$f"; chmod 600 "$f.gpg"
    fi
  done
  echo "Backups CRIPTOGRAFADOS (.gpg)."
else
  echo "AVISO: backups SEM criptografia. Pra criptografar: defina BACKUP_PASSPHRASE no .env e tenha o gpg instalado."
fi

# Mantém só os 14 backups mais recentes de cada tipo (inclui os .gpg)
ls -1t backups/engine-*.db 2>/dev/null | tail -n +15 | xargs -r rm -f
ls -1t backups/env-*.bak  2>/dev/null | tail -n +15 | xargs -r rm -f
ls -1t backups/engine-*.db.gpg 2>/dev/null | tail -n +15 | xargs -r rm -f
ls -1t backups/env-*.bak.gpg  2>/dev/null | tail -n +15 | xargs -r rm -f

echo "Backup feito em /opt/motor/backups/engine-$STAMP.db"
ls -lh backups/ | tail -5

# Pra agendar todo dia às 3h da manhã, rode UMA vez:
#   (crontab -l 2>/dev/null; echo "0 3 * * * cd /opt/motor && bash backup-motor.sh >> /opt/motor/backups/backup.log 2>&1") | crontab -
