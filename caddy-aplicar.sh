#!/usr/bin/env bash
# ============================================================
# Aplica o /opt/motor/Caddyfile no Caddy QUE ESTÁ RODANDO, sem derrubar nada.
# Seguro: 1) guarda cópia do que está em uso; 2) PARA se algum domínio em uso
# ficaria de fora; 3) valida antes; 4) se o reload falhar, volta o anterior.
# Uso (no console da VPS):  bash caddy-aplicar.sh
# ============================================================
set -u
cd /opt/motor || exit 1
DC="docker compose --profile proxy"
TS=$(date +%Y%m%d-%H%M%S)

echo "==> 1) Guardando o Caddyfile em uso..."
$DC exec -T caddy cat /etc/caddy/Caddyfile > "Caddyfile.em-uso.$TS" 2>/dev/null || { echo "!! O Caddy nao respondeu. Nada foi alterado."; exit 1; }
[ -s "Caddyfile.em-uso.$TS" ] || { echo "!! A copia saiu vazia. Nada foi alterado."; exit 1; }
echo "   copia: /opt/motor/Caddyfile.em-uso.$TS"

echo "==> 2) Conferindo se algum dominio em uso ficaria de fora..."
falta=""
for h in $(grep -oE '^[^#[:space:]{}][^{]*' "Caddyfile.em-uso.$TS" | tr ',' ' '); do
  grep -qF "$h" Caddyfile || falta="$falta $h"
done
if [ -n "$falta" ]; then
  echo "!! PAREI: estes dominios estao no ar mas nao estao no Caddyfile novo:$falta"
  echo "   Nada foi alterado. Mande este print pro Claude."
  exit 1
fi
echo "   ok, nenhum dominio fica de fora."

echo "==> 3) Validando o Caddyfile novo..."
cp -f Caddyfile /tmp/Caddyfile.aplicar
$DC exec -T caddy sh -c 'cat > /tmp/Caddyfile.novo' < /tmp/Caddyfile.aplicar
if ! $DC exec -T caddy caddy validate --config /tmp/Caddyfile.novo --adapter caddyfile > /tmp/caddy-validate.log 2>&1; then
  echo "!! O Caddyfile novo tem erro. Nada foi alterado:"; tail -5 /tmp/caddy-validate.log
  exit 1
fi
echo "   valido."

echo "==> 4) Aplicando no Caddy (sem derrubar nada)..."
$DC exec -T caddy sh -c 'cat > /etc/caddy/Caddyfile' < /tmp/Caddyfile.aplicar
if $DC exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile; then
  echo "   dominios no ar agora:"
  grep -oE '^[^#[:space:]{}][^{]*' Caddyfile | sed 's/^/     /'
  echo
  echo "CADDY_OK"
else
  echo "!! O reload falhou. Voltando o anterior..."
  $DC exec -T caddy sh -c 'cat > /etc/caddy/Caddyfile' < "Caddyfile.em-uso.$TS"
  $DC exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile && echo "   anterior restaurado (tudo como estava)."
  exit 1
fi
