#!/usr/bin/env bash
# ============================================================
# Deploy do MOTOR v2 no VPS. Roda em /opt/motor DEPOIS que os
# arquivos novos (engine/src/index.ts, engine/config.json,
# docker-compose.yml) já estiverem atualizados aqui.
# Seguro: se o build falhar, o container velho continua rodando.
# ============================================================
set -e
cd /opt/motor

echo "==> 1) Garantindo tokens dos webhooks de pagamento no .env..."
gen(){ tr -dc 'a-zA-Z0-9' </dev/urandom | head -c 32; }
grep -q '^GURU_TOKEN='     .env || echo "GURU_TOKEN=$(gen)"     >> .env
grep -q '^LASTLINK_TOKEN=' .env || echo "LASTLINK_TOKEN=$(gen)" >> .env

echo "==> 2) Backup do index.ts atual..."
cp -f engine/src/index.ts "engine/src/index.ts.bak-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true

echo "==> 3) Rebuildando SÓ o engine (evolution/postgres/redis ficam de pé)..."
docker compose up -d --build engine

echo "==> 4) Esperando o engine responder..."
KEY=$(grep -oP '(?<=ENGINE_API_KEY=).*' .env)
ok=""
for i in $(seq 1 20); do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
if [ -z "$ok" ]; then echo "!! engine não respondeu /health — veja: docker compose logs --tail=50 engine"; exit 1; fi
echo "   engine OK."

echo "==> 5) Instalando a config (produto->projeto + mensagens)..."
docker compose cp ./engine/config.json engine:/data/config.json
curl -s -X POST http://localhost:8080/config/reload -H "x-api-key: $KEY" >/dev/null && echo "   config carregada."

echo "==> 6) Registrando o número pessoal (Ezequias-pessoal) no projeto TECLADO..."
curl -s -X POST http://localhost:8080/numbers/register \
  -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  -d '{"projeto":"teclado","instancia":"Ezequias-pessoal","telefone":"5516981518794"}'
echo

echo "==> 7) Sincronizando status dos números com o Evolution..."
curl -s -X POST http://localhost:8080/numbers/sync -H "x-api-key: $KEY" >/dev/null && echo "   ok."

echo "==> 7b) Protegendo o Caddyfile (o deploy nunca apaga um dominio que esta no ar)..."
# [+caddy-protecao 27/10] O pacote do GitHub traz um Caddyfile que pode nao ter os dominios criados
# direto no servidor (teclado/baixo/violao/harpa/areadoaluno...). Aqui eles sao mantidos: tudo que
# esta no Caddy em uso e falta no Caddyfile novo e ACRESCENTADO no fim (e o bloco global, se faltar).
if docker compose --profile proxy ps caddy 2>/dev/null | grep -qiE "up|running"; then
  if docker compose --profile proxy exec -T caddy cat /etc/caddy/Caddyfile > /tmp/Caddyfile.em-uso 2>/dev/null && [ -s /tmp/Caddyfile.em-uso ]; then
    cp -f Caddyfile "Caddyfile.pacote-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true
    awk 'NR==FNR{ if ($0 ~ /^[^#[:space:]{][^{]*\{[[:space:]]*$/) { h=$0; sub(/[[:space:]]*\{[[:space:]]*$/,"",h); tem[h]=1 } next }
         { if (!dentro && $0 ~ /^[^#[:space:]{][^{]*\{[[:space:]]*$/) { h=$0; sub(/[[:space:]]*\{[[:space:]]*$/,"",h); faltando=!(h in tem); if (faltando) print ""; dentro=1; prof=0 }
           if (dentro && faltando) print
           if (dentro) { n=gsub(/\{/,"{"); m=gsub(/\}/,"}"); prof+=n-m; if (prof<=0) { dentro=0; faltando=0 } } }' Caddyfile /tmp/Caddyfile.em-uso > /tmp/Caddyfile.faltando || true
    awk 'BEGIN{st=0} { if (st==0) { if ($0 ~ /^[[:space:]]*$/ || $0 ~ /^[[:space:]]*#/) next; if ($0 ~ /^\{[[:space:]]*$/) { st=1; prof=0 } else exit } if (st==1) { print; n=gsub(/\{/,"{"); m=gsub(/\}/,"}"); prof+=n-m; if (prof<=0) exit } }' /tmp/Caddyfile.em-uso > /tmp/Caddyfile.global || true
    temglobal=$(awk '{ if ($0 ~ /^[[:space:]]*$/ || $0 ~ /^[[:space:]]*#/) next; if ($0 ~ /^\{[[:space:]]*$/) print "sim"; exit }' Caddyfile)
    if [ -s /tmp/Caddyfile.global ] && [ "$temglobal" != "sim" ]; then
      { cat /tmp/Caddyfile.global; echo; cat Caddyfile; } > /tmp/Caddyfile.comglobal && cp -f /tmp/Caddyfile.comglobal Caddyfile
      echo "   bloco global do Caddy mantido."
    fi
    if [ -s /tmp/Caddyfile.faltando ]; then
      cat /tmp/Caddyfile.faltando >> Caddyfile
      echo "   dominios que estavam no ar foram mantidos no Caddyfile:"
      grep -oE '^[^#[:space:]{}][^{]*' /tmp/Caddyfile.faltando | sed 's/^/     /' || true
    else
      echo "   Caddyfile ok (nenhum dominio no ar ficou de fora)."
    fi
  fi
fi

echo "==> 8) Recarregando o Caddy (pega domínios novos do Caddyfile, ex: evolution.*)..."
if docker compose --profile proxy ps caddy 2>/dev/null | grep -qiE "up|running"; then
  docker compose --profile proxy up -d caddy >/dev/null 2>&1 || true
  docker compose --profile proxy exec -T caddy caddy reload --config /etc/caddy/Caddyfile 2>/dev/null \
    && echo "   Caddy recarregado (subdomínios novos serão certificados automaticamente)." \
    || echo "   (Caddy nao recarregou via exec; se precisar: docker compose --profile proxy restart caddy)"
else
  echo "   Caddy nao esta ligado — rode enable-https.sh pra ligar o HTTPS."
fi

echo "==> 9) Agendando backup diario do banco (3h da manha), se ainda nao houver..."
mkdir -p /opt/motor/backups
if crontab -l 2>/dev/null | grep -q "backup-motor.sh"; then
  echo "   backup diario ja estava agendado."
else
  ( crontab -l 2>/dev/null; echo "0 3 * * * cd /opt/motor && bash backup-motor.sh >> /opt/motor/backups/backup.log 2>&1" ) | crontab -
  echo "   backup diario agendado."
fi

echo
echo "==================== PRONTO ===================="
echo "Números cadastrados no motor:"
curl -s http://localhost:8080/numbers -H "x-api-key: $KEY"; echo
echo
BASE=$(grep -oP '(?<=PUBLIC_BASE_URL=).*' .env)
[ -z "$BASE" ] && BASE="http://$(curl -s ifconfig.me 2>/dev/null || echo SEU_IP):8080"
echo "URLs de webhook pra colar nas plataformas de pagamento:"
echo "  Guru     : $BASE/webhook/payment/guru?token=$(grep -oP '(?<=GURU_TOKEN=).*' .env)"
echo "  LastLink : $BASE/webhook/payment/lastlink?token=$(grep -oP '(?<=LASTLINK_TOKEN=).*' .env)"
echo "(Se ainda não tiver HTTPS/domínio, configure o Caddy antes de usar essas URLs em produção.)"
echo "================================================"
