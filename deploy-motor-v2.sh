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
