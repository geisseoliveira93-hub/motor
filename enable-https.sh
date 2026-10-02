#!/usr/bin/env bash
# ============================================================
# Liga o HTTPS (domínio + SSL automático via Caddy).
# PRÉ-REQUISITO: o DNS motor.melodiaseriffs.com.br já tem que
# estar apontando pro IP do servidor (179.236.233.223),
# em modo "DNS only" (nuvem CINZA no Cloudflare), senão o
# Let's Encrypt não emite o certificado.
# ============================================================
set -e
cd /opt/motor
DOM="motor.melodiaseriffs.com.br"

echo "==> Setando domínio no .env..."
grep -q '^PUBLIC_DOMAIN='   .env && sed -i "s#^PUBLIC_DOMAIN=.*#PUBLIC_DOMAIN=$DOM#"              .env || echo "PUBLIC_DOMAIN=$DOM"               >> .env
grep -q '^PUBLIC_BASE_URL=' .env && sed -i "s#^PUBLIC_BASE_URL=.*#PUBLIC_BASE_URL=https://$DOM#"  .env || echo "PUBLIC_BASE_URL=https://$DOM"    >> .env

echo "==> Liberando portas 80/443 (se o firewall ufw estiver ativo)..."
command -v ufw >/dev/null 2>&1 && { ufw allow 80/tcp || true; ufw allow 443/tcp || true; } || true

echo "==> Subindo o Caddy (proxy HTTPS)..."
docker compose --profile proxy up -d caddy

echo "==> Reiniciando o engine pra pegar a PUBLIC_BASE_URL nova..."
docker compose up -d engine

echo "==> Esperando o certificado (pode levar 1-2 min)..."
ok=""
for i in $(seq 1 30); do
  if curl -fsS "https://$DOM/health" >/dev/null 2>&1; then ok=1; break; fi
  sleep 4
done
if [ -n "$ok" ]; then
  echo "   HTTPS OK -> https://$DOM/health"
else
  echo "   Ainda não respondeu. Veja os logs: docker compose logs --tail=40 caddy"
  echo "   Causas comuns: DNS ainda propagando, ou Cloudflare em modo proxy (laranja) — deixe CINZA."
fi
