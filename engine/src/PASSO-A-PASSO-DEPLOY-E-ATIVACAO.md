# Motor #2 — Passo a passo pra subir e ativar (quando você quiser)

Ezequias, construí tudo **desligado e sem risco**. O código novo só age quando VOCÊ liga uma chave.
Enquanto não ligar, o motor se comporta exatamente como antes e a Sellflux segue vendendo normal.

O arquivo pronto está em: `F:\Claude\Editor-Video\_DEPLOY_MOTOR\index.ts` (8436 linhas, validado).

---

## 1) Subir o código (deploy) — precisa ser você

Não consigo fazer da nuvem (o GitHub não está logado aqui e o passo final da VPS é bloqueado por segurança).

1. Abrir o GitHub: repositório **geisseoliveira93-hub/motor**, arquivo **engine/src/index.ts**.
2. Substituir o conteúdo pelo do arquivo `F:\Claude\Editor-Video\_DEPLOY_MOTOR\index.ts` e dar **Commit**.
3. Na VPS (hPanel → console), rodar: `bash deploy-motor-v2.sh`
4. Conferir que subiu: abrir `https://motor.melodiaseriffs.com.br/health` (tem que responder ok).

> Se der qualquer problema, o backup de antes está em
> `F:\Claude\Editor-Video\_BACKUPS_CODIGO\2026-10-17-motor2-trilha-tags\index-PRE.ts` — é só subir ele de volta.

---

## 2) O que já vem pronto (não precisa configurar nada)

- As 4 tags dos low tickets (nomes idênticos aos da Sellflux) já estão mapeadas no motor.
- O tratamento de **order bump e upsell** (entregar + taguear + disparar fluxo de CADA item) já está pronto.
- A trava do **número compartilhado 8151**: o agente só responde a quem tem tag (comprador low ticket
  ou grupo-vip). Quem chega sem tag **fica na fila pro atendente humano**.

---

## 3) Ligar quando quiser testar lado a lado com a Sellflux

Tudo começa **desligado**. Pra ligar, é só uma chamada (dá pra fazer pelo app ou por mim):

- Ligar a trilha (tags/bump/upsell): `POST /motor2/flag` → `{ "flag": "motor2_ativo", "on": true }`
- Ver o estado atual: `GET /motor2/config`
- Trava do agente por tag (já vem LIGADA): `POST /motor2/flag` → `{ "flag": "agente_tag_gate", "on": true/false }`

E pra **ligar um agente de verdade no WhatsApp** (continua desligado por segurança), é o
`agentesHabilitados` do projeto — a gente liga junto quando você quiser, com calma.

---

## 4) O que ainda falta pra rodar 100% (depende de você me passar)

1. **Apontar os webhooks do Guru e do LastLink pro motor** — hoje o motor ainda não recebe as
   compras (por isso /transacoes e /produtos estão vazios). Sem isso, nada chega pra taguear/entregar.
   URL do motor: `https://motor.melodiaseriffs.com.br/webhook/payment/guru?token=...` e `.../lastlink?token=...`
2. **Conteúdo dos fluxos por tag** — a sequência de mensagens, qual grupo, qual webinário. Isso NÃO
   veio no export da Sellflux (só vieram as páginas, não as automações). Você me passa o conteúdo
   (ou a gente copia da Sellflux) e eu monto cada fluxo.
3. **Tags dos outros produtos** (lançamento / perpétuo / captação) e os **order bumps/upsells exatos** —
   abrir as ofertas no Guru/LastLink pra eu mapear. Hoje só os 4 low tickets estão no mapa.

---

## Resumo de segurança

- Nada apagado — só adicionei. O caminho antigo de compra/entrega está intacto.
- Tudo novo nasce DESLIGADO (`motor2_ativo=OFF`).
- A trava do 8151 nasce LIGADA (agente não fala com quem não tem tag).
- Backup completo (antes e depois) nas duas pastas de backup.
