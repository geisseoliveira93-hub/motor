/**
 * Motor de WhatsApp do Melodias e Riffs — "engine" (a cola).
 * -----------------------------------------------------------------
 * O que este serviço faz:
 *  - Fala com a Evolution API (conecta números por QR, envia texto/mídia, grupos).
 *  - Separa tudo por PROJETO: baixo / violao / teclado (cada um com seus números).
 *  - Guarda CONTATOS, TAGS, MENSAGENS e NEGÓCIOS (CRM) num SQLite.
 *  - Recebe o que chega no WhatsApp (webhook da Evolution) e salva/auto-taggeia.
 *  - Recebe WEBHOOK DE PAGAMENTO (Guru / LastLink) e dispara ENTREGA e RECUPERAÇÃO.
 *  - Faz DISPARO EM MASSA por TAG (contatos e grupos) com ritmo humano (anti-ban).
 *
 * Tudo protegido por uma chave (ENGINE_API_KEY) no header "x-api-key".
 * Webhooks de pagamento são validados por token na URL (?token=...).
 *
 * v2 (02/10): roteamento de projeto POR CADASTRO (tabela numeros) em vez de pelo
 * nome da instância; endpoint pra registrar instância já existente num projeto;
 * webhooks de Guru E LastLink com mapa produto->projeto e mensagens configuráveis.
 */
import express from 'express';
import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';

const PORT = Number(process.env.PORT || 8080);
const EVOLUTION_URL = process.env.EVOLUTION_URL || 'http://evolution:8080';
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY || '';
const ENGINE_API_KEY = process.env.ENGINE_API_KEY || '';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || '';
// Tokens que validam os webhooks de pagamento (vêm na URL: ?token=...).
const GURU_TOKEN = process.env.GURU_TOKEN || '';
const LASTLINK_TOKEN = process.env.LASTLINK_TOKEN || '';

const PROJETOS = ['baixo', 'violao', 'teclado'] as const;
type Projeto = (typeof PROJETOS)[number];
const ehProjeto = (x: any): x is Projeto => PROJETOS.includes(x);

// ----------------------- Banco (SQLite) -----------------------
const db = new Database('/data/engine.db');
db.pragma('journal_mode = WAL');
db.exec(`
CREATE TABLE IF NOT EXISTS numeros (
  id TEXT PRIMARY KEY, projeto TEXT, telefone TEXT, instancia TEXT UNIQUE,
  tipo TEXT DEFAULT 'nao-oficial', status TEXT DEFAULT 'desconectado',
  teto_dia INTEGER DEFAULT 1000, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS contatos (
  id TEXT PRIMARY KEY, projeto TEXT, telefone TEXT, nome TEXT, email TEXT,
  origem TEXT, criado_em TEXT, atualizado_em TEXT,
  UNIQUE(projeto, telefone)
);
CREATE TABLE IF NOT EXISTS tags (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, UNIQUE(projeto, nome)
);
CREATE TABLE IF NOT EXISTS contato_tags (
  contato_id TEXT, tag_id TEXT, criado_em TEXT, PRIMARY KEY (contato_id, tag_id)
);
CREATE TABLE IF NOT EXISTS mensagens (
  id TEXT PRIMARY KEY, projeto TEXT, contato_id TEXT, direcao TEXT,
  texto TEXT, tipo TEXT DEFAULT 'texto', criado_em TEXT
);
CREATE TABLE IF NOT EXISTS negocios (
  id TEXT PRIMARY KEY, projeto TEXT, contato_id TEXT, titulo TEXT,
  valor REAL DEFAULT 0, etapa TEXT DEFAULT 'novo', criado_em TEXT, atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS fila_envio (
  id TEXT PRIMARY KEY, projeto TEXT, para TEXT, is_grupo INTEGER DEFAULT 0,
  texto TEXT, status TEXT DEFAULT 'pendente', tentativas INTEGER DEFAULT 0,
  agendado_para TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS pagamentos_processados (
  provider TEXT, pedido TEXT, criado_em TEXT, PRIMARY KEY (provider, pedido)
);
`);

const agora = () => new Date().toISOString();

// ----------------------- Config editável (sem redeploy) -----------------------
// Carrega /data/config.json se existir; senão usa o default abaixo.
// O Ezequias/eu editamos esse arquivo pra mapear produto->projeto e as mensagens,
// sem precisar rebuildar o container.
type Config = {
  // Mapa de produto (id OU nome, em minúsculas) -> projeto.
  produtoProjeto: Record<string, Projeto>;
  // Palavra que aparece no produto -> projeto (fallback por "contém").
  palavraChaveProjeto: { contem: string; projeto: Projeto }[];
  // Projeto usado quando não dá pra identificar.
  projetoPadrao: Projeto;
  // Mensagens por projeto. {nome} é substituído pelo nome do comprador.
  entrega: Record<Projeto, string>;
  recuperacao: Record<Projeto, string>;
};
const CONFIG_DEFAULT: Config = {
  produtoProjeto: {},
  palavraChaveProjeto: [
    { contem: 'baixo', projeto: 'baixo' },
    { contem: 'violao', projeto: 'violao' },
    { contem: 'violão', projeto: 'violao' },
    { contem: 'teclado', projeto: 'teclado' },
  ],
  projetoPadrao: 'teclado',
  entrega: {
    baixo: 'Olá {nome}! 🎸 Sua compra foi aprovada. Seu acesso está aqui: (configurar link)',
    violao: 'Olá {nome}! 🎸 Sua compra foi aprovada. Seu acesso está aqui: (configurar link)',
    teclado: 'Olá {nome}! 🎹 Sua compra foi aprovada. Seu acesso está aqui: (configurar link)',
  },
  recuperacao: {
    baixo: 'Oi {nome}! Vi que sua compra do curso de baixo não foi concluída. Posso te ajudar a finalizar? 👇',
    violao: 'Oi {nome}! Vi que sua compra do curso de violão não foi concluída. Posso te ajudar a finalizar? 👇',
    teclado: 'Oi {nome}! Vi que sua compra do curso de teclado não foi concluída. Posso te ajudar a finalizar? 👇',
  },
};
function carregarConfig(): Config {
  try {
    const raw = fs.readFileSync('/data/config.json', 'utf8');
    const c = JSON.parse(raw);
    return { ...CONFIG_DEFAULT, ...c };
  } catch {
    return CONFIG_DEFAULT;
  }
}
let CONFIG = carregarConfig();

/** Descobre o projeto de um pagamento pelo produto (id, nome). */
function projetoDoProduto(idOuNome: string | undefined, nome?: string): Projeto {
  const alvo = `${idOuNome || ''} ${nome || ''}`.toLowerCase().trim();
  // 1) match exato por id ou nome cadastrado
  for (const chave of Object.keys(CONFIG.produtoProjeto)) {
    if (alvo.includes(chave.toLowerCase())) return CONFIG.produtoProjeto[chave];
  }
  // 2) por palavra-chave (baixo/violao/teclado no nome)
  for (const { contem, projeto } of CONFIG.palavraChaveProjeto) {
    if (alvo.includes(contem.toLowerCase())) return projeto;
  }
  // 3) padrão
  return CONFIG.projetoPadrao;
}

// ----------------------- Evolution API client -----------------------
async function evo(path: string, method = 'GET', body?: unknown) {
  const res = await fetch(`${EVOLUTION_URL}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', apikey: EVOLUTION_API_KEY },
    body: body ? JSON.stringify(body) : undefined,
  });
  const txt = await res.text();
  let data: any = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = txt; }
  if (!res.ok) throw new Error(`Evolution ${res.status}: ${txt}`);
  return data;
}

/** Cria/garante uma instância (número) e devolve o QR pra escanear. */
async function conectarNumero(projeto: Projeto, telefone: string) {
  const instancia = `${projeto}-${telefone}`.replace(/[^a-z0-9-]/gi, '');
  try {
    await evo('/instance/create', 'POST', {
      instanceName: instancia, integration: 'WHATSAPP-BAILEYS', qrcode: true,
    });
  } catch { /* já existe: segue pra pegar o QR */ }
  const qr = await evo(`/instance/connect/${instancia}`, 'GET');
  const id = randomUUID();
  db.prepare(
    `INSERT INTO numeros(id,projeto,telefone,instancia,status,criado_em)
     VALUES(?,?,?,?,?,?) ON CONFLICT(instancia) DO UPDATE SET status='aguardando_qr'`
  ).run(id, projeto, telefone, instancia, 'aguardando_qr', agora());
  return { instancia, qr }; // qr.base64 = imagem do QR pro app mostrar
}

async function statusNumero(instancia: string) {
  return evo(`/instance/connectionState/${instancia}`, 'GET');
}

/** Normaliza o estado que a Evolution devolve pro nosso vocabulário. */
function normalizaEstado(estado: any): string {
  const s = String(estado || '').toLowerCase();
  if (s === 'open') return 'conectado';
  if (s === 'close' || s === 'closed') return 'desconectado';
  if (s === 'connecting') return 'conectando';
  return s || 'desconectado';
}

/**
 * Registra uma instância JÁ existente no Evolution dentro de um projeto do engine.
 * Usado quando a instância foi criada direto no painel do Evolution (sem passar
 * pelo /numbers/connect). Não precisa re-escanear QR.
 */
async function registrarInstanciaExistente(projeto: Projeto, instancia: string, telefone?: string) {
  let status = 'desconectado';
  let tel = telefone || '';
  try {
    const st = await statusNumero(instancia);
    status = normalizaEstado(st?.instance?.state ?? st?.state);
  } catch { /* se não conseguir ler o estado, fica desconectado até o webhook atualizar */ }
  // tenta descobrir o número conectado (varia conforme versão da Evolution)
  if (!tel) {
    try {
      const todas = await evo('/instance/fetchInstances', 'GET');
      const arr = Array.isArray(todas) ? todas : (todas?.instances || []);
      const achou = arr.find((i: any) =>
        (i?.name || i?.instance?.instanceName || i?.instanceName) === instancia);
      const owner = achou?.ownerJid || achou?.instance?.owner || achou?.owner || '';
      tel = String(owner).replace(/@.*/, '');
    } catch { /* ok, segue sem telefone */ }
  }
  const existe = db.prepare(`SELECT id FROM numeros WHERE instancia=?`).get(instancia) as any;
  if (existe) {
    db.prepare(`UPDATE numeros SET projeto=?, status=?, telefone=COALESCE(NULLIF(?,''),telefone) WHERE instancia=?`)
      .run(projeto, status, tel, instancia);
    return { instancia, projeto, status, telefone: tel, novo: false };
  }
  db.prepare(`INSERT INTO numeros(id,projeto,telefone,instancia,status,criado_em) VALUES(?,?,?,?,?,?)`)
    .run(randomUUID(), projeto, tel, instancia, status, agora());
  return { instancia, projeto, status, telefone: tel, novo: true };
}

/** Qual projeto é dono desta instância? Primeiro o cadastro; depois o nome; depois o padrão. */
function projetoDaInstancia(instancia: string): Projeto {
  const row = db.prepare(`SELECT projeto FROM numeros WHERE instancia=?`).get(instancia) as any;
  if (row && ehProjeto(row.projeto)) return row.projeto;
  const prefixo = String(instancia || '').split('-')[0];
  if (ehProjeto(prefixo)) return prefixo;
  return CONFIG.projetoPadrao;
}

/** Envia texto por um número do projeto (pega o 1º conectado como padrão). */
async function enviarTexto(projeto: Projeto, para: string, texto: string) {
  const num = db.prepare(
    `SELECT * FROM numeros WHERE projeto=? AND status='conectado' ORDER BY criado_em LIMIT 1`
  ).get(projeto) as any;
  if (!num) throw new Error(`Nenhum número conectado no projeto ${projeto}`);
  return evo(`/message/sendText/${num.instancia}`, 'POST', {
    number: para, text: texto,
  });
}

// ----------------------- Helpers de CRM/tags -----------------------
function upsertContato(projeto: string, telefone: string, nome?: string, email?: string, origem?: string) {
  const existe = db.prepare(`SELECT id FROM contatos WHERE projeto=? AND telefone=?`).get(projeto, telefone) as any;
  if (existe) {
    db.prepare(`UPDATE contatos SET nome=COALESCE(?,nome), email=COALESCE(?,email), atualizado_em=? WHERE id=?`)
      .run(nome ?? null, email ?? null, agora(), existe.id);
    return existe.id as string;
  }
  const id = randomUUID();
  db.prepare(`INSERT INTO contatos(id,projeto,telefone,nome,email,origem,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`)
    .run(id, projeto, telefone, nome ?? null, email ?? null, origem ?? 'whatsapp', agora(), agora());
  return id;
}
function aplicarTag(projeto: string, contatoId: string, tagNome: string) {
  let tag = db.prepare(`SELECT id FROM tags WHERE projeto=? AND nome=?`).get(projeto, tagNome) as any;
  if (!tag) { const tid = randomUUID(); db.prepare(`INSERT INTO tags(id,projeto,nome) VALUES(?,?,?)`).run(tid, projeto, tagNome); tag = { id: tid }; }
  db.prepare(`INSERT OR IGNORE INTO contato_tags(contato_id,tag_id,criado_em) VALUES(?,?,?)`).run(contatoId, tag.id, agora());
}
const preenche = (tpl: string, nome?: string) => String(tpl || '').replace(/\{nome\}/g, (nome || '').trim());

// ----------------------- API HTTP -----------------------
const app = express();
app.use(express.json({ limit: '2mb' }));

// Webhooks de pagamento NÃO exigem nossa chave (vêm de fora), mas são validados por token na URL.
// Todo o resto exige x-api-key.
app.use((req, res, next) => {
  if (req.path.startsWith('/webhook/')) return next();
  if (req.path === '/health') return next();
  if ((req.header('x-api-key') || '') !== ENGINE_API_KEY) return res.status(401).json({ erro: 'sem_autorizacao' });
  next();
});

app.get('/health', (_req, res) => res.json({ ok: true, projetos: PROJETOS, hora: agora() }));

// ---- Números (conectar por QR / status / registrar existente / sincronizar) ----
app.post('/numbers/connect', async (req, res) => {
  try {
    const { projeto, telefone } = req.body as { projeto: Projeto; telefone: string };
    if (!ehProjeto(projeto)) return res.status(400).json({ erro: 'projeto_invalido' });
    res.json(await conectarNumero(projeto, telefone));
  } catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

// Registrar uma instância que já existe no Evolution dentro de um projeto (sem re-escanear).
app.post('/numbers/register', async (req, res) => {
  try {
    const { projeto, instancia, telefone } = req.body as { projeto: Projeto; instancia: string; telefone?: string };
    if (!ehProjeto(projeto)) return res.status(400).json({ erro: 'projeto_invalido' });
    if (!instancia) return res.status(400).json({ erro: 'instancia_obrigatoria' });
    res.json(await registrarInstanciaExistente(projeto, instancia, telefone));
  } catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

// Sincroniza TODAS as instâncias do Evolution que ainda não estão no cadastro,
// e atualiza o status das que já estão. (Não força projeto; usa o nome como dica.)
app.post('/numbers/sync', async (_req, res) => {
  try {
    const todas = await evo('/instance/fetchInstances', 'GET');
    const arr = Array.isArray(todas) ? todas : (todas?.instances || []);
    const resumo: any[] = [];
    for (const i of arr) {
      const nome = i?.name || i?.instance?.instanceName || i?.instanceName;
      if (!nome) continue;
      const estado = i?.connectionStatus || i?.instance?.state || i?.state;
      const status = normalizaEstado(estado);
      const owner = i?.ownerJid || i?.instance?.owner || i?.owner || '';
      const tel = String(owner).replace(/@.*/, '');
      const row = db.prepare(`SELECT id FROM numeros WHERE instancia=?`).get(nome) as any;
      if (row) {
        db.prepare(`UPDATE numeros SET status=?, telefone=COALESCE(NULLIF(?,''),telefone) WHERE instancia=?`).run(status, tel, nome);
        resumo.push({ instancia: nome, status, novo: false });
      } else {
        const prefixo = String(nome).split('-')[0];
        const projeto = ehProjeto(prefixo) ? prefixo : CONFIG.projetoPadrao;
        db.prepare(`INSERT INTO numeros(id,projeto,telefone,instancia,status,criado_em) VALUES(?,?,?,?,?,?)`)
          .run(randomUUID(), projeto, tel, nome, status, agora());
        resumo.push({ instancia: nome, projeto, status, novo: true });
      }
    }
    res.json({ ok: true, total: resumo.length, numeros: resumo });
  } catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

app.get('/numbers', (_req, res) => res.json(db.prepare(`SELECT * FROM numeros`).all()));
app.get('/numbers/:instancia/status', async (req, res) => {
  try { res.json(await statusNumero(req.params.instancia)); }
  catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

// Recarrega a config (produto->projeto, mensagens) sem reiniciar o serviço.
app.post('/config/reload', (_req, res) => { CONFIG = carregarConfig(); res.json({ ok: true, config: CONFIG }); });
app.get('/config', (_req, res) => res.json(CONFIG));

// ---- Envio 1x1 ----
app.post('/send', async (req, res) => {
  try {
    const { projeto, to, text } = req.body as { projeto: Projeto; to: string; text: string };
    const r = await enviarTexto(projeto, to, text);
    const cid = upsertContato(projeto, to);
    db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,criado_em) VALUES(?,?,?,?,?,?)`)
      .run(randomUUID(), projeto, cid, 'saida', text, agora());
    res.json({ ok: true, evolution: r });
  } catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

// ---- Disparo em massa por TAG (enfileira; worker manda com ritmo humano) ----
app.post('/campaign', (req, res) => {
  const { projeto, tag, texto, grupos } = req.body as { projeto: Projeto; tag: string; texto: string; grupos?: string[] };
  const alvos: { para: string; grupo: boolean }[] = [];
  if (grupos?.length) grupos.forEach((g) => alvos.push({ para: g, grupo: true }));
  if (tag) {
    const rows = db.prepare(
      `SELECT c.telefone FROM contatos c JOIN contato_tags ct ON ct.contato_id=c.id
       JOIN tags t ON t.id=ct.tag_id WHERE c.projeto=? AND t.nome=?`
    ).all(projeto, tag) as any[];
    rows.forEach((r) => alvos.push({ para: r.telefone, grupo: false }));
  }
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,agendado_para,criado_em) VALUES(?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => alvos.forEach((a) => ins.run(randomUUID(), projeto, a.para, a.grupo ? 1 : 0, texto, agora(), agora())));
  tx();
  res.json({ ok: true, enfileirados: alvos.length });
});

// ---- Tags / contatos / CRM (básico) ----
app.post('/tags/apply', (req, res) => {
  const { projeto, telefone, tag, nome, email } = req.body;
  const cid = upsertContato(projeto, telefone, nome, email);
  aplicarTag(projeto, cid, tag);
  res.json({ ok: true, contato: cid });
});
app.get('/contacts', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json(db.prepare(`SELECT * FROM contatos WHERE projeto=? ORDER BY atualizado_em DESC LIMIT 500`).all(projeto));
});

// ---- Webhook da Evolution: mensagem recebida -> salva + auto-tag ----
app.post('/webhook/evolution', (req, res) => {
  try {
    const ev = req.body;
    const data = ev?.data;
    if (ev?.event === 'messages.upsert' && data) {
      const projeto = projetoDaInstancia(String(ev.instance || ''));
      const telefone = String(data.key?.remoteJid || '').replace(/@.*/, '');
      const texto = data.message?.conversation || data.message?.extendedTextMessage?.text || '';
      if (telefone && !data.key?.fromMe) {
        const cid = upsertContato(projeto, telefone, data.pushName);
        db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,criado_em) VALUES(?,?,?,?,?,?)`)
          .run(randomUUID(), projeto, cid, 'entrada', texto, agora());
        // TODO: aqui entra o roteamento pro agente IA / auto-resposta / flow builder
      }
    }
    if (ev?.event === 'connection.update' && data?.state) {
      db.prepare(`UPDATE numeros SET status=? WHERE instancia=?`).run(normalizaEstado(data.state), ev.instance);
    }
    res.json({ ok: true });
  } catch (e: any) { res.status(200).json({ ok: false, erro: String(e.message || e) }); }
});

// ---- Webhook de pagamento: ENTREGA e RECUPERAÇÃO (Guru + LastLink) ----
// URL: /webhook/payment/:provider?token=XXXX  (provider = guru | lastlink)

/** Extrai {pedido, status, telefone, nome, email, produto} de Guru OU LastLink.
 * Formatos (docs oficiais 10/2026):
 *  - LastLink: { Id, IsTest, Event, CreatedAt, Data:{ Buyer:{Email,Name,PhoneNumber}, Products:[{Id,Name,Price}], Offer, Purchase } }
 *       Eventos: Purchase_Order_Confirmed (pago), Purchase_Request_Confirmed/Canceled/Expired, Abandoned_Cart,
 *                Payment_Refund, Payment_Chargeback, Recurrent_Payment, Product_Access_Started/Ended, Subscription_*
 *  - Guru (Digital Manager Guru): { id, status, contact:{name,email,phone_number}, product:{id,name,marketplace_id} }
 *       Status: approved, waiting_payment, billet_printed, abandoned, pending, refused, canceled, refunded, chargeback, trial
 */
function normalizaPagamento(provider: string, b: any): {
  pedido: string; status: string; telefone: string; nome?: string; email?: string; produtoId?: string; produtoNome?: string;
} {
  if (provider === 'lastlink') {
    const d = b?.Data || b?.data || b;
    const buyer = d?.Buyer || d?.buyer || {};
    const prod = (d?.Products || d?.products || [])[0] || d?.Product || {};
    return {
      pedido: String(b?.Id || b?.id || d?.Purchase?.PaymentId || randomUUID()),
      status: String(b?.Event || b?.event || '').toLowerCase(),
      telefone: String(buyer?.PhoneNumber || buyer?.Phone || d?.phone || '').replace(/\D/g, ''),
      nome: buyer?.Name || buyer?.name,
      email: buyer?.Email || buyer?.email,
      produtoId: String(prod?.Id || prod?.id || ''),
      produtoNome: prod?.Name || prod?.name,
    };
  }
  // Guru
  const contato = b?.contact || b?.subscriber || b?.customer || {};
  const prod = b?.product || b?.items?.[0] || {};
  return {
    pedido: String(b?.id || b?.order_id || b?.transaction || b?.subscription?.id || randomUUID()),
    status: String(b?.status || b?.last_status || b?.event || '').toLowerCase(),
    telefone: String(contato?.phone_number || contato?.phone || contato?.cellphone || b?.phone || '').replace(/\D/g, ''),
    nome: contato?.name || b?.customer_name || b?.name,
    email: contato?.email || b?.email,
    produtoId: String(prod?.id || prod?.marketplace_id || b?.product_id || ''),
    produtoNome: prod?.name || b?.product_name,
  };
}

/** Classifica o evento em: aprovado (entrega) | perdido (recuperação) | reembolso | outro. */
function classificaPagamento(provider: string, statusRaw: string): 'aprovado' | 'perdido' | 'reembolso' | 'outro' {
  const s = String(statusRaw || '').toLowerCase();
  if (provider === 'lastlink') {
    if (/refund|chargeback/.test(s)) return 'reembolso';
    if (/order_confirmed|recurrent_payment|product_access_started/.test(s)) return 'aprovado';
    if (/canceled|cancelled|expired|abandoned/.test(s)) return 'perdido';
    return 'outro';
  }
  // guru
  if (/refund|chargeback|reembols|estorn/.test(s)) return 'reembolso';
  if (/^approved$|aprovad|^paid$|complete/.test(s)) return 'aprovado';
  // recuperação: abandono / recusado / cancelado / expirado (NÃO waiting_payment, que ainda pode pagar o boleto/pix)
  if (/abandon|refus|recus|cancel|expired|expirad/.test(s)) return 'perdido';
  return 'outro';
}

app.post('/webhook/payment/:provider', async (req, res) => {
  try {
    const provider = String(req.params.provider || '').toLowerCase();
    // valida o token da URL por provedor
    const tokenEsperado = provider === 'guru' ? GURU_TOKEN : provider === 'lastlink' ? LASTLINK_TOKEN : '';
    if (!tokenEsperado || String(req.query.token || '') !== tokenEsperado) {
      return res.status(401).json({ erro: 'token_invalido' });
    }
    const b = req.body || {};
    const p = normalizaPagamento(provider, b);

    // dedupe por pedido
    const ja = db.prepare(`SELECT 1 FROM pagamentos_processados WHERE provider=? AND pedido=?`).get(provider, p.pedido);
    if (ja) return res.json({ ok: true, dedupe: true });
    db.prepare(`INSERT INTO pagamentos_processados(provider,pedido,criado_em) VALUES(?,?,?)`).run(provider, p.pedido, agora());

    const projeto = projetoDoProduto(p.produtoId, p.produtoNome);
    const categoria = classificaPagamento(provider, p.status);
    if (p.telefone) {
      const cid = upsertContato(projeto, p.telefone, p.nome, p.email, `pagamento:${provider}`);
      if (categoria === 'aprovado') {
        aplicarTag(projeto, cid, 'comprou');
        await enviarTexto(projeto, p.telefone, preenche(CONFIG.entrega[projeto], p.nome)).catch(() => {});
      } else if (categoria === 'perdido') {
        aplicarTag(projeto, cid, 'recuperacao');
        await enviarTexto(projeto, p.telefone, preenche(CONFIG.recuperacao[projeto], p.nome)).catch(() => {});
      } else if (categoria === 'reembolso') {
        aplicarTag(projeto, cid, 'reembolso'); // sem mensagem automática
      }
    }
    res.json({ ok: true, projeto, status: p.status, categoria });
  } catch (e: any) { res.status(200).json({ ok: false, erro: String(e.message || e) }); }
});

// ----------------------- Worker de disparo (ritmo humano / anti-ban) -----------------------
let enviando = false;
async function tickFila() {
  if (enviando) return; enviando = true;
  try {
    const item = db.prepare(`SELECT * FROM fila_envio WHERE status='pendente' ORDER BY criado_em LIMIT 1`).get() as any;
    if (item) {
      try {
        await enviarTexto(item.projeto, item.para, item.texto);
        db.prepare(`UPDATE fila_envio SET status='enviado' WHERE id=?`).run(item.id);
      } catch {
        db.prepare(`UPDATE fila_envio SET tentativas=tentativas+1, status=CASE WHEN tentativas>=3 THEN 'falhou' ELSE 'pendente' END WHERE id=?`).run(item.id);
      }
    }
  } finally { enviando = false; }
}
// 1 envio a cada 8–15s (ritmo humano). Ajustar por número/aquecimento depois.
setInterval(tickFila, 8000 + Math.floor(Math.random() * 7000));

app.listen(PORT, () => console.log(`[engine] ouvindo na porta ${PORT} — base ${PUBLIC_BASE_URL}`));
