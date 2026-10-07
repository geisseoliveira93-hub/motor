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
// (Opcional) Token do webhook da Cademí. As vendas hoje só passam por Guru/LastLink,
// então a Cademí NÃO é necessária como gatilho de venda. Mas deixamos o endpoint pronto:
// se um dia houver venda pelo checkout da própria Cademí, basta pôr CADEMI_TOKEN no .env
// e criar o webhook na Cademí apontando pra /webhook/cademi?token=...
const CADEMI_TOKEN = process.env.CADEMI_TOKEN || '';
// Segurança: liga/desliga a página de importação de leads (/importar e /import/leads).
// Depois de terminar a migração, pôr IMPORTAR_ATIVO=false no .env pra desligar a porta de entrada.
const IMPORTAR_ATIVO = (process.env.IMPORTAR_ATIVO || 'true') !== 'false';

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
  agendado_para TEXT, criado_em TEXT,
  tipo TEXT DEFAULT 'texto', url TEXT, legenda TEXT
);
CREATE TABLE IF NOT EXISTS pagamentos_processados (
  provider TEXT, pedido TEXT, criado_em TEXT, PRIMARY KEY (provider, pedido)
);
CREATE TABLE IF NOT EXISTS eventos (
  id TEXT PRIMARY KEY, projeto TEXT, titulo TEXT, inicio TEXT, fim TEXT,
  contato_id TEXT, obs TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS respostas_rapidas (
  id TEXT PRIMARY KEY, projeto TEXT, atalho TEXT, texto TEXT, criado_em TEXT
);
`);

// Tabelas da V4 (usabilidade): listas, segmentos, campos customizados, pipelines,
// motivos, tickets, tarefas, produtos, departamentos, roteiros, avaliacoes, equipe, transacoes.
db.exec(`
CREATE TABLE IF NOT EXISTS listas (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS contato_listas (
  contato_id TEXT, lista_id TEXT, criado_em TEXT, PRIMARY KEY (contato_id, lista_id)
);
CREATE TABLE IF NOT EXISTS segmentos (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, filtro TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS campos_customizados (
  id TEXT PRIMARY KEY, projeto TEXT, chave TEXT, rotulo TEXT, tipo TEXT DEFAULT 'texto', criado_em TEXT,
  UNIQUE(projeto, chave)
);
CREATE TABLE IF NOT EXISTS contato_campos (
  contato_id TEXT, campo_id TEXT, valor TEXT, PRIMARY KEY (contato_id, campo_id)
);
CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, tipo TEXT DEFAULT 'vendas', etapas TEXT, ordem INTEGER DEFAULT 0, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS motivos (
  id TEXT PRIMARY KEY, projeto TEXT, tipo TEXT DEFAULT 'ganho', nome TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS tickets (
  id TEXT PRIMARY KEY, projeto TEXT, contato_id TEXT, titulo TEXT, descricao TEXT,
  status TEXT DEFAULT 'aberto', prioridade TEXT DEFAULT 'media', criado_em TEXT, atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS tarefas (
  id TEXT PRIMARY KEY, projeto TEXT, titulo TEXT, responsavel TEXT, prazo TEXT,
  concluida INTEGER DEFAULT 0, contato_id TEXT, criado_em TEXT, atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS produtos (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, preco REAL DEFAULT 0, descricao TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS departamentos (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS roteiros (
  id TEXT PRIMARY KEY, projeto TEXT, titulo TEXT, texto TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS avaliacoes (
  id TEXT PRIMARY KEY, projeto TEXT, contato_id TEXT, nota INTEGER DEFAULT 0, comentario TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS equipe (
  id TEXT PRIMARY KEY, nome TEXT, email TEXT, papel TEXT DEFAULT 'operador', ativo INTEGER DEFAULT 1, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS transacoes (
  id TEXT PRIMARY KEY, projeto TEXT, contato_id TEXT, produto TEXT, valor REAL DEFAULT 0,
  status TEXT DEFAULT 'pago', provider TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS automacoes (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, ativo INTEGER DEFAULT 1,
  gatilho TEXT, condicao TEXT, acoes TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS automacao_logs (
  id TEXT PRIMARY KEY, automacao_id TEXT, projeto TEXT, contato_id TEXT, gatilho TEXT, detalhe TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS api_tokens (
  id TEXT PRIMARY KEY, nome TEXT, token TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS paginas (
  id TEXT PRIMARY KEY, projeto TEXT, slug TEXT UNIQUE, titulo TEXT, html TEXT,
  publicada INTEGER DEFAULT 0, criado_em TEXT, atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS formularios (
  id TEXT PRIMARY KEY, projeto TEXT, titulo TEXT, campos TEXT, tag TEXT, redirecionar TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS formulario_respostas (
  id TEXT PRIMARY KEY, formulario_id TEXT, projeto TEXT, contato_id TEXT, dados TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS pixels (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, plataforma TEXT, pixel_id TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS email_templates (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, assunto TEXT, html TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS email_dominios (
  id TEXT PRIMARY KEY, projeto TEXT, dominio TEXT, smtp_host TEXT, smtp_porta INTEGER, smtp_usuario TEXT, remetente TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS email_fila (
  id TEXT PRIMARY KEY, projeto TEXT, para TEXT, assunto TEXT, html TEXT, status TEXT DEFAULT 'pendente', criado_em TEXT
);
CREATE TABLE IF NOT EXISTS comentarios (
  id TEXT PRIMARY KEY, projeto TEXT, rede TEXT, post_id TEXT, autor TEXT, texto TEXT,
  respondido INTEGER DEFAULT 0, resposta TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS templates_whatsapp (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, idioma TEXT DEFAULT 'pt_BR', categoria TEXT DEFAULT 'UTILITY',
  corpo TEXT, status TEXT DEFAULT 'rascunho', criado_em TEXT
);
CREATE TABLE IF NOT EXISTS webhooks_saida (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, url TEXT, evento TEXT, ativo INTEGER DEFAULT 1, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS integracao_eventos (
  id TEXT PRIMARY KEY, projeto TEXT, origem TEXT, tipo TEXT, dados TEXT, criado_em TEXT
);
CREATE TABLE IF NOT EXISTS apps_externos (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, base_url TEXT, descricao TEXT, criado_em TEXT
);
`);

// Migração suave: bancos ANTIGOS (já no ar) não têm as colunas novas da fila.
// ALTER TABLE ... ADD COLUMN é seguro; se a coluna já existir, ignora o erro.
for (const alter of [
  `ALTER TABLE fila_envio ADD COLUMN tipo TEXT DEFAULT 'texto'`,
  `ALTER TABLE fila_envio ADD COLUMN url TEXT`,
  `ALTER TABLE fila_envio ADD COLUMN legenda TEXT`,
]) {
  try { db.exec(alter); } catch { /* coluna já existe */ }
}

// Migração suave: colunas novas em negocios (vários funis + motivo de ganho/perda).
for (const alter of [
  `ALTER TABLE negocios ADD COLUMN pipeline_id TEXT`,
  `ALTER TABLE negocios ADD COLUMN motivo TEXT`,
  `ALTER TABLE negocios ADD COLUMN motivo_tipo TEXT`,
]) {
  try { db.exec(alter); } catch { /* coluna já existe */ }
}

// Etapa 9: colunas novas p/ o construtor de Paginas (blocos) e o Quiz multi-passos.
for (const alter of [
  `ALTER TABLE paginas ADD COLUMN blocos TEXT`,
  `ALTER TABLE paginas ADD COLUMN pasta TEXT`,
  `ALTER TABLE paginas ADD COLUMN idioma TEXT DEFAULT 'pt'`,
  `ALTER TABLE formularios ADD COLUMN passos TEXT`,
  `ALTER TABLE formularios ADD COLUMN tema TEXT`,
  `ALTER TABLE formularios ADD COLUMN acessos INTEGER DEFAULT 0`,
]) {
  try { db.exec(alter); } catch { /* coluna já existe */ }
}

// Etapa 11: tabela de Empresas (cadastro B2B do projeto).
db.exec(`CREATE TABLE IF NOT EXISTS empresas (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, cnpj TEXT, site TEXT, telefone TEXT, criado_em TEXT
);`);

const agora = () => new Date().toISOString();
const emSegundos = (s: number) => new Date(Date.now() + Math.max(0, s) * 1000).toISOString();

// ----------------------- Config editável (sem redeploy) -----------------------
// Carrega /data/config.json se existir; senão usa o default abaixo.
// O Ezequias/eu editamos esse arquivo pra mapear produto->projeto e as mensagens,
// sem precisar rebuildar o container.
// Um PASSO de um fluxo de mensagens (igual aos blocos da SellFlux):
//  - tipo: texto | imagem | video | audio | documento
//  - texto: o conteúdo (pro tipo texto) OU a legenda da mídia
//  - url: endereço do material (imagem/vídeo/áudio/PDF) — pros low-tickets entregues por WhatsApp
//  - delaySegundos: quanto esperar ANTES de mandar este passo (ritmo humano entre as mensagens)
// {nome} é trocado pelo nome do comprador em texto e legenda.
type FluxoPasso = {
  tipo?: 'texto' | 'imagem' | 'video' | 'audio' | 'documento';
  texto?: string;
  legenda?: string;
  url?: string;
  delaySegundos?: number;
};
// Um fluxo por PRODUTO: casa pelo id OU por um pedaço do nome do produto, e entrega
// o fluxo escolhido. Serve pra "integrar o fluxo à entrega do produto que eu quiser".
type FluxoProduto = { chave: string; porId?: boolean; projeto?: Projeto; fluxo: FluxoPasso[] };

type Config = {
  // Mapa de produto (id OU nome, em minúsculas) -> projeto.
  produtoProjeto: Record<string, Projeto>;
  // (Opcional) Mapa por ID EXATO do produto -> projeto. Mais confiável pros casos
  // ambíguos (produtos de nome parecido entre instrumentos). Checado ANTES de tudo.
  produtoProjetoId?: Record<string, Projeto>;
  // Palavra que aparece no produto -> projeto (fallback por "contém").
  palavraChaveProjeto: { contem: string; projeto: Projeto }[];
  // Projeto usado quando não dá pra identificar.
  projetoPadrao: Projeto;
  // Mensagens por projeto (modo simples). {nome} é substituído pelo nome do comprador.
  entrega: Record<Projeto, string>;
  recuperacao: Record<Projeto, string>;
  // (Opcional) FLUXOS por projeto — vários passos com mídia e delay. Quando existir
  // um fluxo pro projeto, ele é usado NO LUGAR da mensagem simples de entrega/recuperação.
  entregaFluxo?: Partial<Record<Projeto, FluxoPasso[]>>;
  recuperacaoFluxo?: Partial<Record<Projeto, FluxoPasso[]>>;
  // (Opcional) FLUXOS por PRODUTO (ex.: um low-ticket que entrega o ebook direto no WhatsApp).
  // Tem prioridade sobre o fluxo do projeto. Casa pelo nome (padrão) ou por id (porId:true).
  entregaFluxoProduto?: FluxoProduto[];
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

// ----------------------- Salvar config (construtor de fluxos do app) -----------------------
// Grava o CONFIG atual em /data/config.json (volume do engine). Usado pelos endpoints /flows
// pro app publicar os fluxos de entrega/recuperacao sem editar o arquivo na mao.
function salvarConfig(): void {
  fs.mkdirSync('/data', { recursive: true });
  fs.writeFileSync('/data/config.json', JSON.stringify(CONFIG, null, 2), 'utf8');
}


/** Descobre o projeto de um pagamento pelo produto (id, nome).
 * Ordem (do mais confiável pro mais genérico):
 *  1) ID EXATO do produto (produtoProjetoId) — resolve nomes ambíguos entre instrumentos.
 *  2) PALAVRA-CHAVE no nome (baixo/violão/teclado) — cobre a grande maioria dos produtos.
 *  3) Pedaço do NOME cadastrado (produtoProjeto) — pros nomes que NÃO têm o instrumento
 *     (ex.: "pestana perfeita"->violao, "violonista"->violao).
 *  4) Projeto padrão.
 * Obs.: a palavra-chave vem ANTES do mapa por nome de propósito — assim um produto
 * "...no Violão" nunca cai num atalho genérico antes de bater o instrumento do nome.
 */
function projetoDoProduto(idOuNome: string | undefined, nome?: string): Projeto {
  const id = String(idOuNome || '').toLowerCase().trim();
  const alvo = `${idOuNome || ''} ${nome || ''}`.toLowerCase().trim();
  // 1) ID exato do produto
  const porId = CONFIG.produtoProjetoId || {};
  if (id && porId[id]) return porId[id];
  for (const chave of Object.keys(porId)) {
    if (id && id === chave.toLowerCase()) return porId[chave];
  }
  // 2) por palavra-chave (baixo/violao/teclado no nome)
  for (const { contem, projeto } of CONFIG.palavraChaveProjeto) {
    if (alvo.includes(contem.toLowerCase())) return projeto;
  }
  // 3) pedaço do nome cadastrado (atalhos pros nomes sem instrumento)
  for (const chave of Object.keys(CONFIG.produtoProjeto)) {
    if (alvo.includes(chave.toLowerCase())) return CONFIG.produtoProjeto[chave];
  }
  // 4) padrão
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

/** Pega o 1º número conectado do projeto (base do rodízio, que entra depois). */
function numeroConectado(projeto: Projeto): any {
  return db.prepare(
    `SELECT * FROM numeros WHERE projeto=? AND status='conectado' ORDER BY criado_em LIMIT 1`
  ).get(projeto);
}

/** Envia texto por um número do projeto (pega o 1º conectado como padrão). */
async function enviarTexto(projeto: Projeto, para: string, texto: string) {
  const num = numeroConectado(projeto);
  if (!num) throw new Error(`Nenhum número conectado no projeto ${projeto}`);
  return evo(`/message/sendText/${num.instancia}`, 'POST', {
    number: para, text: texto,
  });
}

/** Envia MÍDIA (imagem/vídeo/áudio/documento) por URL, por um número do projeto. */
async function enviarMidia(projeto: Projeto, para: string, tipo: string, url: string, legenda?: string) {
  const num = numeroConectado(projeto);
  if (!num) throw new Error(`Nenhum número conectado no projeto ${projeto}`);
  if (!url) throw new Error('midia_sem_url');
  if (tipo === 'audio') {
    return evo(`/message/sendWhatsAppAudio/${num.instancia}`, 'POST', { number: para, audio: url });
  }
  const mediatype = tipo === 'video' ? 'video' : tipo === 'documento' ? 'document' : 'image';
  const body: any = { number: para, mediatype, media: url };
  if (legenda) body.caption = preenche(legenda);
  if (mediatype === 'document') body.fileName = (url.split('/').pop() || 'arquivo').split('?')[0];
  return evo(`/message/sendMedia/${num.instancia}`, 'POST', body);
}

/** Envia UM passo de fluxo (texto ou mídia), já com {nome} preenchido. */
async function enviarPasso(projeto: Projeto, para: string, passo: FluxoPasso, nome?: string) {
  const tipo = passo.tipo || 'texto';
  if (tipo === 'texto') {
    return enviarTexto(projeto, para, preenche(passo.texto || '', nome));
  }
  return enviarMidia(projeto, para, tipo, passo.url || '', preenche(passo.texto || passo.legenda || '', nome));
}

/** Enfileira um FLUXO (vários passos com delay) pra um contato. O worker manda no ritmo,
 * respeitando o delay de cada passo (agendado_para). Preenche {nome} em texto/legenda. */
function enfileirarFluxo(projeto: Projeto, para: string, passos: FluxoPasso[], nome?: string) {
  const ins = db.prepare(
    `INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,agendado_para,criado_em)
     VALUES(?,?,?,?,?,?,?,?,?,?,?)`
  );
  let acumulado = 0;
  const tx = db.transaction(() => {
    for (const p of passos || []) {
      acumulado += Math.max(0, Number(p.delaySegundos || 0));
      const tipo = p.tipo || 'texto';
      const texto = tipo === 'texto' ? preenche(p.texto || '', nome) : '';
      const legenda = tipo === 'texto' ? '' : preenche(p.texto || p.legenda || '', nome);
      ins.run(randomUUID(), projeto, para, 0, texto, tipo, p.url || null, legenda || null,
        'pendente', emSegundos(acumulado), agora());
    }
  });
  tx();
  return (passos || []).length;
}

/** Acha o FLUXO de entrega de um produto: 1º por produto (id/nome), 2º por projeto. */
function fluxoEntregaDoProduto(projeto: Projeto, produtoId?: string, produtoNome?: string): FluxoPasso[] | null {
  const alvo = `${produtoId || ''} ${produtoNome || ''}`.toLowerCase();
  const id = String(produtoId || '').toLowerCase();
  for (const fp of CONFIG.entregaFluxoProduto || []) {
    const chave = String(fp.chave || '').toLowerCase();
    if (!chave) continue;
    const casa = fp.porId ? (id && id === chave) : alvo.includes(chave);
    if (casa && Array.isArray(fp.fluxo) && fp.fluxo.length) return fp.fluxo;
  }
  const porProjeto = CONFIG.entregaFluxo?.[projeto];
  if (Array.isArray(porProjeto) && porProjeto.length) return porProjeto;
  return null;
}

/** Dispara a ENTREGA: usa o fluxo do produto/projeto se houver; senão a mensagem simples. */
async function dispararEntrega(projeto: Projeto, telefone: string, nome?: string, produtoId?: string, produtoNome?: string): Promise<'fluxo' | 'mensagem'> {
  const fluxo = fluxoEntregaDoProduto(projeto, produtoId, produtoNome);
  if (fluxo) { enfileirarFluxo(projeto, telefone, fluxo, nome); return 'fluxo'; }
  await enviarTexto(projeto, telefone, preenche(CONFIG.entrega[projeto], nome)).catch(() => {});
  return 'mensagem';
}
/** Dispara a RECUPERAÇÃO: usa o fluxo do projeto se houver; senão a mensagem simples. */
async function dispararRecuperacao(projeto: Projeto, telefone: string, nome?: string): Promise<'fluxo' | 'mensagem'> {
  const fluxo = CONFIG.recuperacaoFluxo?.[projeto];
  if (Array.isArray(fluxo) && fluxo.length) { enfileirarFluxo(projeto, telefone, fluxo, nome); return 'fluxo'; }
  await enviarTexto(projeto, telefone, preenche(CONFIG.recuperacao[projeto], nome)).catch(() => {});
  return 'mensagem';
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
// aceita CSV cru no corpo (pra importação em massa de leads da SellFlux)
app.use(express.text({ type: ['text/csv', 'application/csv', 'text/plain'], limit: '120mb' }));

// Webhooks de pagamento NÃO exigem nossa chave (vêm de fora), mas são validados por token na URL.
// Todo o resto exige x-api-key.
// ---- Segurança: cabeçalhos básicos ----
app.use((_req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  next();
});

// ---- Segurança: liga/desliga a porta de entrada da importação ----
// Quando IMPORTAR_ATIVO=false, a página e o envio de importação respondem 403.
app.use((req, res, next) => {
  if (!IMPORTAR_ATIVO && (req.path === '/importar' || req.path === '/import/leads')) {
    return res.status(403).json({ erro: 'importacao_desativada' });
  }
  next();
});

// ---- Segurança: anti-força-bruta na chave (rate limit simples, em memória) ----
// Se um mesmo IP errar a chave muitas vezes em pouco tempo, bloqueia por uns minutos.
const falhasAuth = new Map<string, { n: number; ate: number }>();
function ipDoReq(req: express.Request): string {
  return String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'desconhecido';
}

app.use((req, res, next) => {
  if (req.path.startsWith('/webhook/')) return next();
  if (req.path === '/health') return next();
  if (req.path === '/importar' && req.method === 'GET') return next(); // só a PÁGINA; o envio ainda exige a chave
  // Rotas PÚBLICAS da V4: páginas (/p/:slug), formulários (/f/:id) e recebimento de eventos
  // (/receive/:token, validado pelo token na URL). O resto continua exigindo a x-api-key.
  if (req.path.startsWith('/p/')) return next();
  if (req.path.startsWith('/f/')) return next();
  if (req.path.startsWith('/receive/')) return next();
  if (req.path.startsWith('/q/')) return next(); // Etapa 9: quiz multi-passos publico
  const ip = ipDoReq(req);
  const agora2 = Date.now();
  const reg = falhasAuth.get(ip);
  if (reg && reg.ate > agora2 && reg.n >= 20) {
    return res.status(429).json({ erro: 'muitas_tentativas', tente_em_segundos: Math.ceil((reg.ate - agora2) / 1000) });
  }
  if ((req.header('x-api-key') || '') !== ENGINE_API_KEY) {
    const r = (reg && reg.ate > agora2) ? reg : { n: 0, ate: agora2 + 300000 }; // janela de 5 min
    r.n++; falhasAuth.set(ip, r);
    return res.status(401).json({ erro: 'sem_autorizacao' });
  }
  if (reg) falhasAuth.delete(ip); // acertou a chave -> limpa o contador
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

// ======================= API SELLFLUX (CRM / Kanban / Agenda / Inbox / Respostas) =======================
// Tudo protegido por x-api-key. Reusa tabelas existentes + eventos/respostas_rapidas.
function tagsDoContato(contatoId: string): string[] {
  return (db.prepare(`SELECT t.nome FROM contato_tags ct JOIN tags t ON t.id=ct.tag_id WHERE ct.contato_id=?`).all(contatoId) as any[]).map((r) => r.nome);
}
// ---- CRM: contatos (busca + filtro por tag + paginacao) ----
app.get('/crm/contatos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const busca = String(req.query.busca || '').trim();
  const tag = String(req.query.tag || '').trim();
  const limit = Math.min(500, Math.max(1, Number(req.query.limit || 100)));
  const offset = Math.max(0, Number(req.query.offset || 0));
  const cond: string[] = ['c.projeto=?']; const args: any[] = [projeto];
  if (busca) { cond.push('(c.nome LIKE ? OR c.telefone LIKE ? OR c.email LIKE ?)'); args.push('%' + busca + '%', '%' + busca + '%', '%' + busca + '%'); }
  let join = '';
  if (tag) { join = 'JOIN contato_tags ct ON ct.contato_id=c.id JOIN tags t ON t.id=ct.tag_id'; cond.push('t.nome=?'); args.push(tag); }
  const sql = 'SELECT c.* FROM contatos c ' + join + ' WHERE ' + cond.join(' AND ') + ' ORDER BY c.atualizado_em DESC LIMIT ? OFFSET ?';
  const rows = db.prepare(sql).all(...args, limit, offset) as any[];
  rows.forEach((r) => (r.tags = tagsDoContato(r.id)));
  res.json({ ok: true, contatos: rows });
});
app.get('/crm/contatos/count', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const busca = String(req.query.busca || '').trim();
  const tag = String(req.query.tag || '').trim();
  const cond: string[] = ['c.projeto=?']; const args: any[] = [projeto];
  if (busca) { cond.push('(c.nome LIKE ? OR c.telefone LIKE ? OR c.email LIKE ?)'); args.push('%' + busca + '%', '%' + busca + '%', '%' + busca + '%'); }
  let join = '';
  if (tag) { join = 'JOIN contato_tags ct ON ct.contato_id=c.id JOIN tags t ON t.id=ct.tag_id'; cond.push('t.nome=?'); args.push(tag); }
  const r = db.prepare('SELECT COUNT(DISTINCT c.id) n FROM contatos c ' + join + ' WHERE ' + cond.join(' AND ')).get(...args) as any;
  res.json({ ok: true, total: (r && r.n) || 0 });
});
app.get('/crm/contato/:id', (req, res) => {
  const c = db.prepare(`SELECT * FROM contatos WHERE id=?`).get(req.params.id) as any;
  if (!c) return res.status(404).json({ erro: 'nao_encontrado' });
  c.tags = tagsDoContato(c.id);
  c.mensagens = db.prepare(`SELECT * FROM mensagens WHERE contato_id=? ORDER BY criado_em DESC LIMIT 50`).all(c.id);
  c.negocios = db.prepare(`SELECT * FROM negocios WHERE contato_id=? ORDER BY atualizado_em DESC`).all(c.id);
  res.json({ ok: true, contato: c });
});
app.get('/crm/tags', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT t.nome, COUNT(ct.contato_id) n FROM tags t LEFT JOIN contato_tags ct ON ct.tag_id=t.id WHERE t.projeto=? GROUP BY t.id ORDER BY n DESC`).all(projeto);
  res.json({ ok: true, tags: rows });
});
app.post('/crm/contato/tags', (req, res) => {
  const b = req.body || {};
  const projeto = String(b.projeto || ''); const contatoId = String(b.contatoId || '');
  if (!contatoId) return res.status(400).json({ erro: 'contato_obrigatorio' });
  (Array.isArray(b.add) ? b.add : []).forEach((nome: string) => aplicarTag(projeto, contatoId, String(nome)));
  (Array.isArray(b.remove) ? b.remove : []).forEach((nome: string) => {
    const t = db.prepare(`SELECT id FROM tags WHERE projeto=? AND nome=?`).get(projeto, String(nome)) as any;
    if (t) db.prepare(`DELETE FROM contato_tags WHERE contato_id=? AND tag_id=?`).run(contatoId, t.id);
  });
  // Automacoes: gatilho "tag_adicionada" (condicao = nome exato da tag) pra cada tag adicionada.
  const ctt = db.prepare(`SELECT telefone, nome FROM contatos WHERE id=?`).get(contatoId) as any;
  (Array.isArray(b.add) ? b.add : []).forEach((nome: string) => rodarAutomacoes(projeto, 'tag_adicionada', { contatoId, tag: String(nome), telefone: ctt?.telefone, nome: ctt?.nome }).catch(() => {}));
  res.json({ ok: true, tags: tagsDoContato(contatoId) });
});
// ---- Kanban / Negocios ----
app.get('/crm/negocios', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT n.*, c.nome contato_nome, c.telefone contato_telefone FROM negocios n LEFT JOIN contatos c ON c.id=n.contato_id WHERE n.projeto=? ORDER BY n.atualizado_em DESC`).all(projeto);
  res.json({ ok: true, negocios: rows });
});
app.post('/crm/negocio', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM negocios WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE negocios SET titulo=?, valor=?, etapa=?, contato_id=?, atualizado_em=? WHERE id=?`).run(String(b.titulo || ''), Number(b.valor || 0), String(b.etapa || 'novo'), b.contatoId || null, agora(), id);
  else db.prepare(`INSERT INTO negocios(id,projeto,contato_id,titulo,valor,etapa,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), b.contatoId || null, String(b.titulo || ''), Number(b.valor || 0), String(b.etapa || 'novo'), agora(), agora());
  res.json({ ok: true, id });
});
app.post('/crm/negocio/etapa', (req, res) => {
  const b = req.body || {};
  db.prepare(`UPDATE negocios SET etapa=?, atualizado_em=? WHERE id=?`).run(String(b.etapa || 'novo'), agora(), String(b.id || ''));
  res.json({ ok: true });
});
app.post('/crm/negocio/remover', (req, res) => { db.prepare(`DELETE FROM negocios WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
// ---- Agenda / Eventos ----
app.get('/agenda/eventos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, eventos: db.prepare(`SELECT * FROM eventos WHERE projeto=? ORDER BY inicio ASC`).all(projeto) });
});
app.post('/agenda/evento', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM eventos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE eventos SET titulo=?, inicio=?, fim=?, contato_id=?, obs=? WHERE id=?`).run(String(b.titulo || ''), String(b.inicio || ''), String(b.fim || ''), b.contatoId || null, String(b.obs || ''), id);
  else db.prepare(`INSERT INTO eventos(id,projeto,titulo,inicio,fim,contato_id,obs,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.titulo || ''), String(b.inicio || ''), String(b.fim || ''), b.contatoId || null, String(b.obs || ''), agora());
  res.json({ ok: true, id });
});
app.post('/agenda/evento/remover', (req, res) => { db.prepare(`DELETE FROM eventos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
// ---- Mensagens rapidas ----
app.get('/crm/respostas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, respostas: db.prepare(`SELECT * FROM respostas_rapidas WHERE projeto=? ORDER BY atalho ASC`).all(projeto) });
});
app.post('/crm/resposta', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM respostas_rapidas WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE respostas_rapidas SET atalho=?, texto=? WHERE id=?`).run(String(b.atalho || ''), String(b.texto || ''), id);
  else db.prepare(`INSERT INTO respostas_rapidas(id,projeto,atalho,texto,criado_em) VALUES(?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.atalho || ''), String(b.texto || ''), agora());
  res.json({ ok: true, id });
});
app.post('/crm/resposta/remover', (req, res) => { db.prepare(`DELETE FROM respostas_rapidas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
// ---- Inbox / Conversas ----
app.get('/inbox/conversas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare('SELECT c.id contato_id, c.nome, c.telefone, m.texto ultima, m.direcao, MAX(m.criado_em) quando FROM mensagens m JOIN contatos c ON c.id=m.contato_id WHERE m.projeto=? GROUP BY c.id ORDER BY quando DESC LIMIT 100').all(projeto);
  res.json({ ok: true, conversas: rows });
});
app.get('/inbox/mensagens', (req, res) => {
  const contatoId = String(req.query.contatoId || '');
  res.json({ ok: true, mensagens: db.prepare(`SELECT * FROM mensagens WHERE contato_id=? ORDER BY criado_em ASC LIMIT 300`).all(contatoId) });
});
// ---- Campanhas: resumo da fila ----
app.get('/campaigns/resumo', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, porStatus: db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE projeto=? GROUP BY status`).all(projeto) });
});

// ----------------------- API de FLUXOS (construtor de fluxos do app) -----------------------
// Protegidos por x-api-key (middleware acima). O processo principal do app chama estes
// endpoints pra LISTAR/SALVAR/REMOVER os fluxos. O runner existente (dispararEntrega/
// dispararRecuperacao/enfileirarFluxo + fluxoEntregaDoProduto) ja executa esses fluxos.
function normalizaFluxo(f: any): FluxoPasso[] {
  return (Array.isArray(f) ? f : []).map((p: any) => ({
    tipo: p?.tipo || 'texto',
    texto: p?.texto,
    legenda: p?.legenda,
    url: p?.url,
    delaySegundos: Number(p?.delaySegundos || 0),
  }));
}

// Lista todos os fluxos cadastrados (por produto e por projeto).
app.get('/flows', (_req, res) => {
  res.json({
    ok: true,
    entregaFluxoProduto: CONFIG.entregaFluxoProduto || [],
    entregaFluxo: CONFIG.entregaFluxo || {},
    recuperacaoFluxo: CONFIG.recuperacaoFluxo || {},
  });
});

// Cria/atualiza um fluxo de ENTREGA por PRODUTO. body: { chave, porId?, projeto?, fluxo:[] }
app.post('/flows/produto', (req, res) => {
  const b = req.body || {};
  const chave = String(b.chave || '').trim();
  if (!chave) return res.status(400).json({ erro: 'chave_obrigatoria' });
  const item: FluxoProduto = { chave, porId: !!b.porId, projeto: b.projeto, fluxo: normalizaFluxo(b.fluxo) };
  const lista = CONFIG.entregaFluxoProduto || [];
  const i = lista.findIndex((f) => f.chave.toLowerCase() === chave.toLowerCase() && !!f.porId === !!b.porId);
  if (i >= 0) lista[i] = item; else lista.push(item);
  CONFIG.entregaFluxoProduto = lista;
  salvarConfig();
  res.json({ ok: true, total: lista.length, item });
});

// Remove um fluxo de entrega por produto (pela chave).
app.post('/flows/produto/remover', (req, res) => {
  const chave = String((req.body || {}).chave || '').trim().toLowerCase();
  if (!chave) return res.status(400).json({ erro: 'chave_obrigatoria' });
  const antes = (CONFIG.entregaFluxoProduto || []).length;
  CONFIG.entregaFluxoProduto = (CONFIG.entregaFluxoProduto || []).filter((f) => f.chave.toLowerCase() !== chave);
  salvarConfig();
  res.json({ ok: true, removidos: antes - (CONFIG.entregaFluxoProduto || []).length });
});

// Salva um fluxo por PROJETO (entrega OU recuperacao). body: { projeto, tipo, fluxo:[] }
app.post('/flows/projeto', (req, res) => {
  const b = req.body || {};
  const projeto = b.projeto as Projeto;
  if (!PROJETOS.includes(projeto)) return res.status(400).json({ erro: 'projeto_invalido' });
  const tipo = b.tipo === 'recuperacao' ? 'recuperacao' : 'entrega';
  const fluxo = normalizaFluxo(b.fluxo);
  if (tipo === 'entrega') CONFIG.entregaFluxo = { ...(CONFIG.entregaFluxo || {}), [projeto]: fluxo };
  else CONFIG.recuperacaoFluxo = { ...(CONFIG.recuperacaoFluxo || {}), [projeto]: fluxo };
  salvarConfig();
  res.json({ ok: true, projeto, tipo, passos: fluxo.length });
});


// Página simples de importação de leads (sem precisar de terminal).
app.get('/importar', (_req, res) => {
  res.type('html').send(`<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Importar leads - Melodias e Riffs</title>
<style>
  :root{color-scheme:dark} *{box-sizing:border-box}
  body{margin:0;font:16px/1.5 system-ui,Segoe UI,Arial;background:#0e1116;color:#e6e6e6;display:flex;justify-content:center;padding:28px 16px}
  .card{width:100%;max-width:520px;background:#161b22;border:1px solid #2a313c;border-radius:14px;padding:24px}
  h1{font-size:20px;margin:0 0 4px} p.sub{color:#9aa4b2;margin:0 0 20px;font-size:14px}
  label{display:block;margin:14px 0 6px;font-weight:600}
  input,select{width:100%;padding:11px 12px;border-radius:9px;border:1px solid #2a313c;background:#0e1116;color:#e6e6e6;font-size:15px}
  button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:9px;background:#1db954;color:#07210f;font-weight:700;font-size:16px;cursor:pointer}
  button:disabled{opacity:.5;cursor:default}
  #res{margin-top:18px;white-space:pre-wrap;font-size:14px;padding:12px;border-radius:9px;display:none}
  .ok{background:#0f2e1b;border:1px solid #1db95455} .err{background:#2e1313;border:1px solid #b91d1d55}
  small{color:#9aa4b2}
</style></head><body><div class="card">
<h1>Importar leads 🎸</h1>
<p class="sub">Suba o arquivo .CSV exportado da SellFlux. Os contatos e tags entram no projeto escolhido.</p>
<label>Chave do motor (ENGINE_API_KEY)</label>
<input id="key" type="password" placeholder="cole sua chave aqui" autocomplete="off">
<small>Fica no seu arquivo F:\\Claude\\_VPS\\chaves-motor.txt</small>
<label>Projeto</label>
<select id="proj"><option value="teclado">Teclado</option><option value="violao">Violão</option><option value="baixo">Baixo</option></select>
<label>Arquivo CSV</label>
<input id="file" type="file" accept=".csv,text/csv">
<button id="go">Importar</button>
<div id="res"></div>
</div>
<script>
const $=s=>document.querySelector(s);
$('#go').onclick=async()=>{
  const key=$('#key').value.trim(), proj=$('#proj').value, f=$('#file').files[0], res=$('#res');
  res.style.display='block'; res.className='';
  if(!key){res.className='err';res.textContent='Cole a chave do motor.';return;}
  if(!f){res.className='err';res.textContent='Escolha um arquivo CSV.';return;}
  $('#go').disabled=true; res.textContent='Importando... (pode levar um tempo pra arquivos grandes)';
  try{
    const csv=await f.text();
    const r=await fetch('/import/leads?projeto='+encodeURIComponent(proj),{method:'POST',headers:{'x-api-key':key,'Content-Type':'text/csv'},body:csv});
    const j=await r.json();
    if(r.ok&&j.ok){res.className='ok';res.textContent='✅ Pronto!\\n\\nImportados: '+j.importados+'\\nSem telefone (pulados): '+j.sem_telefone+'\\nTags aplicadas: '+j.tags_aplicadas+'\\n\\nColunas detectadas: '+JSON.stringify(j.colunas_detectadas);}
    else{res.className='err';res.textContent='Erro: '+(j.erro||r.status);}
  }catch(e){res.className='err';res.textContent='Falha: '+e.message;}
  $('#go').disabled=false;
};
</script></body></html>`);
});

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
app.get('/contacts/count', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const c = db.prepare(`SELECT COUNT(*) n FROM contatos WHERE projeto=?`).get(projeto) as any;
  res.json({ projeto, total: c?.n ?? 0 });
});

// ---- Importação em massa de leads (CSV exportado da SellFlux) ----
// POST /import/leads?projeto=teclado   corpo = CSV (Content-Type: text/csv)
// Detecta as colunas pelo cabeçalho (nome/email/telefone/tags), aceita , ou ; como separador.
function detectDelim(line: string): string {
  const c = (line.match(/,/g) || []).length;
  const s = (line.match(/;/g) || []).length;
  const t = (line.match(/\t/g) || []).length;
  if (t >= c && t >= s) return '\t';
  return s > c ? ';' : ',';
}
// Lê o campo de tags: aceita lista JSON ["a","b"] (formato da SellFlux) OU separado por ; , |
function parseTags(raw: string): string[] {
  const s = String(raw || '').trim();
  if (!s) return [];
  if (s.startsWith('[')) {
    try {
      const arr = JSON.parse(s);
      if (Array.isArray(arr)) return arr.map((t) => String(t).trim()).filter(Boolean);
    } catch { /* cai no fallback */ }
    return s.replace(/[\[\]"]/g, '').split(/[;,|]/).map((t) => t.trim()).filter(Boolean);
  }
  return s.split(/[;,|]/).map((t) => t.trim()).filter(Boolean);
}
function parseCSV(txt: string, delim: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [], cur = '', q = false;
  for (let i = 0; i < txt.length; i++) {
    const ch = txt[i];
    if (q) {
      if (ch === '"') { if (txt[i + 1] === '"') { cur += '"'; i++; } else q = false; }
      else cur += ch;
    } else {
      if (ch === '"') q = true;
      else if (ch === delim) { row.push(cur); cur = ''; }
      else if (ch === '\n') { row.push(cur); rows.push(row); row = []; cur = ''; }
      else if (ch === '\r') { /* ignora */ }
      else cur += ch;
    }
  }
  if (cur.length || row.length) { row.push(cur); rows.push(row); }
  return rows;
}
app.post('/import/leads', (req, res) => {
  try {
    const projeto = String(req.query.projeto || '');
    if (!ehProjeto(projeto)) return res.status(400).json({ erro: 'projeto_invalido' });
    const csv = typeof req.body === 'string' ? req.body : '';
    if (!csv.trim()) return res.status(400).json({ erro: 'csv_vazio' });
    const nl = csv.indexOf('\n');
    const delim = detectDelim(nl >= 0 ? csv.slice(0, nl) : csv);
    const rows = parseCSV(csv, delim);
    if (rows.length < 2) return res.status(400).json({ erro: 'sem_linhas' });
    const header = rows[0].map((h) => h.trim().toLowerCase().replace(/^﻿/, ''));
    const col = (names: string[]) => header.findIndex((h) => names.some((n) => h === n) ) ;
    const colLike = (names: string[]) => header.findIndex((h) => names.some((n) => h.includes(n)));
    const pick = (names: string[]) => { const e = col(names); return e >= 0 ? e : colLike(names); };
    const iNome = pick(['nome', 'name', 'lead', 'cliente', 'contato', 'contact']);
    const iEmail = pick(['email', 'e-mail', 'mail']);
    const iTel = pick(['telefone', 'phone', 'celular', 'whatsapp', 'numero', 'número', 'fone', 'mobile', 'telephone']);
    const iTags = pick(['tags', 'etiquetas', 'tag']);
    let importados = 0, semTelefone = 0, tagsAplicadas = 0;
    const tx = db.transaction(() => {
      for (let r = 1; r < rows.length; r++) {
        const row = rows[r];
        if (!row || row.length === 0 || (row.length === 1 && !row[0])) continue;
        const tel = (iTel >= 0 ? String(row[iTel] || '') : '').replace(/\D/g, '');
        if (!tel || tel.length < 10) { semTelefone++; continue; }
        const nome = iNome >= 0 ? String(row[iNome] || '').trim() : '';
        const email = iEmail >= 0 ? String(row[iEmail] || '').trim() : '';
        const cid = upsertContato(projeto, tel, nome || undefined, email || undefined, 'sellflux-import');
        importados++;
        if (iTags >= 0 && row[iTags]) {
          const tags = parseTags(String(row[iTags]));
          for (const t of tags) { aplicarTag(projeto, cid, t); tagsAplicadas++; }
        }
      }
    });
    tx();
    res.json({ ok: true, projeto, importados, sem_telefone: semTelefone, tags_aplicadas: tagsAplicadas,
      colunas_detectadas: { nome: iNome, email: iEmail, telefone: iTel, tags: iTags }, cabecalho: header });
  } catch (e: any) { res.status(500).json({ erro: String(e.message || e) }); }
});

// ======================= API SELLFLUX V4 (usabilidade) =======================
// Modulos novos da versao nova da SellFlux que ainda nao tinhamos. Tudo protegido
// por x-api-key (middleware acima). SO funcoes de USABILIDADE (sem a parte de IA deles).

// ---- Listas de leads ----
app.get('/crm/listas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT l.id, l.nome, l.criado_em, COUNT(cl.contato_id) n FROM listas l LEFT JOIN contato_listas cl ON cl.lista_id=l.id WHERE l.projeto=? GROUP BY l.id ORDER BY l.nome ASC`).all(projeto);
  res.json({ ok: true, listas: rows });
});
app.post('/crm/lista', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM listas WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE listas SET nome=? WHERE id=?`).run(String(b.nome || ''), id);
  else db.prepare(`INSERT INTO listas(id,projeto,nome,criado_em) VALUES(?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), agora());
  res.json({ ok: true, id });
});
app.post('/crm/lista/remover', (req, res) => {
  const id = String((req.body || {}).id || '');
  db.prepare(`DELETE FROM contato_listas WHERE lista_id=?`).run(id);
  db.prepare(`DELETE FROM listas WHERE id=?`).run(id);
  res.json({ ok: true });
});
app.post('/crm/lista/contatos', (req, res) => {
  const b = req.body || {};
  const listaId = String(b.listaId || '');
  if (!listaId) return res.status(400).json({ erro: 'lista_obrigatoria' });
  (Array.isArray(b.add) ? b.add : []).forEach((cid: string) => db.prepare(`INSERT OR IGNORE INTO contato_listas(contato_id,lista_id,criado_em) VALUES(?,?,?)`).run(String(cid), listaId, agora()));
  (Array.isArray(b.remove) ? b.remove : []).forEach((cid: string) => db.prepare(`DELETE FROM contato_listas WHERE contato_id=? AND lista_id=?`).run(String(cid), listaId));
  const n = db.prepare(`SELECT COUNT(*) n FROM contato_listas WHERE lista_id=?`).get(listaId) as any;
  res.json({ ok: true, total: (n && n.n) || 0 });
});
app.get('/crm/lista/:id/contatos', (req, res) => {
  const rows = db.prepare(`SELECT c.* FROM contatos c JOIN contato_listas cl ON cl.contato_id=c.id WHERE cl.lista_id=? ORDER BY c.nome ASC`).all(req.params.id);
  res.json({ ok: true, contatos: rows });
});

// ---- Segmentos (filtros salvos) ----
app.get('/crm/segmentos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = (db.prepare(`SELECT * FROM segmentos WHERE projeto=? ORDER BY nome ASC`).all(projeto) as any[]).map((s) => { try { s.filtro = JSON.parse(s.filtro || '{}'); } catch { s.filtro = {}; } return s; });
  res.json({ ok: true, segmentos: rows });
});
app.post('/crm/segmento', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const filtro = JSON.stringify(b.filtro || {});
  const existe = db.prepare(`SELECT id FROM segmentos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE segmentos SET nome=?, filtro=? WHERE id=?`).run(String(b.nome || ''), filtro, id);
  else db.prepare(`INSERT INTO segmentos(id,projeto,nome,filtro,criado_em) VALUES(?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), filtro, agora());
  res.json({ ok: true, id });
});
app.post('/crm/segmento/remover', (req, res) => { db.prepare(`DELETE FROM segmentos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Campos customizados ----
app.get('/crm/campos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, campos: db.prepare(`SELECT * FROM campos_customizados WHERE projeto=? ORDER BY rotulo ASC`).all(projeto) });
});
app.post('/crm/campo', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM campos_customizados WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE campos_customizados SET chave=?, rotulo=?, tipo=? WHERE id=?`).run(String(b.chave || ''), String(b.rotulo || ''), String(b.tipo || 'texto'), id);
  else db.prepare(`INSERT INTO campos_customizados(id,projeto,chave,rotulo,tipo,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.chave || ''), String(b.rotulo || ''), String(b.tipo || 'texto'), agora());
  res.json({ ok: true, id });
});
app.post('/crm/campo/remover', (req, res) => {
  const id = String((req.body || {}).id || '');
  db.prepare(`DELETE FROM contato_campos WHERE campo_id=?`).run(id);
  db.prepare(`DELETE FROM campos_customizados WHERE id=?`).run(id);
  res.json({ ok: true });
});
app.get('/crm/contato/:id/campos', (req, res) => {
  const rows = db.prepare(`SELECT cc.id campo_id, cc.chave, cc.rotulo, cc.tipo, v.valor FROM campos_customizados cc LEFT JOIN contato_campos v ON v.campo_id=cc.id AND v.contato_id=? WHERE cc.projeto=(SELECT projeto FROM contatos WHERE id=?) ORDER BY cc.rotulo ASC`).all(req.params.id, req.params.id);
  res.json({ ok: true, campos: rows });
});
app.post('/crm/contato/campos', (req, res) => {
  const b = req.body || {};
  const contatoId = String(b.contatoId || '');
  const valores = (b.valores && typeof b.valores === 'object') ? b.valores : {};
  for (const campoId of Object.keys(valores)) {
    const valor = String(valores[campoId] ?? '');
    const existe = db.prepare(`SELECT 1 FROM contato_campos WHERE contato_id=? AND campo_id=?`).get(contatoId, campoId);
    if (existe) db.prepare(`UPDATE contato_campos SET valor=? WHERE contato_id=? AND campo_id=?`).run(valor, contatoId, campoId);
    else db.prepare(`INSERT INTO contato_campos(contato_id,campo_id,valor) VALUES(?,?,?)`).run(contatoId, campoId, valor);
  }
  res.json({ ok: true });
});

// ---- Pipelines (varios funis) ----
app.get('/crm/pipelines', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const tipo = String(req.query.tipo || '');
  const cond: string[] = ['projeto=?']; const args: any[] = [projeto];
  if (tipo) { cond.push('tipo=?'); args.push(tipo); }
  const rows = (db.prepare('SELECT * FROM pipelines WHERE ' + cond.join(' AND ') + ' ORDER BY ordem ASC, nome ASC').all(...args) as any[]).map((p) => { try { p.etapas = JSON.parse(p.etapas || '[]'); } catch { p.etapas = []; } return p; });
  res.json({ ok: true, pipelines: rows });
});
app.post('/crm/pipeline', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const etapas = JSON.stringify(Array.isArray(b.etapas) ? b.etapas : []);
  const existe = db.prepare(`SELECT id FROM pipelines WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE pipelines SET nome=?, tipo=?, etapas=?, ordem=? WHERE id=?`).run(String(b.nome || ''), String(b.tipo || 'vendas'), etapas, Number(b.ordem || 0), id);
  else db.prepare(`INSERT INTO pipelines(id,projeto,nome,tipo,etapas,ordem,criado_em) VALUES(?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.tipo || 'vendas'), etapas, Number(b.ordem || 0), agora());
  res.json({ ok: true, id });
});
app.post('/crm/pipeline/remover', (req, res) => { db.prepare(`DELETE FROM pipelines WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Motivos de ganho/perda + marcar motivo no negocio ----
app.get('/crm/motivos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const tipo = String(req.query.tipo || '');
  const cond: string[] = ['projeto=?']; const args: any[] = [projeto];
  if (tipo) { cond.push('tipo=?'); args.push(tipo); }
  res.json({ ok: true, motivos: db.prepare('SELECT * FROM motivos WHERE ' + cond.join(' AND ') + ' ORDER BY nome ASC').all(...args) });
});
app.post('/crm/motivo', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM motivos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE motivos SET tipo=?, nome=? WHERE id=?`).run(String(b.tipo || 'ganho'), String(b.nome || ''), id);
  else db.prepare(`INSERT INTO motivos(id,projeto,tipo,nome,criado_em) VALUES(?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.tipo || 'ganho'), String(b.nome || ''), agora());
  res.json({ ok: true, id });
});
app.post('/crm/motivo/remover', (req, res) => { db.prepare(`DELETE FROM motivos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.post('/crm/negocio/pipeline', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE negocios SET pipeline_id=?, atualizado_em=? WHERE id=?`).run(String(b.pipelineId || ''), agora(), String(b.id || '')); res.json({ ok: true }); });
app.post('/crm/negocio/motivo', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE negocios SET motivo=?, motivo_tipo=?, atualizado_em=? WHERE id=?`).run(String(b.motivo || ''), String(b.motivoTipo || ''), agora(), String(b.id || '')); res.json({ ok: true }); });

// ---- Tickets (suporte) ----
app.get('/tickets', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const status = String(req.query.status || '');
  const cond: string[] = ['t.projeto=?']; const args: any[] = [projeto];
  if (status) { cond.push('t.status=?'); args.push(status); }
  const rows = db.prepare('SELECT t.*, c.nome contato_nome, c.telefone contato_telefone FROM tickets t LEFT JOIN contatos c ON c.id=t.contato_id WHERE ' + cond.join(' AND ') + ' ORDER BY t.atualizado_em DESC').all(...args);
  res.json({ ok: true, tickets: rows });
});
app.post('/ticket', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM tickets WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE tickets SET titulo=?, descricao=?, status=?, prioridade=?, contato_id=?, atualizado_em=? WHERE id=?`).run(String(b.titulo || ''), String(b.descricao || ''), String(b.status || 'aberto'), String(b.prioridade || 'media'), b.contatoId || null, agora(), id);
  else db.prepare(`INSERT INTO tickets(id,projeto,contato_id,titulo,descricao,status,prioridade,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), b.contatoId || null, String(b.titulo || ''), String(b.descricao || ''), String(b.status || 'aberto'), String(b.prioridade || 'media'), agora(), agora());
  res.json({ ok: true, id });
});
app.post('/ticket/status', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE tickets SET status=?, atualizado_em=? WHERE id=?`).run(String(b.status || 'aberto'), agora(), String(b.id || '')); res.json({ ok: true }); });
app.post('/ticket/remover', (req, res) => { db.prepare(`DELETE FROM tickets WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Tarefas (to-do) ----
app.get('/tarefas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const cond: string[] = ['projeto=?']; const args: any[] = [projeto];
  if (req.query.responsavel) { cond.push('responsavel=?'); args.push(String(req.query.responsavel)); }
  if (req.query.concluida !== undefined) { cond.push('concluida=?'); args.push(Number(req.query.concluida)); }
  res.json({ ok: true, tarefas: db.prepare('SELECT * FROM tarefas WHERE ' + cond.join(' AND ') + ' ORDER BY concluida ASC, prazo ASC').all(...args) });
});
app.post('/tarefa', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM tarefas WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE tarefas SET titulo=?, responsavel=?, prazo=?, contato_id=?, atualizado_em=? WHERE id=?`).run(String(b.titulo || ''), String(b.responsavel || ''), String(b.prazo || ''), b.contatoId || null, agora(), id);
  else db.prepare(`INSERT INTO tarefas(id,projeto,titulo,responsavel,prazo,concluida,contato_id,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.titulo || ''), String(b.responsavel || ''), String(b.prazo || ''), 0, b.contatoId || null, agora(), agora());
  res.json({ ok: true, id });
});
app.post('/tarefa/concluir', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE tarefas SET concluida=?, atualizado_em=? WHERE id=?`).run(b.concluida ? 1 : 0, agora(), String(b.id || '')); res.json({ ok: true }); });
app.post('/tarefa/remover', (req, res) => { db.prepare(`DELETE FROM tarefas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Produtos ----
app.get('/produtos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, produtos: db.prepare(`SELECT * FROM produtos WHERE projeto=? ORDER BY nome ASC`).all(projeto) });
});
app.post('/produto', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM produtos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE produtos SET nome=?, preco=?, descricao=? WHERE id=?`).run(String(b.nome || ''), Number(b.preco || 0), String(b.descricao || ''), id);
  else db.prepare(`INSERT INTO produtos(id,projeto,nome,preco,descricao,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), Number(b.preco || 0), String(b.descricao || ''), agora());
  res.json({ ok: true, id });
});
app.post('/produto/remover', (req, res) => { db.prepare(`DELETE FROM produtos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Departamentos ----
app.get('/departamentos', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, departamentos: db.prepare(`SELECT * FROM departamentos WHERE projeto=? ORDER BY nome ASC`).all(projeto) });
});
app.post('/departamento', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM departamentos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE departamentos SET nome=? WHERE id=?`).run(String(b.nome || ''), id);
  else db.prepare(`INSERT INTO departamentos(id,projeto,nome,criado_em) VALUES(?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), agora());
  res.json({ ok: true, id });
});
app.post('/departamento/remover', (req, res) => { db.prepare(`DELETE FROM departamentos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Roteiros de atendimento ----
app.get('/roteiros', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, roteiros: db.prepare(`SELECT * FROM roteiros WHERE projeto=? ORDER BY titulo ASC`).all(projeto) });
});
app.post('/roteiro', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM roteiros WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE roteiros SET titulo=?, texto=? WHERE id=?`).run(String(b.titulo || ''), String(b.texto || ''), id);
  else db.prepare(`INSERT INTO roteiros(id,projeto,titulo,texto,criado_em) VALUES(?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.titulo || ''), String(b.texto || ''), agora());
  res.json({ ok: true, id });
});
app.post('/roteiro/remover', (req, res) => { db.prepare(`DELETE FROM roteiros WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Avaliacoes (NPS / satisfacao) ----
app.get('/avaliacoes', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT a.*, c.nome contato_nome FROM avaliacoes a LEFT JOIN contatos c ON c.id=a.contato_id WHERE a.projeto=? ORDER BY a.criado_em DESC LIMIT 500`).all(projeto);
  const m = db.prepare(`SELECT AVG(nota) media, COUNT(*) n FROM avaliacoes WHERE projeto=?`).get(projeto) as any;
  res.json({ ok: true, avaliacoes: rows, media: (m && m.media) || 0, total: (m && m.n) || 0 });
});
app.post('/avaliacao', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  db.prepare(`INSERT INTO avaliacoes(id,projeto,contato_id,nota,comentario,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), b.contatoId || null, Number(b.nota || 0), String(b.comentario || ''), agora());
  res.json({ ok: true, id });
});
app.post('/avaliacao/remover', (req, res) => { db.prepare(`DELETE FROM avaliacoes WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Equipe (usuarios/permissoes) ----
app.get('/equipe', (_req, res) => { res.json({ ok: true, equipe: db.prepare(`SELECT * FROM equipe ORDER BY nome ASC`).all() }); });
app.post('/equipe/membro', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM equipe WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE equipe SET nome=?, email=?, papel=?, ativo=? WHERE id=?`).run(String(b.nome || ''), String(b.email || ''), String(b.papel || 'operador'), b.ativo === false ? 0 : 1, id);
  else db.prepare(`INSERT INTO equipe(id,nome,email,papel,ativo,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.nome || ''), String(b.email || ''), String(b.papel || 'operador'), b.ativo === false ? 0 : 1, agora());
  res.json({ ok: true, id });
});
app.post('/equipe/membro/remover', (req, res) => { db.prepare(`DELETE FROM equipe WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Transacoes ----
app.get('/transacoes', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT tr.*, c.nome contato_nome FROM transacoes tr LEFT JOIN contatos c ON c.id=tr.contato_id WHERE tr.projeto=? ORDER BY tr.criado_em DESC LIMIT 500`).all(projeto);
  const m = db.prepare(`SELECT SUM(valor) total, COUNT(*) n FROM transacoes WHERE projeto=? AND status='pago'`).get(projeto) as any;
  res.json({ ok: true, transacoes: rows, total_pago: (m && m.total) || 0, qtd_pago: (m && m.n) || 0 });
});
app.post('/transacao', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM transacoes WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE transacoes SET produto=?, valor=?, status=?, provider=?, contato_id=? WHERE id=?`).run(String(b.produto || ''), Number(b.valor || 0), String(b.status || 'pago'), String(b.provider || ''), b.contatoId || null, id);
  else db.prepare(`INSERT INTO transacoes(id,projeto,contato_id,produto,valor,status,provider,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), b.contatoId || null, String(b.produto || ''), Number(b.valor || 0), String(b.status || 'pago'), String(b.provider || ''), agora());
  res.json({ ok: true, id });
});
app.post('/transacao/remover', (req, res) => { db.prepare(`DELETE FROM transacoes WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- Relatorios / Dashboards (numeros reais dos dados do motor) ----
app.get('/relatorios/resumo', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const contatos = (db.prepare(`SELECT COUNT(*) n FROM contatos WHERE projeto=?`).get(projeto) as any)?.n || 0;
  const negociosPorEtapa = db.prepare(`SELECT etapa, COUNT(*) n, SUM(valor) valor FROM negocios WHERE projeto=? GROUP BY etapa`).all(projeto);
  const valorPipeline = (db.prepare(`SELECT SUM(valor) v FROM negocios WHERE projeto=?`).get(projeto) as any)?.v || 0;
  const filaCampanhas = db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE projeto=? GROUP BY status`).all(projeto);
  const ticketsAbertos = (db.prepare(`SELECT COUNT(*) n FROM tickets WHERE projeto=? AND status!='fechado'`).get(projeto) as any)?.n || 0;
  const tarefasPendentes = (db.prepare(`SELECT COUNT(*) n FROM tarefas WHERE projeto=? AND concluida=0`).get(projeto) as any)?.n || 0;
  const avaliacao = db.prepare(`SELECT AVG(nota) media, COUNT(*) n FROM avaliacoes WHERE projeto=?`).get(projeto) as any;
  const transacoes = db.prepare(`SELECT SUM(valor) total, COUNT(*) n FROM transacoes WHERE projeto=? AND status='pago'`).get(projeto) as any;
  res.json({ ok: true, resumo: {
    contatos, negociosPorEtapa, valorPipeline, filaCampanhas, ticketsAbertos, tarefasPendentes,
    avaliacaoMedia: (avaliacao && avaliacao.media) || 0, avaliacaoQtd: (avaliacao && avaliacao.n) || 0,
    transacoesTotal: (transacoes && transacoes.total) || 0, transacoesQtd: (transacoes && transacoes.n) || 0,
  } });
});
app.get('/relatorios/leads', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, porDia: db.prepare(`SELECT substr(criado_em,1,10) dia, COUNT(*) n FROM contatos WHERE projeto=? GROUP BY dia ORDER BY dia DESC LIMIT 60`).all(projeto) });
});
app.get('/relatorios/funil', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, etapas: db.prepare(`SELECT etapa, COUNT(*) n, SUM(valor) valor FROM negocios WHERE projeto=? GROUP BY etapa ORDER BY n DESC`).all(projeto) });
});
app.get('/relatorios/tags', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, tags: db.prepare(`SELECT t.nome, COUNT(ct.contato_id) n FROM tags t LEFT JOIN contato_tags ct ON ct.tag_id=t.id WHERE t.projeto=? GROUP BY t.id ORDER BY n DESC`).all(projeto) });
});

// ======================= AUTOMACOES (gatilho -> acoes), 24/7 no motor =======================
// Regra simples e poderosa: quando um GATILHO acontece (mensagem recebida com palavra,
// tag adicionada, pagamento aprovado/perdido), roda uma lista de ACOES (aplicar tag, enviar
// texto, mover etapa do negocio, criar tarefa, abrir ticket, enfileirar fluxo do projeto).
// Nao depende de nada externo: usa os proprios numeros/fluxos ja configurados.
async function rodarAutomacoes(projeto: string, gatilho: string, ctx: any) {
  const autos = db.prepare(`SELECT * FROM automacoes WHERE projeto=? AND gatilho=? AND ativo=1`).all(projeto, gatilho) as any[];
  for (const a of autos) {
    try {
      const cond = String(a.condicao || '').trim().toLowerCase();
      if (cond) {
        if (gatilho === 'mensagem_recebida') { if (!String(ctx.texto || '').toLowerCase().includes(cond)) continue; }
        else if (gatilho === 'tag_adicionada') { if (String(ctx.tag || '').toLowerCase() !== cond) continue; }
      }
      let acoes: any[] = [];
      try { acoes = JSON.parse(a.acoes || '[]'); } catch { acoes = []; }
      for (const ac of acoes) {
        const tipo = String(ac?.tipo || '');
        if (tipo === 'aplicar_tag' && ctx.contatoId && ac.tag) {
          aplicarTag(projeto, ctx.contatoId, String(ac.tag));
        } else if (tipo === 'enviar_texto' && ctx.telefone && ac.texto) {
          await enviarTexto(projeto as Projeto, ctx.telefone, preenche(String(ac.texto), ctx.nome)).catch(() => {});
        } else if (tipo === 'enviar_texto_delay' && ctx.telefone && ac.texto) {
          // Etapa 9: envio com ATRASO — enfileira respeitando o ritmo/anti-ban do worker (agendado_para).
          const seg = Math.max(0, Number(ac.delaySegundos || 0));
          db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,agendado_para,criado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?)`)
            .run(randomUUID(), projeto, ctx.telefone, 0, preenche(String(ac.texto), ctx.nome), 'texto', null, null, 'pendente', emSegundos(seg), agora());
        } else if (tipo === 'mover_etapa' && ctx.contatoId && ac.etapa) {
          db.prepare(`UPDATE negocios SET etapa=?, atualizado_em=? WHERE contato_id=? AND projeto=?`).run(String(ac.etapa), agora(), ctx.contatoId, projeto);
        } else if (tipo === 'criar_tarefa' && ac.titulo) {
          db.prepare(`INSERT INTO tarefas(id,projeto,titulo,responsavel,prazo,concluida,contato_id,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`).run(randomUUID(), projeto, String(ac.titulo), String(ac.responsavel || ''), '', 0, ctx.contatoId || null, agora(), agora());
        } else if (tipo === 'abrir_ticket' && ac.titulo) {
          db.prepare(`INSERT INTO tickets(id,projeto,contato_id,titulo,descricao,status,prioridade,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`).run(randomUUID(), projeto, ctx.contatoId || null, String(ac.titulo), String(ac.descricao || ''), 'aberto', String(ac.prioridade || 'media'), agora(), agora());
        } else if (tipo === 'enfileirar_fluxo' && ctx.telefone) {
          const f = (ac.tipoFluxo === 'recuperacao' ? CONFIG.recuperacaoFluxo : CONFIG.entregaFluxo)?.[projeto as Projeto];
          if (Array.isArray(f) && f.length) enfileirarFluxo(projeto as Projeto, ctx.telefone, f, ctx.nome);
        }
      }
      db.prepare(`INSERT INTO automacao_logs(id,automacao_id,projeto,contato_id,gatilho,detalhe,criado_em) VALUES(?,?,?,?,?,?,?)`).run(randomUUID(), a.id, projeto, ctx.contatoId || null, gatilho, 'ok: ' + acoes.length + ' acao(oes)', agora());
    } catch (e: any) {
      try { db.prepare(`INSERT INTO automacao_logs(id,automacao_id,projeto,contato_id,gatilho,detalhe,criado_em) VALUES(?,?,?,?,?,?,?)`).run(randomUUID(), a.id, projeto, ctx.contatoId || null, gatilho, 'erro: ' + String(e?.message || e), agora()); } catch { /* log best-effort */ }
    }
  }
}
app.get('/automacoes', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = (db.prepare(`SELECT * FROM automacoes WHERE projeto=? ORDER BY criado_em DESC`).all(projeto) as any[]).map((a) => { try { a.acoes = JSON.parse(a.acoes || '[]'); } catch { a.acoes = []; } return a; });
  res.json({ ok: true, automacoes: rows });
});
app.post('/automacao', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const acoes = JSON.stringify(Array.isArray(b.acoes) ? b.acoes : []);
  const existe = db.prepare(`SELECT id FROM automacoes WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE automacoes SET nome=?, ativo=?, gatilho=?, condicao=?, acoes=? WHERE id=?`).run(String(b.nome || ''), b.ativo === false ? 0 : 1, String(b.gatilho || ''), String(b.condicao || ''), acoes, id);
  else db.prepare(`INSERT INTO automacoes(id,projeto,nome,ativo,gatilho,condicao,acoes,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), b.ativo === false ? 0 : 1, String(b.gatilho || ''), String(b.condicao || ''), acoes, agora());
  res.json({ ok: true, id });
});
app.post('/automacao/ativar', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE automacoes SET ativo=? WHERE id=?`).run(b.ativo ? 1 : 0, String(b.id || '')); res.json({ ok: true }); });
app.post('/automacao/remover', (req, res) => { db.prepare(`DELETE FROM automacoes WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/automacoes/logs', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, logs: db.prepare(`SELECT l.*, a.nome automacao_nome FROM automacao_logs l LEFT JOIN automacoes a ON a.id=l.automacao_id WHERE l.projeto=? ORDER BY l.criado_em DESC LIMIT 200`).all(projeto) });
});
app.post('/automacao/testar', async (req, res) => { const b = req.body || {}; await rodarAutomacoes(String(b.projeto || ''), String(b.gatilho || 'mensagem_recebida'), b.ctx || {}); res.json({ ok: true }); });

// ---- Tokens de API (registro das chaves que sistemas externos usam pra chamar o motor) ----
app.get('/api-tokens', (_req, res) => { res.json({ ok: true, tokens: db.prepare(`SELECT id,nome,token,criado_em FROM api_tokens ORDER BY criado_em DESC`).all() }); });
app.post('/api-token', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const token = String(b.token || ('mr_' + randomUUID().replace(/-/g, '')));
  db.prepare(`INSERT INTO api_tokens(id,nome,token,criado_em) VALUES(?,?,?,?)`).run(id, String(b.nome || ''), token, agora());
  res.json({ ok: true, id, token });
});
app.post('/api-token/remover', (req, res) => { db.prepare(`DELETE FROM api_tokens WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ======================= MODULOS QUE DEPENDEM DE CONEXAO (codigo pronto) =======================
// Paginas, Formularios/Quiz, Pixels, E-mail (templates/dominios/fila), Comentarios IG/FB,
// Templates WhatsApp oficial, Integracoes (webhooks de saida, API Receive, Apps).
// O que roda SO no motor ja funciona (paginas /p/:slug e formularios /f/:id sao servidos aqui).
// O que precisa de credencial externa (SMTP, Meta, WhatsApp oficial) fica ENFILEIRADO/registrado
// e e enviado de verdade quando a conexao for ligada no fim.

function esc(s: any): string {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' } as any)[c]);
}

// ---- PAGINAS (landing pages servidas pelo proprio motor) ----
app.get('/paginas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, paginas: db.prepare(`SELECT id,projeto,slug,titulo,publicada,criado_em,atualizado_em FROM paginas WHERE projeto=? ORDER BY atualizado_em DESC`).all(projeto) });
});
app.get('/pagina/:id', (req, res) => {
  const p = db.prepare(`SELECT * FROM paginas WHERE id=?`).get(req.params.id);
  if (!p) return res.status(404).json({ erro: 'nao_encontrada' });
  res.json({ ok: true, pagina: p });
});
app.post('/pagina', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const slug = String(b.slug || '').toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '') || id.slice(0, 8);
  const existe = db.prepare(`SELECT id FROM paginas WHERE id=?`).get(id);
  try {
    if (existe) db.prepare(`UPDATE paginas SET slug=?, titulo=?, html=?, publicada=?, atualizado_em=? WHERE id=?`).run(slug, String(b.titulo || ''), String(b.html || ''), b.publicada ? 1 : 0, agora(), id);
    else db.prepare(`INSERT INTO paginas(id,projeto,slug,titulo,html,publicada,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), slug, String(b.titulo || ''), String(b.html || ''), b.publicada ? 1 : 0, agora(), agora());
  } catch (e: any) { return res.status(400).json({ erro: 'slug_duplicado_ou_invalido', detalhe: String(e?.message || e) }); }
  res.json({ ok: true, id, slug, url: (PUBLIC_BASE_URL || '') + '/p/' + slug });
});
app.post('/pagina/remover', (req, res) => { db.prepare(`DELETE FROM paginas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
// publico: serve a pagina publicada
app.get('/p/:slug', (req, res) => {
  const p = db.prepare(`SELECT * FROM paginas WHERE slug=? AND publicada=1`).get(req.params.slug) as any;
  if (!p) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Pagina nao encontrada</h1>');
  res.type('html').send(String(p.html || ''));
});

// ---- FORMULARIOS / QUIZ (servidos pelo motor; resposta salva + cria contato + tag) ----
app.get('/formularios', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = (db.prepare(`SELECT * FROM formularios WHERE projeto=? ORDER BY criado_em DESC`).all(projeto) as any[]).map((f) => { try { f.campos = JSON.parse(f.campos || '[]'); } catch { f.campos = []; } return f; });
  res.json({ ok: true, formularios: rows });
});
app.post('/formulario', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const campos = JSON.stringify(Array.isArray(b.campos) ? b.campos : []);
  const existe = db.prepare(`SELECT id FROM formularios WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE formularios SET titulo=?, campos=?, tag=?, redirecionar=? WHERE id=?`).run(String(b.titulo || ''), campos, String(b.tag || ''), String(b.redirecionar || ''), id);
  else db.prepare(`INSERT INTO formularios(id,projeto,titulo,campos,tag,redirecionar,criado_em) VALUES(?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.titulo || ''), campos, String(b.tag || ''), String(b.redirecionar || ''), agora());
  res.json({ ok: true, id, url: (PUBLIC_BASE_URL || '') + '/f/' + id });
});
app.post('/formulario/remover', (req, res) => { db.prepare(`DELETE FROM formularios WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/formularios/:id/respostas', (req, res) => {
  const rows = (db.prepare(`SELECT * FROM formulario_respostas WHERE formulario_id=? ORDER BY criado_em DESC LIMIT 500`).all(req.params.id) as any[]).map((r) => { try { r.dados = JSON.parse(r.dados || '{}'); } catch { r.dados = {}; } return r; });
  res.json({ ok: true, respostas: rows });
});
// publico: renderiza o formulario
app.get('/f/:id', (req, res) => {
  const f = db.prepare(`SELECT * FROM formularios WHERE id=?`).get(req.params.id) as any;
  if (!f) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Formulario nao encontrado</h1>');
  let campos: any[] = [];
  try { campos = JSON.parse(f.campos || '[]'); } catch { campos = []; }
  const inputs = campos.map((c: any) => {
    const req2 = c.obrigatorio ? 'required' : '';
    const nome = esc(c.chave || c.rotulo);
    if (c.tipo === 'textarea') return '<label>' + esc(c.rotulo) + '</label><textarea name="' + nome + '" ' + req2 + '></textarea>';
    const tipo = c.tipo === 'email' ? 'email' : c.tipo === 'telefone' ? 'tel' : c.tipo === 'numero' ? 'number' : 'text';
    return '<label>' + esc(c.rotulo) + '</label><input type="' + tipo + '" name="' + nome + '" ' + req2 + '>';
  }).join('');
  res.type('html').send('<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(f.titulo) + '</title>'
    + '<style>body{margin:0;font:16px/1.5 system-ui,Segoe UI,Arial;background:#0e1116;color:#e6e6e6;display:flex;justify-content:center;padding:28px 16px}.card{width:100%;max-width:520px;background:#161b22;border:1px solid #2a313c;border-radius:14px;padding:24px}h1{font-size:22px;margin:0 0 16px}label{display:block;margin:12px 0 5px;font-weight:600}input,textarea{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:9px;border:1px solid #2a313c;background:#0e1116;color:#e6e6e6;font-size:15px}button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:9px;background:#1db954;color:#07210f;font-weight:700;font-size:16px;cursor:pointer}#ok{display:none;margin-top:16px;padding:12px;border-radius:9px;background:#0f2e1b;border:1px solid #1db95455}</style></head><body><div class="card">'
    + '<h1>' + esc(f.titulo) + '</h1><form id="frm">' + inputs + '<button type="submit">Enviar</button></form><div id="ok">✅ Enviado! Obrigado.</div></div>'
    + '<script>document.getElementById("frm").onsubmit=async(e)=>{e.preventDefault();const d={};new FormData(e.target).forEach((v,k)=>d[k]=v);const r=await fetch(location.pathname,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(d)});if(r.ok){e.target.style.display="none";document.getElementById("ok").style.display="block";' + (f.redirecionar ? 'setTimeout(()=>location.href=' + JSON.stringify(String(f.redirecionar)) + ',1200);' : '') + '}};</script></body></html>');
});
// publico: recebe a resposta
app.post('/f/:id', (req, res) => {
  const f = db.prepare(`SELECT * FROM formularios WHERE id=?`).get(req.params.id) as any;
  if (!f) return res.status(404).json({ erro: 'nao_encontrado' });
  const dados = (req.body && typeof req.body === 'object') ? req.body : {};
  let contatoId: string | null = null;
  const tel = String(dados.telefone || dados.phone || dados.celular || dados.whatsapp || '').replace(/\D/g, '');
  const nome = dados.nome || dados.name;
  const email = dados.email;
  if (tel && tel.length >= 10) {
    contatoId = upsertContato(f.projeto, tel, nome, email, 'formulario');
    if (f.tag) aplicarTag(f.projeto, contatoId, String(f.tag));
    if (contatoId) rodarAutomacoes(f.projeto, 'tag_adicionada', { contatoId, tag: String(f.tag || ''), telefone: tel, nome }).catch(() => {});
  }
  db.prepare(`INSERT INTO formulario_respostas(id,formulario_id,projeto,contato_id,dados,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), f.id, f.projeto, contatoId, JSON.stringify(dados), agora());
  res.json({ ok: true });
});

// ---- PIXELS (registro de config pra injetar nas paginas/rastreamento) ----
app.get('/pixels', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, pixels: db.prepare(`SELECT * FROM pixels WHERE projeto=? ORDER BY nome ASC`).all(projeto) }); });
app.post('/pixel', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM pixels WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE pixels SET nome=?, plataforma=?, pixel_id=? WHERE id=?`).run(String(b.nome || ''), String(b.plataforma || 'meta'), String(b.pixel_id || ''), id);
  else db.prepare(`INSERT INTO pixels(id,projeto,nome,plataforma,pixel_id,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.plataforma || 'meta'), String(b.pixel_id || ''), agora());
  res.json({ ok: true, id });
});
app.post('/pixel/remover', (req, res) => { db.prepare(`DELETE FROM pixels WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- E-MAIL: templates, dominios (SMTP) e fila (envio real liga no fim) ----
app.get('/email/templates', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, templates: db.prepare(`SELECT * FROM email_templates WHERE projeto=? ORDER BY nome ASC`).all(projeto) }); });
app.post('/email/template', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM email_templates WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE email_templates SET nome=?, assunto=?, html=? WHERE id=?`).run(String(b.nome || ''), String(b.assunto || ''), String(b.html || ''), id);
  else db.prepare(`INSERT INTO email_templates(id,projeto,nome,assunto,html,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.assunto || ''), String(b.html || ''), agora());
  res.json({ ok: true, id });
});
app.post('/email/template/remover', (req, res) => { db.prepare(`DELETE FROM email_templates WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/email/dominios', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, dominios: db.prepare(`SELECT * FROM email_dominios WHERE projeto=? ORDER BY dominio ASC`).all(projeto) }); });
app.post('/email/dominio', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM email_dominios WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE email_dominios SET dominio=?, smtp_host=?, smtp_porta=?, smtp_usuario=?, remetente=? WHERE id=?`).run(String(b.dominio || ''), String(b.smtp_host || ''), Number(b.smtp_porta || 587), String(b.smtp_usuario || ''), String(b.remetente || ''), id);
  else db.prepare(`INSERT INTO email_dominios(id,projeto,dominio,smtp_host,smtp_porta,smtp_usuario,remetente,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.dominio || ''), String(b.smtp_host || ''), Number(b.smtp_porta || 587), String(b.smtp_usuario || ''), String(b.remetente || ''), agora());
  res.json({ ok: true, id });
});
app.post('/email/dominio/remover', (req, res) => { db.prepare(`DELETE FROM email_dominios WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/email/fila', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, fila: db.prepare(`SELECT * FROM email_fila WHERE projeto=? ORDER BY criado_em DESC LIMIT 300`).all(projeto) }); });
app.post('/email/enviar', (req, res) => {
  const b = req.body || {};
  let assunto = String(b.assunto || ''); let html = String(b.html || '');
  if (b.templateId) { const t = db.prepare(`SELECT * FROM email_templates WHERE id=?`).get(String(b.templateId)) as any; if (t) { assunto = assunto || t.assunto; html = html || t.html; } }
  db.prepare(`INSERT INTO email_fila(id,projeto,para,assunto,html,status,criado_em) VALUES(?,?,?,?,?,?,?)`).run(randomUUID(), String(b.projeto || ''), String(b.para || ''), assunto, html, 'pendente', agora());
  res.json({ ok: true, enfileirado: true, obs: 'Envio real ocorre quando o SMTP do dominio estiver ligado.' });
});

// ---- COMENTARIOS IG/FB (inbox; responder de verdade liga no fim via Meta) ----
app.get('/comentarios', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, comentarios: db.prepare(`SELECT * FROM comentarios WHERE projeto=? ORDER BY criado_em DESC LIMIT 300`).all(projeto) }); });
app.post('/comentario', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  db.prepare(`INSERT INTO comentarios(id,projeto,rede,post_id,autor,texto,respondido,resposta,criado_em) VALUES(?,?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.rede || 'instagram'), String(b.post_id || ''), String(b.autor || ''), String(b.texto || ''), 0, '', agora());
  res.json({ ok: true, id });
});
app.post('/comentario/responder', (req, res) => { const b = req.body || {}; db.prepare(`UPDATE comentarios SET respondido=1, resposta=? WHERE id=?`).run(String(b.resposta || ''), String(b.id || '')); res.json({ ok: true, obs: 'Resposta salva. A publicacao no IG/FB ocorre quando a API da Meta estiver ligada.' }); });
app.post('/comentario/remover', (req, res) => { db.prepare(`DELETE FROM comentarios WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- TEMPLATES WHATSAPP OFICIAL (registro; submissao a Meta liga no fim) ----
app.get('/wpp/templates', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, templates: db.prepare(`SELECT * FROM templates_whatsapp WHERE projeto=? ORDER BY nome ASC`).all(projeto) }); });
app.post('/wpp/template', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM templates_whatsapp WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE templates_whatsapp SET nome=?, idioma=?, categoria=?, corpo=?, status=? WHERE id=?`).run(String(b.nome || ''), String(b.idioma || 'pt_BR'), String(b.categoria || 'UTILITY'), String(b.corpo || ''), String(b.status || 'rascunho'), id);
  else db.prepare(`INSERT INTO templates_whatsapp(id,projeto,nome,idioma,categoria,corpo,status,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.idioma || 'pt_BR'), String(b.categoria || 'UTILITY'), String(b.corpo || ''), String(b.status || 'rascunho'), agora());
  res.json({ ok: true, id });
});
app.post('/wpp/template/remover', (req, res) => { db.prepare(`DELETE FROM templates_whatsapp WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ---- INTEGRACOES: webhooks de saida, API Receive, Apps ----
async function dispararWebhooksSaida(projeto: string, evento: string, payload: any) {
  const hs = db.prepare(`SELECT * FROM webhooks_saida WHERE projeto=? AND evento=? AND ativo=1`).all(projeto, evento) as any[];
  for (const h of hs) {
    try { await fetch(String(h.url), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ evento, projeto, payload }) }); } catch { /* best-effort */ }
  }
}
app.get('/webhooks', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, webhooks: db.prepare(`SELECT * FROM webhooks_saida WHERE projeto=? ORDER BY criado_em DESC`).all(projeto) }); });
app.post('/webhook-saida', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM webhooks_saida WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE webhooks_saida SET nome=?, url=?, evento=?, ativo=? WHERE id=?`).run(String(b.nome || ''), String(b.url || ''), String(b.evento || ''), b.ativo === false ? 0 : 1, id);
  else db.prepare(`INSERT INTO webhooks_saida(id,projeto,nome,url,evento,ativo,criado_em) VALUES(?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.url || ''), String(b.evento || ''), b.ativo === false ? 0 : 1, agora());
  res.json({ ok: true, id });
});
app.post('/webhook-saida/remover', (req, res) => { db.prepare(`DELETE FROM webhooks_saida WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/integracao/eventos', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, eventos: db.prepare(`SELECT * FROM integracao_eventos WHERE projeto=? ORDER BY criado_em DESC LIMIT 200`).all(projeto) }); });
app.get('/apps', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, apps: db.prepare(`SELECT * FROM apps_externos WHERE projeto=? ORDER BY nome ASC`).all(projeto) }); });
app.post('/app', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM apps_externos WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE apps_externos SET nome=?, base_url=?, descricao=? WHERE id=?`).run(String(b.nome || ''), String(b.base_url || ''), String(b.descricao || ''), id);
  else db.prepare(`INSERT INTO apps_externos(id,projeto,nome,base_url,descricao,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.base_url || ''), String(b.descricao || ''), agora());
  res.json({ ok: true, id });
});
app.post('/app/remover', (req, res) => { db.prepare(`DELETE FROM apps_externos WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
// publico: API Receive — recebe eventos externos validados por token na URL (gere o token em "Tokens API")
app.all('/receive/:token', (req, res) => {
  const token = String(req.params.token || '');
  const t = db.prepare(`SELECT id FROM api_tokens WHERE token=?`).get(token) as any;
  if (!t) return res.status(401).json({ erro: 'token_invalido' });
  const q = req.query as any;
  const b = (req.body && typeof req.body === 'object') ? req.body : {};
  const projeto = String(b.projeto || q.projeto || CONFIG.projetoPadrao);
  const tipo = String(b.tipo || b.evento || q.tipo || 'evento');
  db.prepare(`INSERT INTO integracao_eventos(id,projeto,origem,tipo,dados,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), projeto, 'api-receive', tipo, JSON.stringify({ query: q, body: b }), agora());
  // se vier telefone, cria/atualiza contato e dispara automacao de evento recebido via tag opcional
  const tel = String(b.telefone || b.phone || q.telefone || '').replace(/\D/g, '');
  if (tel && tel.length >= 10) {
    const cid = upsertContato(projeto, tel, b.nome || q.nome, b.email || q.email, 'api-receive');
    if (b.tag || q.tag) { aplicarTag(projeto, cid, String(b.tag || q.tag)); rodarAutomacoes(projeto, 'tag_adicionada', { contatoId: cid, tag: String(b.tag || q.tag), telefone: tel, nome: b.nome || q.nome }).catch(() => {}); }
  }
  dispararWebhooksSaida(projeto, 'evento_recebido', { tipo, query: q, body: b }).catch(() => {});
  res.json({ ok: true, recebido: true });
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
        try { db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,instancia,criado_em) VALUES(?,?,?,?,?,?,?)`).run(randomUUID(), projeto, cid, 'entrada', texto, String(ev.instance||''), agora()); }
        catch { db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), projeto, cid, 'entrada', texto, agora()); }
        // Automacoes: gatilho "mensagem_recebida" (palavra-chave opcional na condicao).
        rodarAutomacoes(projeto, 'mensagem_recebida', { contatoId: cid, telefone, nome: data.pushName, texto }).catch(() => {});
        try { if (((CONFIG as any).agentesHabilitados || {})[projeto]) processarMensagemAgente(projeto, telefone, texto, data.pushName, cid).catch(() => {}); } catch { /* agente IA best-effort, etapa14 */ }
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
  pedido: string; status: string; telefone: string; nome?: string; email?: string; produtoId?: string; produtoNome?: string; valor?: number;
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
      valor: Number(prod?.Price || prod?.price || d?.Purchase?.Price || d?.Purchase?.Value || d?.Purchase?.Amount || d?.Offer?.Price || b?.Value || 0),
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
    valor: Number(b?.payment?.total || b?.total_value || b?.value || b?.amount || prod?.total_value || prod?.unit_value || b?.payment?.marketplace_value || 0),
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
    // Faturamento: registra/atualiza a venda (upsert por pedido) — NAO depende do dedupe de entrega.
    try {
      const _projT = projetoDoProduto(p.produtoId, p.produtoNome);
      const _catT = classificaPagamento(provider, p.status);
      const _stF = statusFaturamento(p.status, _catT);
      const _cidT = p.telefone ? upsertContato(_projT, p.telefone, p.nome, p.email, `pagamento:${provider}`) : null;
      const _exT = db.prepare(`SELECT id FROM transacoes WHERE id=?`).get(p.pedido);
      if (_exT) db.prepare(`UPDATE transacoes SET valor=?, status=?, provider=?, produto=?, contato_id=COALESCE(contato_id,?) WHERE id=?`).run(Number(p.valor || 0), _stF, provider, p.produtoNome || '', _cidT, p.pedido);
      else db.prepare(`INSERT INTO transacoes(id,projeto,contato_id,produto,valor,status,provider,criado_em) VALUES(?,?,?,?,?,?,?,?)`).run(p.pedido, _projT, _cidT, p.produtoNome || '', Number(p.valor || 0), _stF, provider, agora());
    } catch { /* faturamento best-effort */ }

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
        await dispararEntrega(projeto, p.telefone, p.nome, p.produtoId, p.produtoNome).catch(() => {});
        await rodarAutomacoes(projeto, 'pagamento_aprovado', { contatoId: cid, telefone: p.telefone, nome: p.nome }).catch(() => {});
      } else if (categoria === 'perdido') {
        aplicarTag(projeto, cid, 'recuperacao');
        await dispararRecuperacao(projeto, p.telefone, p.nome).catch(() => {});
        await rodarAutomacoes(projeto, 'pagamento_perdido', { contatoId: cid, telefone: p.telefone, nome: p.nome }).catch(() => {});
      } else if (categoria === 'reembolso') {
        aplicarTag(projeto, cid, 'reembolso'); // sem mensagem automática
      }
    }
    res.json({ ok: true, projeto, status: p.status, categoria });
  } catch (e: any) { res.status(200).json({ ok: false, erro: String(e.message || e) }); }
});

// ---- Webhook da CADEMÍ (OPCIONAL) — entrega por "acesso liberado" (aluno criado / entrega adicionada) ----
// URL: /webhook/cademi?token=XXXX
// Hoje as vendas só passam por Guru/LastLink, então ISTO NÃO É NECESSÁRIO. Fica pronto pro caso
// de venda pelo checkout da própria Cademí. Aceita os 2 formatos da Cademí:
//   - LEGADO (query-params, igual o da SellFlux): ?nome=..&email=..&phone=..&produto=..&pedido=..
//   - v3 (JSON no corpo): { event_type, usuario:{nome,email,celular}, produto:{nome,id}, id }
// Só dispara entrega em eventos de ACESSO/CRIAÇÃO (ignora progresso/prova/certificado/etc).
function normalizaCademi(req: express.Request): {
  pedido: string; evento: string; telefone: string; nome?: string; email?: string; produtoId?: string; produtoNome?: string;
} {
  const q = req.query as any;
  const b = (req.body && typeof req.body === 'object') ? req.body as any : {};
  const u = b.usuario || b.user || b.aluno || {};
  const prod = b.produto || b.product || {};
  const nome = q.nome || q.name || u.nome || u.name;
  const email = q.email || u.email;
  const telefoneRaw = q.phone || q.telefone || q.celular || u.celular || u.telefone || u.phone || '';
  const evento = String(q.evento || q.event || b.event_type || b.evento || 'acesso').toLowerCase();
  return {
    pedido: String(q.pedido || q.id || b.id || b.event_id || b.pedido || randomUUID()),
    evento,
    telefone: String(telefoneRaw).replace(/\D/g, ''),
    nome, email,
    produtoId: String(q.produto_id || prod.id || ''),
    produtoNome: q.produto || q.product || prod.nome || prod.name,
  };
}
app.all('/webhook/cademi', async (req, res) => {
  try {
    if (!CADEMI_TOKEN || String(req.query.token || '') !== CADEMI_TOKEN) {
      return res.status(401).json({ erro: 'token_invalido' });
    }
    const c = normalizaCademi(req);
    // Só nos interessa "acesso liberado / aluno criado / entrega adicionada".
    // Primeiro IGNORA os eventos que não são de acesso (progresso/prova/certificado/termo/ponto),
    // depois aceita os de acesso/criação. (Assim "usuario.progresso" não vira entrega.)
    const ignorar = /progress|prova|certific|termo|ponto|assinad|exam|login|acesso_realizado/.test(c.evento);
    const ehEntrega = !ignorar && /acesso|aluno|usuario|usuário|criad|entrega|matricul|member|enroll|created/.test(c.evento);
    if (!ehEntrega) return res.json({ ok: true, ignorado: c.evento });
    // dedupe
    const ja = db.prepare(`SELECT 1 FROM pagamentos_processados WHERE provider=? AND pedido=?`).get('cademi', c.pedido);
    if (ja) return res.json({ ok: true, dedupe: true });
    db.prepare(`INSERT INTO pagamentos_processados(provider,pedido,criado_em) VALUES(?,?,?)`).run('cademi', c.pedido, agora());
    const projeto = projetoDoProduto(c.produtoId, c.produtoNome);
    if (c.telefone) {
      const cid = upsertContato(projeto, c.telefone, c.nome, c.email, 'cademi');
      aplicarTag(projeto, cid, 'comprou');
      await dispararEntrega(projeto, c.telefone, c.nome, c.produtoId, c.produtoNome).catch(() => {});
    }
    res.json({ ok: true, projeto, evento: c.evento });
  } catch (e: any) { res.status(200).json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= ETAPA 9: construtor de Paginas (blocos) + Quiz multi-passos =======================
// Paginas: lista completa (com pasta/idioma) e gravacao dos blocos do construtor visual.
// A pagina publica continua servida em /p/:slug (HTML gerado no app a partir dos blocos).
app.get('/paginas/full', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, paginas: db.prepare(`SELECT id,projeto,slug,titulo,pasta,idioma,publicada,criado_em,atualizado_em FROM paginas WHERE projeto=? ORDER BY atualizado_em DESC`).all(projeto) });
});
app.post('/pagina/blocos', (req, res) => {
  const b = req.body || {};
  db.prepare(`UPDATE paginas SET blocos=?, pasta=?, idioma=?, atualizado_em=? WHERE id=?`).run(String(b.blocos || '[]'), String(b.pasta || ''), String(b.idioma || 'pt'), agora(), String(b.id || ''));
  res.json({ ok: true });
});

// Quiz: grava os passos + tema do construtor multi-passos (o formulario base ja existe via /formulario).
app.post('/formulario/passos', (req, res) => {
  const b = req.body || {};
  db.prepare(`UPDATE formularios SET passos=?, tema=? WHERE id=?`).run(String(b.passos || '[]'), String(b.tema || '{}'), String(b.id || ''));
  res.json({ ok: true, url: (PUBLIC_BASE_URL || '') + '/q/' + String(b.id || '') });
});

// publico: renderiza o QUIZ multi-passos (uma tela por passo). Envia pro /f/:id (cria contato + tag + resposta).
app.get('/q/:id', (req, res) => {
  const f = db.prepare(`SELECT * FROM formularios WHERE id=?`).get(req.params.id) as any;
  if (!f) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Quiz nao encontrado</h1>');
  try { db.prepare(`UPDATE formularios SET acessos=COALESCE(acessos,0)+1 WHERE id=?`).run(f.id); } catch { /* ok */ }
  let passos: any[] = []; try { passos = JSON.parse(f.passos || '[]'); } catch { passos = []; }
  let tema: any = {}; try { tema = JSON.parse(f.tema || '{}'); } catch { tema = {}; }
  const fundo = esc(tema.fundo || '#0e1116'), cor = esc(tema.texto || '#e6e6e6'), botao = esc(tema.botao || '#1db954');
  const passosJson = JSON.stringify(passos).replace(/</g, '\\u003c');
  const redir = f.redirecionar ? JSON.stringify(String(f.redirecionar)) : 'null';
  res.type('html').send('<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(f.titulo) + '</title>'
    + '<style>*{box-sizing:border-box}body{margin:0;font-family:system-ui,Segoe UI,Arial;background:' + fundo + ';color:' + cor + ';min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}'
    + '.card{width:100%;max-width:560px}h1{font-size:26px;margin:0 0 10px}p.sub{opacity:.8;margin:0 0 18px}'
    + 'input,textarea{width:100%;padding:13px;border-radius:10px;border:1px solid #ffffff22;background:#ffffff0d;color:inherit;font-size:16px;margin-bottom:10px}'
    + 'button,.opt{cursor:pointer;border:0;border-radius:10px;font-size:16px}.btn{background:' + botao + ';color:#07210f;font-weight:700;padding:14px 22px;width:100%}'
    + '.opt{display:block;width:100%;text-align:left;background:#ffffff14;color:inherit;border:1px solid #ffffff22;padding:13px;margin-bottom:8px}'
    + '.bar{height:5px;background:#ffffff1a;border-radius:4px;margin-bottom:18px;overflow:hidden}.bar>i{display:block;height:100%;background:' + botao + ';width:0;transition:.3s}</style></head>'
    + '<body><div class="card"><div class="bar"><i id="bar"></i></div><div id="steps"></div></div>'
    + '<script>var PS=' + passosJson + ',R={},i=0,redir=' + redir + ';'
    + 'function render(){var s=PS[i];if(!s)return;var h=s.subtitulo?("<h1>"+(s.titulo||"")+"</h1><p class=sub>"+(s.subtitulo||"")+"</p>"):("<h1>"+(s.titulo||"")+"</h1>");'
    + 'if(s.tipo==="multipla"){(s.opcoes||[]).forEach(function(o){h+="<button class=opt data-v=\\""+String(o).replace(/"/g,"&quot;")+"\\">"+o+"</button>";});}'
    + 'else if(s.tipo==="texto"){h+="<textarea id=inp rows=3></textarea>";}'
    + 'else if(s.tipo==="nome"||s.tipo==="telefone"||s.tipo==="email"){h+="<input id=inp type=\\""+(s.tipo==="email"?"email":s.tipo==="telefone"?"tel":"text")+"\\">";}'
    + 'if(s.tipo!=="multipla"){h+="<button class=btn id=nx>"+(i===PS.length-1?"Enviar":"Continuar")+"</button>";}'
    + 'var d=document.getElementById("steps");d.innerHTML="<div>"+h+"</div>";'
    + 'document.getElementById("bar").style.width=(i/((PS.length-1)||1)*100)+"%";'
    + 'var opts=d.querySelectorAll(".opt");opts.forEach(function(b){b.onclick=function(){R[s.chave||"opcao"]=b.getAttribute("data-v");next();};});'
    + 'var nx=document.getElementById("nx");if(nx)nx.onclick=function(){var inp=document.getElementById("inp");if(inp){if(s.obrigatorio&&!inp.value){inp.focus();return;}if(s.chave)R[s.chave]=inp.value;}next();};}'
    + 'function next(){if(i>=PS.length-1){enviar();return;}i++;render();}'
    + 'function enviar(){document.getElementById("bar").style.width="100%";fetch(location.pathname.replace("/q/","/f/"),{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(R)}).then(function(){document.getElementById("steps").innerHTML="<h1>\\u2705 Enviado!</h1><p class=sub>Obrigado.</p>";if(redir)setTimeout(function(){location.href=redir;},1200);}).catch(function(){document.getElementById("steps").innerHTML="<h1>\\u2705 Enviado!</h1>";});}'
    + 'render();</script></body></html>');
});

// ======================= ETAPA 11: Empresas (cadastro B2B) =======================
app.get('/empresas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, empresas: db.prepare(`SELECT * FROM empresas WHERE projeto=? ORDER BY nome ASC`).all(projeto) });
});
app.post('/empresa', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM empresas WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE empresas SET nome=?, cnpj=?, site=?, telefone=? WHERE id=?`).run(String(b.nome || ''), String(b.cnpj || ''), String(b.site || ''), String(b.telefone || ''), id);
  else db.prepare(`INSERT INTO empresas(id,projeto,nome,cnpj,site,telefone,criado_em) VALUES(?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.cnpj || ''), String(b.site || ''), String(b.telefone || ''), agora());
  res.json({ ok: true, id });
});
app.post('/empresa/remover', (req, res) => { db.prepare(`DELETE FROM empresas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });

// ----------------------- Worker de disparo (ritmo humano / anti-ban) -----------------------
let enviando = false;
async function tickFila() {
  if (enviando) return; enviando = true;
  try {
    // Pega o próximo pendente JÁ liberado (respeita o delay do passo via agendado_para).
    const item = db.prepare(
      `SELECT * FROM fila_envio
         WHERE status='pendente' AND (agendado_para IS NULL OR agendado_para <= ?)
         ORDER BY agendado_para IS NULL DESC, agendado_para, criado_em LIMIT 1`
    ).get(agora()) as any;
    if (item) {
      try {
        const tipo = item.tipo || 'texto';
        if (tipo === 'texto') {
          await enviarTexto(item.projeto, item.para, item.texto || '');
        } else {
          await enviarMidia(item.projeto, item.para, tipo, item.url || '', item.legenda || '');
        }
        db.prepare(`UPDATE fila_envio SET status='enviado' WHERE id=?`).run(item.id);
      } catch {
        db.prepare(`UPDATE fila_envio SET tentativas=tentativas+1, status=CASE WHEN tentativas>=3 THEN 'falhou' ELSE 'pendente' END WHERE id=?`).run(item.id);
      }
    }
  } finally { enviando = false; }
}
// 1 envio a cada 8–15s (ritmo humano). Ajustar por número/aquecimento depois.
setInterval(tickFila, 8000 + Math.floor(Math.random() * 7000));


// ============================================================================
// ETAPA 14 (06/10) — MODULO DE AGENTES IA (SDR) NO MOTOR  [add-only]
// ----------------------------------------------------------------------------
// Objetivo: qualificar leads por LLM DENTRO do nosso motor, pra substituir os
// "Agentes IA" da SellFlux (baixo/teclado/violao). Usa o que ja existe:
// enviarTexto (Evolution), eventos (agenda), negocios (CRM), aplicarTag (tags),
// upsertContato. So ACRESCENTA tabelas, funcoes e endpoints.
//
// SEGURANCA / CUT-OVER: o agente SO roda quando CONFIG.agentesHabilitados[projeto]
// === true. Padrao = DESLIGADO em todos. Entao subir este codigo pro ar NAO muda
// nada no comportamento atual (a SellFlux continua cuidando do WhatsApp) ate o
// Ezequias habilitar projeto por projeto. Sem a chave do LLM (env LLM_API_KEY) o
// agente tambem nao responde (fica em modo seguro).
//
// LIGAR DEPOIS (com o Ezequias): 1) por a env LLM_API_KEY (e, se quiser,
// LLM_BASE_URL / LLM_MODEL) no .env do VPS; 2) carregar o prompt de cada
// instrumento (POST /agente ou copiar engine/prompts-seed/<proj>.txt ->
// /data/prompts/<proj>.txt); 3) POST /agentes/habilitar {projeto,on:true}.
// ============================================================================

const LLM_BASE_URL = process.env.LLM_BASE_URL || 'https://api.openai.com/v1';
const LLM_API_KEY = process.env.LLM_API_KEY || '';
const LLM_MODEL_PADRAO = process.env.LLM_MODEL || 'gpt-4o-mini';
const AGENTE_MAX_TOOL_LOOPS = Number(process.env.AGENTE_MAX_TOOL_LOOPS || 6);

db.exec(`
CREATE TABLE IF NOT EXISTS agentes_ia (
  id TEXT PRIMARY KEY,
  projeto TEXT,
  nome TEXT,
  prompt TEXT,
  modelo TEXT,
  temperatura REAL DEFAULT 0.7,
  ativo INTEGER DEFAULT 0,
  tools_on INTEGER DEFAULT 1,
  config TEXT,
  criado_em TEXT,
  atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS agente_conversas (
  id TEXT PRIMARY KEY,
  agente_id TEXT,
  projeto TEXT,
  telefone TEXT,
  contato_id TEXT,
  status TEXT DEFAULT 'ativo',
  estado TEXT,
  historico TEXT,
  criado_em TEXT,
  atualizado_em TEXT
);
CREATE TABLE IF NOT EXISTS agente_logs (
  id TEXT PRIMARY KEY,
  agente_id TEXT,
  telefone TEXT,
  papel TEXT,
  conteudo TEXT,
  criado_em TEXT
);
`);

// Metadados por instrumento (grade real, link do grupo gratis, sala, corte de idade).
// dia da semana: 0=Dom 1=Seg 2=Ter 3=Qua 4=Qui 5=Sex 6=Sab.
// Baixo Qua/Sex; Teclado Seg/Qui; Violao Seg/Qui. (fonte: calibracao dos agentes)
const AGENTE_SEED: any = {
  baixo: {
    nome: 'SDR Baixo',
    grade: { dias: [3, 5], horas: ['11:00', '15:00', '21:00'], duracaoMin: 60, antecedenciaH: 2 },
    grupo: 'https://chat.whatsapp.com/FWl2ym2wow0JqVr4u9k3Hm',
    sala: 'https://meet.google.com/rpw-wdwu-cby',
    corteIdade: 24,
  },
  teclado: {
    nome: 'SDR Teclado',
    grade: { dias: [1, 4], horas: ['11:00', '15:00', '20:00'], duracaoMin: 60, antecedenciaH: 2 },
    grupo: 'https://chat.whatsapp.com/C1VYxajhCJiCt1EFVuWjmY',
    sala: '',
    corteIdade: 24,
  },
  violao: {
    nome: 'SDR Violao',
    grade: { dias: [1, 4], horas: ['14:00', '19:00'], duracaoMin: 60, antecedenciaH: 3 },
    grupo: '',
    sala: '',
    corteIdade: 24,
  },
};

// Garante CONFIG.agentesHabilitados (padrao: tudo desligado) sem apagar config existente.
if (!(CONFIG as any).agentesHabilitados) {
  (CONFIG as any).agentesHabilitados = { baixo: false, violao: false, teclado: false };
}

// Semeia 1 agente por projeto (so se ainda nao existir). Prompt vem vazio; e carregado
// depois via /agente, ou de /data/prompts/<projeto>.txt se o arquivo existir.
function _seedAgentes(): void {
  for (const projeto of PROJETOS) {
    const ja = db.prepare(`SELECT id FROM agentes_ia WHERE projeto=?`).get(projeto) as any;
    if (ja) continue;
    const seed = AGENTE_SEED[projeto] || {};
    let prompt = '';
    try {
      const p = `/data/prompts/${projeto}.txt`;
      if (fs.existsSync(p)) prompt = fs.readFileSync(p, 'utf8');
    } catch { /* sem arquivo, segue vazio */ }
    db.prepare(
      `INSERT INTO agentes_ia(id,projeto,nome,prompt,modelo,temperatura,ativo,tools_on,config,criado_em,atualizado_em)
       VALUES(?,?,?,?,?,?,?,?,?,?,?)`
    ).run(
      randomUUID(), projeto, String(seed.nome || ('SDR ' + projeto)), prompt,
      'gpt-5.4-mini', 0.7, 0, 1, JSON.stringify(seed), agora(), agora()
    );
  }
}
try { _seedAgentes(); } catch (e) { console.log('[agentes] seed falhou:', e); }

function agenteDoProjeto(projeto: string): any {
  return db.prepare(`SELECT * FROM agentes_ia WHERE projeto=? ORDER BY atualizado_em DESC LIMIT 1`).get(projeto) as any;
}

// --------- Agenda: calcula horarios livres a partir da grade + eventos ---------
// Observacao: container roda em UTC; o Brasil (BRT) e UTC-3. A matematica abaixo e
// aproximada e deve ser conferida no 1o teste real (horario de verao nao se aplica no BR atual).
const _DIAS_PT = ['domingo', 'segunda', 'terca', 'quarta', 'quinta', 'sexta', 'sabado'];
function _slotsLivres(projeto: string, periodo: string, limite = 10): any[] {
  const seed = AGENTE_SEED[projeto];
  if (!seed || !seed.grade) return [];
  const grade = seed.grade;
  const noPeriodo = (h: string): boolean => {
    const hh = Number(String(h).split(':')[0]);
    if (periodo === 'manha') return hh < 12;
    if (periodo === 'tarde') return hh >= 12 && hh < 18;
    if (periodo === 'noite') return hh >= 18;
    return true;
  };
  const agoraMs = Date.now();
  const minMs = agoraMs + (grade.antecedenciaH || 2) * 3600 * 1000;
  const ocupados = new Set(
    (db.prepare(`SELECT inicio FROM eventos WHERE projeto=?`).all(projeto) as any[]).map((e) => String(e.inicio))
  );
  const out: any[] = [];
  for (let d = 0; d < 21 && out.length < limite; d++) {
    const brt = new Date(agoraMs + d * 86400000 - 3 * 3600000);
    const dow = brt.getUTCDay();
    if (!grade.dias.includes(dow)) continue;
    for (const h of grade.horas) {
      if (!noPeriodo(h)) continue;
      const parts = String(h).split(':');
      const hh = Number(parts[0]);
      const mm = Number(parts[1] || 0);
      const inicioUtcMs = Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate(), hh + 3, mm);
      if (inicioUtcMs < minMs) continue;
      const iso = new Date(inicioUtcMs).toISOString();
      if (ocupados.has(iso)) continue;
      out.push({ iso, label: _DIAS_PT[dow] + ' ' + String(brt.getUTCDate()).padStart(2, '0') + '/' + String(brt.getUTCMonth() + 1).padStart(2, '0') + ' as ' + h });
      if (out.length >= limite) break;
    }
  }
  return out;
}

// --------- Ferramentas (function-calling) que o agente pode chamar ---------
function _toolsDef(): any[] {
  return [
    { type: 'function', function: { name: 'listar_horarios_livres', description: 'Lista horarios livres reais da agenda do professor no periodo pedido. Use SEMPRE antes de oferecer horario; nunca invente horario.', parameters: { type: 'object', properties: { periodo: { type: 'string', enum: ['manha', 'tarde', 'noite'] } }, required: ['periodo'] } } },
    { type: 'function', function: { name: 'salvar_whatsapp', description: 'Grava o numero de WhatsApp da pessoa no cadastro (so digitos, comecando com 55 e DDD).', parameters: { type: 'object', properties: { telefone: { type: 'string' } }, required: ['telefone'] } } },
    { type: 'function', function: { name: 'marcar_agendamento', description: 'Cria o compromisso na agenda. Use o iso exato de um horario que a ferramenta de horarios retornou como livre.', parameters: { type: 'object', properties: { inicio_iso: { type: 'string' }, nome: { type: 'string' }, observacao: { type: 'string' } }, required: ['inicio_iso', 'nome'] } } },
    { type: 'function', function: { name: 'registrar_card_crm', description: 'Cria/atualiza o card do lead no CRM com um resumo da qualificacao (horario, instrumento, igreja/gosto, dor, o que perguntou).', parameters: { type: 'object', properties: { resumo: { type: 'string' } }, required: ['resumo'] } } },
    { type: 'function', function: { name: 'adicionar_tag', description: 'Marca o lead com uma tag (ex: sdr-instagram).', parameters: { type: 'object', properties: { tag: { type: 'string' } }, required: ['tag'] } } },
    { type: 'function', function: { name: 'transferir_fila', description: 'Transfere a conversa pra fila humana (o professor assume). O agente para de responder esse lead depois disso.', parameters: { type: 'object', properties: { motivo: { type: 'string' } }, required: ['motivo'] } } },
  ];
}

async function _execTool(projeto: string, conversa: any, nomeFerr: string, args: any): Promise<any> {
  const seed = AGENTE_SEED[projeto] || {};
  try {
    if (nomeFerr === 'listar_horarios_livres') {
      const slots = _slotsLivres(projeto, String(args.periodo || ''), 10);
      return { ok: true, horarios: slots, sala: seed.sala || '' };
    }
    if (nomeFerr === 'salvar_whatsapp') {
      const tel = String(args.telefone || '').replace(/\D/g, '');
      if (conversa.contato_id && tel) db.prepare(`UPDATE contatos SET telefone=? WHERE id=?`).run(tel, conversa.contato_id);
      return { ok: true, telefone: tel };
    }
    if (nomeFerr === 'marcar_agendamento') {
      const inicio = String(args.inicio_iso || '');
      const durMin = (seed.grade && seed.grade.duracaoMin) || 60;
      const fim = inicio ? new Date(new Date(inicio).getTime() + durMin * 60000).toISOString() : '';
      const id = randomUUID();
      db.prepare(`INSERT INTO eventos(id,projeto,titulo,inicio,fim,contato_id,obs,criado_em) VALUES(?,?,?,?,?,?,?,?)`)
        .run(id, projeto, 'Sessao ' + projeto + ' - ' + String(args.nome || conversa.telefone), inicio, fim, conversa.contato_id || null, String(args.observacao || ''), agora());
      return { ok: true, id, inicio, fim, sala: seed.sala || '' };
    }
    if (nomeFerr === 'registrar_card_crm') {
      const id = randomUUID();
      db.prepare(`INSERT INTO negocios(id,projeto,contato_id,titulo,valor,etapa,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`)
        .run(id, projeto, conversa.contato_id || null, 'Lead SDR - ' + String(conversa.telefone || ''), 0, 'novo', agora(), agora());
      try { db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), projeto, conversa.contato_id || null, 'nota', 'CRM: ' + String(args.resumo || ''), agora()); } catch { /* nota best-effort */ }
      return { ok: true, id };
    }
    if (nomeFerr === 'adicionar_tag') {
      if (conversa.contato_id) aplicarTag(projeto, conversa.contato_id, String(args.tag || ''));
      return { ok: true };
    }
    if (nomeFerr === 'transferir_fila') {
      db.prepare(`UPDATE agente_conversas SET status='humano', atualizado_em=? WHERE id=?`).run(agora(), conversa.id);
      if (conversa.contato_id) { try { aplicarTag(projeto, conversa.contato_id, 'fila-humana'); } catch { /* best-effort */ } }
      return { ok: true, transferido: true };
    }
  } catch (e: any) {
    return { ok: false, erro: String(e && e.message || e) };
  }
  return { ok: false, erro: 'ferramenta desconhecida' };
}

// --------- Chamada ao LLM (API compativel com OpenAI chat/completions) ---------
async function _chamarLLM(messages: any[], modelo: string, temperatura: number, tools: any[]): Promise<any> {
  if (!LLM_API_KEY) return { content: '', _semChave: true };
  const body: any = { model: modelo || LLM_MODEL_PADRAO, messages, temperature: (temperatura == null ? 0.7 : temperatura) };
  if (tools && tools.length) { body.tools = tools; body.tool_choice = 'auto'; }
  const res = await fetch(`${LLM_BASE_URL}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${LLM_API_KEY}` },
    body: JSON.stringify(body),
  });
  const txt = await res.text();
  let data: any = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = txt; }
  if (!res.ok) throw new Error(`LLM ${res.status}: ${txt}`);
  return (data && data.choices && data.choices[0] && data.choices[0].message) || { content: '' };
}

// --------- Runtime do agente: processa UMA mensagem recebida ---------
// opts.teste=true: nao envia pelo WhatsApp, devolve as respostas num array (pro Chat de teste).
async function processarMensagemAgente(projeto: string, telefone: string, texto: string, nome?: string, contatoId?: string, opts?: any): Promise<any> {
  const teste = !!(opts && opts.teste);
  const agente = agenteDoProjeto(projeto);
  if (!agente || !agente.ativo || !String(agente.prompt || '').trim()) {
    return { ok: false, motivo: 'sem agente ativo/ prompt' };
  }
  let conversa = db.prepare(`SELECT * FROM agente_conversas WHERE agente_id=? AND telefone=?`).get(agente.id, telefone) as any;
  if (!conversa) {
    const id = randomUUID();
    db.prepare(`INSERT INTO agente_conversas(id,agente_id,projeto,telefone,contato_id,status,estado,historico,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?)`)
      .run(id, agente.id, projeto, telefone, contatoId || null, 'ativo', '{}', '[]', agora(), agora());
    conversa = db.prepare(`SELECT * FROM agente_conversas WHERE id=?`).get(id) as any;
  }
  if (conversa.status !== 'ativo') return { ok: false, motivo: 'conversa nao-ativa (' + conversa.status + ')' };

  let historico: any[] = [];
  try { historico = JSON.parse(conversa.historico || '[]'); } catch { historico = []; }
  historico.push({ role: 'user', content: texto });

  const tools = agente.tools_on ? _toolsDef() : [];
  const respostasEnviadas: string[] = [];
  try {
    for (let loop = 0; loop < AGENTE_MAX_TOOL_LOOPS; loop++) {
      const messages = [{ role: 'system', content: String(agente.prompt || '') }].concat(historico);
      const msg = await _chamarLLM(messages, agente.modelo, agente.temperatura, tools);
      if (msg._semChave) { return { ok: false, motivo: 'LLM sem chave (LLM_API_KEY)' }; }

      if (msg.tool_calls && msg.tool_calls.length) {
        historico.push({ role: 'assistant', content: msg.content || '', tool_calls: msg.tool_calls });
        for (const tc of msg.tool_calls) {
          let args: any = {};
          try { args = JSON.parse(tc.function.arguments || '{}'); } catch { args = {}; }
          const resultado = await _execTool(projeto, conversa, tc.function.name, args);
          historico.push({ role: 'tool', tool_call_id: tc.id, content: JSON.stringify(resultado) });
          // se transferiu, recarrega status
          if (tc.function.name === 'transferir_fila') conversa = db.prepare(`SELECT * FROM agente_conversas WHERE id=?`).get(conversa.id) as any;
        }
        continue; // volta pro LLM com os resultados das ferramentas
      }

      const conteudo = String(msg.content || '').trim();
      if (conteudo) {
        historico.push({ role: 'assistant', content: conteudo });
        respostasEnviadas.push(conteudo);
        if (!teste) { try { await enviarTexto(projeto as any, telefone, conteudo); } catch (e) { console.log('[agente] enviarTexto falhou', e); } }
      }
      break; // sem tool_calls => fim do turno
    }
  } catch (e: any) {
    console.log('[agente] erro no loop:', e && e.message || e);
  }

  db.prepare(`UPDATE agente_conversas SET historico=?, contato_id=COALESCE(contato_id,?), atualizado_em=? WHERE id=?`)
    .run(JSON.stringify(historico).slice(0, 200000), contatoId || null, agora(), conversa.id);
  try { db.prepare(`INSERT INTO agente_logs(id,agente_id,telefone,papel,conteudo,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), agente.id, telefone, 'turno', (respostasEnviadas.join(' | ')).slice(0, 2000), agora()); } catch { /* log best-effort */ }
  return { ok: true, respostas: respostasEnviadas, status: (db.prepare(`SELECT status FROM agente_conversas WHERE id=?`).get(conversa.id) as any)?.status };
}

// --------- Endpoints (todos atras do x-api-key, igual os outros) ---------
app.get('/agentes', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = projeto
    ? db.prepare(`SELECT * FROM agentes_ia WHERE projeto=? ORDER BY atualizado_em DESC`).all(projeto)
    : db.prepare(`SELECT * FROM agentes_ia ORDER BY projeto ASC`).all();
  res.json({ ok: true, agentes: rows, habilitados: (CONFIG as any).agentesHabilitados || {} });
});
app.post('/agente', (req, res) => {
  const b = req.body || {};
  const id = String(b.id || '');
  if (id) {
    const ex = db.prepare(`SELECT id FROM agentes_ia WHERE id=?`).get(id);
    if (ex) {
      db.prepare(`UPDATE agentes_ia SET nome=?, prompt=?, modelo=?, temperatura=?, ativo=?, tools_on=?, config=?, atualizado_em=? WHERE id=?`)
        .run(String(b.nome || ''), String(b.prompt || ''), String(b.modelo || 'gpt-5.4-mini'), Number(b.temperatura == null ? 0.7 : b.temperatura), b.ativo ? 1 : 0, b.toolsOn === false ? 0 : 1, JSON.stringify(b.config || {}), agora(), id);
      return res.json({ ok: true, id });
    }
  }
  const novo = id || randomUUID();
  db.prepare(`INSERT INTO agentes_ia(id,projeto,nome,prompt,modelo,temperatura,ativo,tools_on,config,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?)`)
    .run(novo, String(b.projeto || ''), String(b.nome || ''), String(b.prompt || ''), String(b.modelo || 'gpt-5.4-mini'), Number(b.temperatura == null ? 0.7 : b.temperatura), b.ativo ? 1 : 0, b.toolsOn === false ? 0 : 1, JSON.stringify(b.config || {}), agora(), agora());
  res.json({ ok: true, id: novo });
});
app.post('/agente/remover', (req, res) => { db.prepare(`DELETE FROM agentes_ia WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/agente/conversas', (req, res) => {
  const projeto = String(req.query.projeto || '');
  res.json({ ok: true, conversas: db.prepare(`SELECT id,projeto,telefone,status,atualizado_em FROM agente_conversas WHERE projeto=? ORDER BY atualizado_em DESC LIMIT 200`).all(projeto) });
});
app.post('/agente/conversa/reativar', (req, res) => { db.prepare(`UPDATE agente_conversas SET status='ativo', atualizado_em=? WHERE id=?`).run(agora(), String((req.body || {}).id || '')); res.json({ ok: true }); });
app.post('/agentes/habilitar', (req, res) => {
  const b = req.body || {};
  if (!(CONFIG as any).agentesHabilitados) (CONFIG as any).agentesHabilitados = {};
  (CONFIG as any).agentesHabilitados[String(b.projeto || '')] = !!b.on;
  try { salvarConfig(); } catch { /* best-effort */ }
  res.json({ ok: true, habilitados: (CONFIG as any).agentesHabilitados });
});
// Chat de teste: roda o agente sem enviar pelo WhatsApp. telefone 'teste-<proj>' por padrao.
app.post('/agente/testar', async (req, res) => {
  const b = req.body || {};
  const projeto = String(b.projeto || '');
  const telefone = String(b.telefone || ('teste-' + projeto));
  const r = await processarMensagemAgente(projeto, telefone, String(b.texto || ''), 'Teste', undefined, { teste: true });
  res.json(r);
});


// ============================================================================
// ETAPA 15 — Central de Atendimento (API unificada de atendimento).
// Aditivo. NAO altera o gatilho do webhook/evolution nem liga canal sozinho.
// O cut-over (ligar a IA de verdade) continua por projeto na aba "Agentes IA".
// ============================================================================
db.exec(`
CREATE TABLE IF NOT EXISTS atendimentos (
  contato_id TEXT PRIMARY KEY,
  projeto TEXT,
  canal TEXT DEFAULT 'wa',
  status TEXT DEFAULT 'fila',
  responsavel TEXT,
  instancia TEXT,
  atualizado_em TEXT
);
`);
try { db.exec(`ALTER TABLE contatos ADD COLUMN canal TEXT DEFAULT 'wa'`); } catch { /* coluna ja existe */ }
try { db.exec(`ALTER TABLE mensagens ADD COLUMN canal TEXT DEFAULT 'wa'`); } catch { /* coluna ja existe */ }
try { db.exec(`ALTER TABLE mensagens ADD COLUMN instancia TEXT`); } catch { /* ja existe */ }

function hhmmCA(iso: string): string {
  try {
    const d = new Date(iso); const hoje = new Date();
    if (d.toDateString() !== hoje.toDateString()) return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' });
    return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
  } catch { return ''; }
}
function statusAtendimentoCA(contatoId: string): { status: string; responsavel?: string; instancia?: string } {
  const at = db.prepare(`SELECT status, responsavel, instancia FROM atendimentos WHERE contato_id=?`).get(contatoId) as any;
  const ac = db.prepare(`SELECT status FROM agente_conversas WHERE contato_id=? ORDER BY atualizado_em DESC LIMIT 1`).get(contatoId) as any;
  if (ac && ac.status === 'ativo') return { status: 'ia', responsavel: at?.responsavel, instancia: at?.instancia };
  if (at) return { status: at.status, responsavel: at.responsavel, instancia: at.instancia };
  return { status: 'fila' };
}
function setAtendimentoCA(contatoId: string, campos: any): void {
  const existe = db.prepare(`SELECT contato_id FROM atendimentos WHERE contato_id=?`).get(contatoId);
  if (existe) {
    const chaves = Object.keys(campos);
    const sets = chaves.map((k) => `${k}=?`).join(',');
    const vals = chaves.map((k) => campos[k]);
    db.prepare(`UPDATE atendimentos SET ${sets}, atualizado_em=? WHERE contato_id=?`).run(...vals, agora(), contatoId);
  } else {
    const c = db.prepare(`SELECT projeto, COALESCE(canal,'wa') canal FROM contatos WHERE id=?`).get(contatoId) as any;
    db.prepare(`INSERT INTO atendimentos(contato_id,projeto,canal,status,responsavel,instancia,atualizado_em) VALUES(?,?,?,?,?,?,?)`)
      .run(contatoId, c?.projeto || '', c?.canal || 'wa', campos.status || 'fila', campos.responsavel || null, campos.instancia || null, agora());
  }
}
function idsDoBodyCA(b: any): string[] {
  const raw = b?.contatos != null ? b.contatos : (b?.contatoId != null ? b.contatoId : []);
  return (Array.isArray(raw) ? raw : [raw]).map((x: any) => String(x || '')).filter((x: string) => x);
}

app.get('/atendimento/conversas', (req, res) => {
  try {
    const canal = String(req.query.canal || 'wa');
    const social = canal.indexOf('ig') >= 0;   // ig / ia-ig => Instagram + Facebook
    const querIA = canal.indexOf('ia') === 0;  // abas que comecam com "ia"
    const rows = db.prepare(`
      SELECT c.id contato_id, c.nome, c.telefone, c.projeto, COALESCE(c.canal,'wa') net,
             m.texto ultima, m.instancia inst_msg, MAX(m.criado_em) quando
      FROM mensagens m JOIN contatos c ON c.id=m.contato_id
      GROUP BY c.id ORDER BY quando DESC LIMIT 400
    `).all() as any[];
    const out: any[] = [];
    const _numByInst: any = {}; const _numByProj: any = {};
    for (const nn of (db.prepare(`SELECT instancia, projeto, telefone, tipo FROM numeros ORDER BY criado_em`).all() as any[])) { if (nn.instancia) _numByInst[nn.instancia] = nn; if (nn.projeto && !_numByProj[nn.projeto]) _numByProj[nn.projeto] = nn; }
    for (const r of rows) {
      const net = r.net || 'wa';
      const ehSocial = net === 'ig' || net === 'fb';
      if (social !== ehSocial) continue;
      const st = statusAtendimentoCA(r.contato_id);
      if (st.status === 'finalizado') continue;
      const ehIA = st.status === 'ia';
      if (querIA !== ehIA) continue;
      const _nn = _numByInst[r.inst_msg] || _numByProj[r.projeto] || null;
      const _tel = _nn ? _nn.telefone : '';
      const _of = !!(_nn && /oficial/i.test(String(_nn.tipo || '')) && !/nao/i.test(String(_nn.tipo || '')));
      out.push({
        contato_id: r.contato_id, nome: r.nome || r.telefone, inst: r.projeto || '', net,
        tipo: 'dm', status: st.status, dev: 0, ultima: r.ultima || '',
        hora: hhmmCA(r.quando), ph: r.telefone || '', live: ehIA,
        numero: _tel, num4: String(_tel).replace(/\D/g,'').slice(-4),
        oficial: _of,
        instancia: r.inst_msg || '',
      });
    }
    res.json({ ok: true, conversas: out });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.get('/atendimento/mensagens', (req, res) => {
  const contatoId = String(req.query.contatoId || '');
  const ms = db.prepare(`SELECT id, direcao, texto, criado_em FROM mensagens WHERE contato_id=? ORDER BY criado_em ASC LIMIT 400`).all(contatoId) as any[];
  res.json({ ok: true, mensagens: ms.map((m) => ({ id: m.id, direcao: m.direcao, texto: m.texto, hora: hhmmCA(m.criado_em) })) });
});

app.post('/atendimento/assumir', (req, res) => {
  const b = req.body || {};
  const contatoId = String(b.contatoId || '');
  if (!contatoId) return res.json({ ok: false, erro: 'contatoId' });
  setAtendimentoCA(contatoId, { status: 'meu', responsavel: String(b.responsavel || 'humano') });
  db.prepare(`UPDATE agente_conversas SET status='humano', atualizado_em=? WHERE contato_id=?`).run(agora(), contatoId);
  res.json({ ok: true });
});

app.post('/atendimento/devolver', (req, res) => {
  const contatoId = String((req.body || {}).contatoId || '');
  if (!contatoId) return res.json({ ok: false, erro: 'contatoId' });
  setAtendimentoCA(contatoId, { status: 'ia', responsavel: null });
  db.prepare(`UPDATE agente_conversas SET status='ativo', atualizado_em=? WHERE contato_id=?`).run(agora(), contatoId);
  res.json({ ok: true });
});

app.post('/atendimento/finalizar', (req, res) => {
  const ids = idsDoBodyCA(req.body || {});
  for (const id of ids) setAtendimentoCA(id, { status: 'finalizado', responsavel: null });
  res.json({ ok: true, n: ids.length });
});

app.post('/atendimento/ativar-ia', (req, res) => {
  const ids = idsDoBodyCA(req.body || {});
  for (const id of ids) {
    setAtendimentoCA(id, { status: 'ia', responsavel: null });
    const ac = db.prepare(`SELECT id FROM agente_conversas WHERE contato_id=? ORDER BY atualizado_em DESC LIMIT 1`).get(id) as any;
    if (ac) db.prepare(`UPDATE agente_conversas SET status='ativo', atualizado_em=? WHERE id=?`).run(agora(), ac.id);
  }
  res.json({ ok: true, n: ids.length });
});

app.post('/atendimento/transferir', (req, res) => {
  const b = req.body || {};
  const ids = idsDoBodyCA(b);
  const paraIA = !!b.agenteIA;
  for (const id of ids) {
    if (paraIA) {
      setAtendimentoCA(id, { status: 'ia', responsavel: null });
      const ac = db.prepare(`SELECT id FROM agente_conversas WHERE contato_id=? ORDER BY atualizado_em DESC LIMIT 1`).get(id) as any;
      if (ac) db.prepare(`UPDATE agente_conversas SET status='ativo', atualizado_em=? WHERE id=?`).run(agora(), ac.id);
    } else {
      setAtendimentoCA(id, { status: 'meu', responsavel: String(b.destino || b.atendente || b.departamento || 'humano') });
      db.prepare(`UPDATE agente_conversas SET status='humano', atualizado_em=? WHERE contato_id=?`).run(agora(), id);
    }
  }
  res.json({ ok: true, n: ids.length });
});

app.get('/atendimento/numeros', (_req, res) => {
  res.json({ ok: true, numeros: db.prepare(`SELECT id, projeto, telefone, instancia, tipo, status FROM numeros ORDER BY projeto, telefone`).all() });
});



// ============================================================================
// ETAPA 15b — Grupos + Disparo em massa (blocos/mídia/velocidade/agendamento/
// sequência) + Tags (listar/criar). ADITIVO. Namespace /disparo/*.
// Reaproveita evo/enviarTexto/enviarMidia/aplicarTag/fila_envio. O disparo usa
// status 'camp_pend' na fila + ticker próprio (NÃO mexe no tickFila existente).
// ============================================================================
db.exec(`
CREATE TABLE IF NOT EXISTS grupos_wpp (
  id TEXT PRIMARY KEY,
  projeto TEXT,
  instancia TEXT,
  nome TEXT,
  tamanho INTEGER DEFAULT 0,
  inst_manual TEXT,
  atualizado_em TEXT
);
`);

app.post('/disparo/grupos/sincronizar', async (req, res) => {
  try {
    const projetoFiltro = String((req.body || {}).projeto || '');
    const nums = db.prepare(`SELECT instancia, projeto FROM numeros${projetoFiltro ? ' WHERE projeto=?' : ''}`).all(...(projetoFiltro ? [projetoFiltro] : [])) as any[];
    let total = 0; const erros: string[] = [];
    for (const n of nums) {
      if (!n.instancia) continue;
      try {
        const r: any = await evo(`/group/fetchAllGroups/${n.instancia}?getParticipants=false`, 'GET');
        const arr: any[] = Array.isArray(r) ? r : (r?.groups || r?.data || []);
        const up = db.prepare(`INSERT INTO grupos_wpp(id,projeto,instancia,nome,tamanho,atualizado_em) VALUES(?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET nome=excluded.nome, tamanho=excluded.tamanho, instancia=excluded.instancia, projeto=excluded.projeto, atualizado_em=excluded.atualizado_em`);
        const tx = db.transaction(() => {
          for (const g of arr) {
            const jid = g.id || g.jid || g.remoteJid; if (!jid) continue;
            up.run(String(jid), n.projeto, n.instancia, g.subject || g.name || String(jid), Number(g.size || (g.participants ? g.participants.length : 0) || 0), agora());
            total++;
          }
        });
        tx();
      } catch (e: any) { erros.push(`${n.instancia}: ${String(e.message || e)}`); }
    }
    res.json({ ok: true, total, erros });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.get('/disparo/grupos', (_req, res) => {
  const rows = db.prepare(`SELECT id, nome, COALESCE(inst_manual, projeto) inst, projeto, instancia, tamanho FROM grupos_wpp ORDER BY projeto, nome`).all();
  res.json({ ok: true, grupos: rows });
});

app.post('/disparo/grupo/instrumento', (req, res) => {
  const b = req.body || {};
  db.prepare(`UPDATE grupos_wpp SET inst_manual=? WHERE id=?`).run(String(b.inst || ''), String(b.id || ''));
  res.json({ ok: true });
});

app.get('/disparo/tags', (req, res) => {
  const projeto = String(req.query.projeto || '');
  const rows = db.prepare(`SELECT t.nome, t.projeto, COUNT(ct.contato_id) n FROM tags t LEFT JOIN contato_tags ct ON ct.tag_id=t.id ${projeto ? 'WHERE t.projeto=?' : ''} GROUP BY t.id ORDER BY n DESC LIMIT 500`).all(...(projeto ? [projeto] : [])) as any[];
  res.json({ ok: true, tags: rows.map((r) => ({ nome: r.nome, inst: r.projeto, n: r.n })) });
});

app.post('/disparo/tag/criar', (req, res) => {
  const b = req.body || {}; const nome = String(b.nome || '').trim(); const projeto = String(b.projeto || '');
  if (!nome || !projeto) return res.json({ ok: false, erro: 'nome/projeto' });
  const ex = db.prepare(`SELECT id FROM tags WHERE projeto=? AND nome=?`).get(projeto, nome);
  if (!ex) db.prepare(`INSERT INTO tags(id,projeto,nome) VALUES(?,?,?)`).run(randomUUID(), projeto, nome);
  res.json({ ok: true, nome, projeto });
});

function _alvosDisparo(p: any): { projeto: string; para: string; grupo: boolean }[] {
  const out: { projeto: string; para: string; grupo: boolean }[] = [];
  const padrao = (CONFIG as any).projetoPadrao || 'teclado';
  if (p.alvoTipo === 'grupos') {
    for (const jid of (p.grupos || [])) {
      const g = db.prepare(`SELECT projeto FROM grupos_wpp WHERE id=?`).get(jid) as any;
      out.push({ projeto: g?.projeto || p.projeto || padrao, para: String(jid), grupo: true });
    }
  } else {
    const inc = (p.incluirTags || []); const exc = (p.excluirTags || []);
    if (!inc.length) return out;
    const ph = (a: any[]) => a.map(() => '?').join(',');
    let sql = `SELECT DISTINCT c.id, c.telefone, c.projeto FROM contatos c
      JOIN contato_tags ct ON ct.contato_id=c.id JOIN tags t ON t.id=ct.tag_id
      WHERE t.nome IN (${ph(inc)})`;
    const args: any[] = [...inc];
    if (exc.length) { sql += ` AND c.id NOT IN (SELECT ct2.contato_id FROM contato_tags ct2 JOIN tags t2 ON t2.id=ct2.tag_id WHERE t2.nome IN (${ph(exc)}))`; args.push(...exc); }
    if (p.projeto) { sql += ` AND c.projeto=?`; args.push(p.projeto); }
    const rows = db.prepare(sql).all(...args) as any[];
    for (const r of rows) if (r.telefone) out.push({ projeto: r.projeto || p.projeto || padrao, para: String(r.telefone), grupo: false });
  }
  return out;
}

function _enfileiraDisparo(p: any): number {
  const alvos = _alvosDisparo(p);
  const blocos = (p.blocos || []).filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
  if (!alvos.length || !blocos.length) return 0;
  const intervalo = Math.max(1, Number(p.intervaloSegundos || 12));
  const base = p.agendamento ? new Date(p.agendamento).getTime() : Date.now();
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,agendado_para,criado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
  let n = 0;
  const tx = db.transaction(() => {
    alvos.forEach((a, i) => {
      blocos.forEach((b: any, j: number) => {
        const quando = new Date(base + i * intervalo * 1000 + j * 2000).toISOString();
        const tipo = b.tipo === 'texto' ? 'texto' : b.tipo;
        ins.run(randomUUID(), a.projeto, a.para, a.grupo ? 1 : 0, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), 'camp_pend', quando, agora());
        n++;
      });
    });
  });
  tx();
  return n;
}

app.post('/disparo/enviar', (req, res) => {
  try { const n = _enfileiraDisparo(req.body || {}); res.json({ ok: true, enfileirados: n }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.post('/disparo/sequencia', (req, res) => {
  try { let n = 0; for (const passo of (((req.body || {}).sequencia) || [])) n += _enfileiraDisparo(passo); res.json({ ok: true, enfileirados: n }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// resumo do disparo (pendentes/enviados/falhou)
app.get('/disparo/resumo', (_req, res) => {
  res.json({ ok: true, porStatus: db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE status IN ('camp_pend','enviado','falhou') GROUP BY status`).all() });
});

// ticker próprio do disparo (1s; respeita o agendado_para → intervalo/agenda; NÃO toca no tickFila)
let _campEnviando = false;
async function tickCampanha() {
  if (_campEnviando) return; _campEnviando = true;
  try {
    const item = db.prepare(`SELECT * FROM fila_envio WHERE status='camp_pend' AND (agendado_para IS NULL OR agendado_para <= ?) ORDER BY agendado_para, criado_em LIMIT 1`).get(agora()) as any;
    if (item) {
      try {
        if ((item.tipo || 'texto') === 'texto') await enviarTexto(item.projeto, item.para, item.texto || '');
        else await enviarMidia(item.projeto, item.para, item.tipo, item.url || '', item.legenda || '');
        db.prepare(`UPDATE fila_envio SET status='enviado' WHERE id=?`).run(item.id);
      } catch {
        db.prepare(`UPDATE fila_envio SET tentativas=tentativas+1, status=CASE WHEN tentativas>=3 THEN 'falhou' ELSE 'camp_pend' END WHERE id=?`).run(item.id);
      }
    }
  } finally { _campEnviando = false; }
}
setInterval(tickCampanha, 1000);



// ---- Faturamento (Dashboard estilo SellFlux) ----
function statusFaturamento(statusRaw: string, categoria: string): string {
  const s = String(statusRaw || '').toLowerCase();
  if (categoria === 'reembolso') return 'estorno';
  if (categoria === 'aprovado') return 'liquidada';
  if (/waiting|pending|billet|boleto|aguard|pix.*(pend|gerad)|request_confirmed/.test(s)) return 'aguardando';
  if (categoria === 'perdido') return 'perdido';
  return 'outro';
}
function _rangeFat(periodo: string): { de: string; ate: string } {
  const now = new Date(); const ate = new Date(now);
  const de = new Date(now); de.setHours(0, 0, 0, 0);
  const p = String(periodo || '30d');
  if (p === 'hoje') { /* de=hoje 00:00 */ }
  else if (p === 'ontem') { de.setDate(de.getDate() - 1); ate.setDate(ate.getDate() - 1); ate.setHours(23, 59, 59, 999); }
  else if (p === '3d') de.setDate(de.getDate() - 2);
  else if (p === '7d') de.setDate(de.getDate() - 6);
  else if (p === '30d') de.setDate(de.getDate() - 29);
  else if (p === '90d') de.setDate(de.getDate() - 89);
  else if (p === 'mes') de.setDate(1);
  else if (p === 'mespassado') { de.setDate(1); de.setMonth(de.getMonth() - 1); const fim = new Date(de.getFullYear(), de.getMonth() + 1, 0, 23, 59, 59, 999); return { de: de.toISOString(), ate: fim.toISOString() }; }
  else if (p === 'vitalicio') { de.setFullYear(2000); }
  else de.setDate(de.getDate() - 29);
  return { de: de.toISOString(), ate: ate.toISOString() };
}
app.get('/relatorios/faturamento', (req, res) => {
  try {
    const { de, ate } = _rangeFat(String(req.query.periodo || '30d'));
    const projeto = String(req.query.projeto || '');
    const where = `criado_em >= ? AND criado_em <= ?` + (projeto ? ` AND projeto=?` : '');
    const args: any[] = projeto ? [de, ate, projeto] : [de, ate];
    const rows = db.prepare(`SELECT substr(criado_em,1,10) dia, status, SUM(valor) v, COUNT(*) n FROM transacoes WHERE ${where} GROUP BY dia, status ORDER BY dia`).all(...args) as any[];
    const totaisRows = db.prepare(`SELECT status, SUM(valor) v, COUNT(*) n FROM transacoes WHERE ${where} GROUP BY status`).all(...args) as any[];
    const totais: any = { liquidada: 0, aguardando: 0, estorno: 0, perdido: 0, outro: 0 };
    for (const t of totaisRows) totais[t.status] = t.v || 0;
    const leadsAtivos = (db.prepare(`SELECT COUNT(*) n FROM contatos` + (projeto ? ` WHERE projeto=?` : ``)).get(...(projeto ? [projeto] : [])) as any)?.n || 0;
    res.json({ ok: true, de, ate, dias: rows, totais, leadsAtivos });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});


app.listen(PORT, () => console.log(`[engine] ouvindo na porta ${PORT} — base ${PUBLIC_BASE_URL}`));
