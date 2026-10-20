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
import { randomUUID, createHash, createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import net from 'node:net';
import tls from 'node:tls';
import fs from 'node:fs';
import { execFileSync, spawn } from 'node:child_process'; // etapa21: rclone (OneDrive); etapa33: spawn p/ ffmpeg (lives)

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

const PROJETOS = ['baixo', 'violao', 'teclado', 'comunidade'] as const;
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
  `ALTER TABLE paginas ADD COLUMN seo_titulo TEXT`,
  `ALTER TABLE paginas ADD COLUMN seo_descricao TEXT`,
  `ALTER TABLE paginas ADD COLUMN seo_imagem TEXT`,
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
// chamarPeloNome: liga/desliga o uso do nome da pessoa na mensagem (padrão: ligado).
// nomeFallback: o que usar no lugar do nome quando NÃO há nome (ou quando está desligado).
type FluxoProduto = { chave: string; porId?: boolean; projeto?: Projeto; fluxo: FluxoPasso[]; chamarPeloNome?: boolean; nomeFallback?: string; instancia?: string };

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
  // [motor2] TAGS DA TRILHA por PRODUTO: casa por id (porId:true) OU por pedaço do nome (padrão),
  // minúsculas. Na compra aprovada (inclusive order bump/upsell), cada tag é aplicada e dispara
  // rodarAutomacoes('tag_adicionada'), reaproveitando os fluxos/automações que já existem
  // (grupo, webinário, sequência de mensagens, qualificação...). INERTE até pubFlag motor2_ativo=1.
  produtoTags?: { chave: string; porId?: boolean; tags: string[] }[];
  // [motor2] Tags (pedaço, minúsculas) que AUTORIZAM o agente IA a responder no número compartilhado
  // (8151). Sem tag casada => lead fica na FILA pra atendente humano. Somadas às padrão do código.
  agenteTagsGatilho?: string[];
  // [num-escolha] Número PADRÃO de envio por projeto (nome da instância). Usado por numeroConectado
  // como preferência; se vazio/desconectado, cai no 1º conectado. Evita mandar do número errado (ex.: pessoal).
  numeroPadrao?: Partial<Record<Projeto, string>>;
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
  // [motor2] SEED inicial das tags por produto = os 4 LOW TICKETS confirmados, com os NOMES EXATOS
  // das tags do Sellflux (pra seguir idêntico ao que já acontecia) + marcadores 'low-ticket' e
  // 'qualificar-24h' (o marcador 'low-ticket' é o que o gate do agente usa — exclui high ticket).
  // Chave = pedaço do NOME do produto (SEM acento; o matcher não tira acento), igual chega no webhook.
  produtoTags: [
    { chave: '20 dedilhados',       tags: ['compra-realizada-20-dedilhados-para-sair-do-basico-em-video-aula-com-pdf', 'low-ticket', 'qualificar-24h'] },
    { chave: 'combo de 200 cifras', tags: ['compra-realizada-combo-de-200-cifras-100-hinos-da-harpa-crista-e-100-classicos-do-gospel', 'low-ticket', 'qualificar-24h'] },
    { chave: 'melhores cifras',     tags: ['compra-realizada-100-melhores-cifras-melodicas-dos-classicos-do-gospel-teclado-volume-1', 'low-ticket', 'qualificar-24h'] },
    { chave: 'ebook 200 cifras',    tags: ['compra-realizada-ebook-digital-com-200-cifras-com-60-de-desconto-4-video-aulas-de-ritmos-no-teclado', 'low-ticket', 'qualificar-24h'] },
  ],
  agenteTagsGatilho: ['low-ticket', 'grupo-vip', 'qualificar'],
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
  // [num-escolha] Preferir o número PADRÃO escolhido pro projeto (CONFIG.numeroPadrao), se conectado.
  // Senão, o 1º conectado (comportamento antigo). Corrige o motor pegar o número errado por ordem de criação.
  const padraoNome = ((CONFIG as any).numeroPadrao || {})[projeto];
  if (padraoNome) {
    const pref = db.prepare(`SELECT * FROM numeros WHERE projeto=? AND instancia=? AND status='conectado'`).get(projeto, String(padraoNome));
    if (pref) return pref;
  }
  return db.prepare(
    `SELECT * FROM numeros WHERE projeto=? AND status='conectado' ORDER BY criado_em LIMIT 1`
  ).get(projeto);
}

/** Envia texto por um número do projeto (pega o 1º conectado como padrão). */
async function enviarTexto(projeto: Projeto, para: string, texto: string, instancia?: string) {
  // [num-escolha] instancia opcional: manda por um número específico. Sem ela, usa o padrão do projeto.
  const inst = instancia || numeroConectado(projeto)?.instancia;
  if (!inst) throw new Error(`Nenhum número conectado no projeto ${projeto}`);
  return evo(`/message/sendText/${inst}`, 'POST', {
    number: para, text: texto,
  });
}

/** Envia MÍDIA (imagem/vídeo/áudio/documento) por URL, por um número do projeto. */
async function enviarMidia(projeto: Projeto, para: string, tipo: string, url: string, legenda?: string, instancia?: string) {
  // [num-escolha] instancia opcional: manda por um número específico. Sem ela, usa o padrão do projeto.
  const inst = instancia || numeroConectado(projeto)?.instancia;
  if (!inst) throw new Error(`Nenhum número conectado no projeto ${projeto}`);
  if (!url) throw new Error('midia_sem_url');
  if (tipo === 'audio') {
    return evo(`/message/sendWhatsAppAudio/${inst}`, 'POST', { number: para, audio: url });
  }
  const mediatype = tipo === 'video' ? 'video' : (tipo === 'documento' || tipo === 'document') ? 'document' : 'image';
  const body: any = { number: para, mediatype, media: url };
  if (legenda) body.caption = preenche(legenda);
  if (mediatype === 'document') body.fileName = (url.split('/').pop() || 'arquivo').split('?')[0];
  return evo(`/message/sendMedia/${inst}`, 'POST', body);
}

/** Envia UM passo de fluxo (texto ou mídia), já com {nome} preenchido. */
async function enviarPasso(projeto: Projeto, para: string, passo: FluxoPasso, nome?: string, opts?: OpcoesNome) {
  const tipo = passo.tipo || 'texto';
  if (tipo === 'texto') {
    return enviarTexto(projeto, para, preencheNome(passo.texto || '', nome, opts));
  }
  return enviarMidia(projeto, para, tipo, passo.url || '', preencheNome(passo.texto || passo.legenda || '', nome, opts));
}

/** Enfileira um FLUXO (vários passos com delay) pra um contato. O worker manda no ritmo,
 * respeitando o delay de cada passo (agendado_para). Preenche {nome} em texto/legenda. */
function enfileirarFluxo(projeto: Projeto, para: string, passos: FluxoPasso[], nome?: string, opts?: OpcoesNome, instancia?: string) {
  // [num-escolha] instancia opcional: grava o número de envio na fila; o tickFila respeita.
  const ins = db.prepare(
    `INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,agendado_para,criado_em)
     VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
  );
  let acumulado = 0;
  const tx = db.transaction(() => {
    for (const p of passos || []) {
      acumulado += Math.max(0, Number(p.delaySegundos || 0));
      const tipo = p.tipo || 'texto';
      const texto = tipo === 'texto' ? preencheNome(p.texto || '', nome, opts) : '';
      const legenda = tipo === 'texto' ? '' : preencheNome(p.texto || p.legenda || '', nome, opts);
      ins.run(randomUUID(), projeto, para, 0, texto, tipo, p.url || null, legenda || null,
        'pendente', instancia || null, emSegundos(acumulado), agora());
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

/** Acha o FLUXO DO PRODUTO inteiro (com as opções de nome), se casar por id/nome. */
function produtoFluxoMatch(produtoId?: string, produtoNome?: string): FluxoProduto | null {
  const alvo = `${produtoId || ''} ${produtoNome || ''}`.toLowerCase();
  const id = String(produtoId || '').toLowerCase();
  for (const fp of CONFIG.entregaFluxoProduto || []) {
    const chave = String(fp.chave || '').toLowerCase();
    if (!chave) continue;
    const casa = fp.porId ? (id && id === chave) : alvo.includes(chave);
    if (casa && Array.isArray(fp.fluxo) && fp.fluxo.length) return fp;
  }
  return null;
}

/** Dispara a ENTREGA: usa o fluxo do produto/projeto se houver; senão a mensagem simples. */
async function dispararEntrega(projeto: Projeto, telefone: string, nome?: string, produtoId?: string, produtoNome?: string): Promise<'fluxo' | 'mensagem'> {
  // Fluxo do PRODUTO (traz junto as opções de "chamar pelo nome"); senão fluxo por projeto.
  const fpProduto = produtoFluxoMatch(produtoId, produtoNome);
  const fluxo = fpProduto ? fpProduto.fluxo : fluxoEntregaDoProduto(projeto, produtoId, produtoNome);
  const optsNome: OpcoesNome | undefined = fpProduto
    ? { chamarPeloNome: fpProduto.chamarPeloNome, nomeFallback: fpProduto.nomeFallback }
    : undefined;
  if (fluxo) { enfileirarFluxo(projeto, telefone, fluxo, nome, optsNome, fpProduto?.instancia || undefined); return 'fluxo'; }
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

// Igual ao preenche, mas respeita o liga/desliga de "chamar pela pessoa pelo nome".
//  - opts.chamarPeloNome === false  -> NÃO usa o nome (mesmo que exista).
//  - ligado e com nome              -> usa o nome.
//  - sem nome (ou desligado)        -> usa opts.nomeFallback; se vazio, remove o {nome}
//    e limpa o resíduo da saudação ("Olá {nome}!" -> "Olá!", "Oi {nome}," -> "Oi,").
type OpcoesNome = { chamarPeloNome?: boolean; nomeFallback?: string };
function preencheNome(tpl: string, nome?: string, opts?: OpcoesNome): string {
  const ligado = !opts || opts.chamarPeloNome !== false; // padrão: ligado
  const nm = (nome || '').trim();
  const usar = (ligado && nm) ? nm : String(opts?.nomeFallback || '').trim();
  let s = String(tpl || '').replace(/\{nome\}/g, usar);
  if (!usar) {
    s = s.replace(/\s+([!,.?:;])/g, '$1'); // tira espaço antes da pontuação: "Olá !" -> "Olá!"
    s = s.replace(/[ \t]{2,}/g, ' ');       // colapsa espaços duplos
    s = s.replace(/[ \t]+\n/g, '\n').replace(/^[ \t]+|[ \t]+$/g, '');
  }
  return s;
}

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
  if (req.path.startsWith('/v/')) return next(); // [#9 VSL] player publico (/v/:id e /v/:id.js)
  if (req.path === '/vsl/evento') return next(); // [#9 VSL] ingest de eventos do player (browser, sem x-api-key)
  if (req.path.startsWith('/vsl/media/')) return next(); // [#9 VSL upload] serve o video proprio (storage/OneDrive) pro player, sem x-api-key
  if (req.path === '/vsl/lead') return next(); // [#9 VSL camada3] gate de e-mail/WhatsApp: captura do navegador, sem x-api-key
  if (req.path === '/pub/onedrive/callback') return next(); // etapa20b: retorno do login OneDrive (Microsoft chama sem a chave)
  if (req.path === '/pub/youtube/callback') return next(); // etapa28: OAuth YouTube (Google chama sem a x-api-key)
  if (req.path === '/pub/youtube-lives/callback') return next(); // [lives-sep]: OAuth YouTube LIVES (projeto separado; Google chama sem a x-api-key)
  if (req.path === '/pub/meta/callback') return next(); // etapa28: OAuth Meta/IG/FB (Facebook chama sem a x-api-key)
  if (req.path === '/pub/pinterest/callback') return next(); // etapa28: OAuth Pinterest (chama sem a x-api-key)
  if (req.path.startsWith('/capi/webhook/')) return next(); // etapa29: webhook público de venda (Hotmart/Kiwify/Guru chamam sem x-api-key)
  if (req.path === '/track/snippet.js') return next(); // [#5 rastreio] snippet do navegador (sem x-api-key)
  if (req.path === '/track/evento') return next();     // [#5 rastreio] evento do navegador (sem x-api-key)
  if (req.path.startsWith('/media/')) return next(); // etapa21: mídia baixada do OneDrive, servida pra Evolution buscar (URL interna da rede docker)
  if (req.path.startsWith('/w/')) return next();      // [webinario] sala pública do webinário (espectadores)
  if (req.path.startsWith('/w-api/')) return next();  // [webinario] APIs públicas da sala (chat/contador/vendas)
  if (req.path.startsWith('/e/')) return next();      // [evento] pagina publica do evento multi-aula
  if (req.path.startsWith('/e-api/')) return next();  // [evento] API publica do evento (inscricao)
  if (req.path.startsWith('/c/')) return next();      // [#13 convite] pagina publica do convite (o aluno ve e agenda)
  if (req.path.startsWith('/c-api/')) return next();  // [#13 convite] API publica do convite (agendamento, sem x-api-key)
  if (req.path === '/mcp' || req.path.startsWith('/mcp/')) return next(); // [MCP] endpoint remoto: auth propria (MCP_API_KEY)
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

// ============================================================================
// [MCP] Endpoint MCP remoto (Streamable HTTP) — comandar a NUVEM de qualquer
//   sessao do Claude (PC1/PC2/PC3/celular), sem .bat e sem PC ligado. ADITIVO.
//   Auth propria (MCP_API_KEY), NAO usa a ENGINE_API_KEY. Chave no .env do motor.
//   A chave vem no header Authorization: Bearer <chave> / x-mcp-key, OU no
//   caminho /mcp/<chave> (se o conector so aceitar URL). Ferramenta: motor_call.
//   Acoes perigosas (apagar/publicar/disparar/gasto/live) exigem confirmar:true.
// ============================================================================
function _mcpChaveEsperada(): string {
  const env = String(process.env.MCP_API_KEY || '').trim();
  if (env) return env;
  try { return String(fs.readFileSync('/data/mcp-api-key.txt', 'utf8') || '').trim(); } catch { return ''; }
}
function _mcpChaveDoReq(req: any): string {
  const auth = String(req.header('authorization') || '');
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  const xk = String(req.header('x-mcp-key') || '');
  if (xk) return xk;
  if (req.params && req.params.key) return String(req.params.key);
  return '';
}
const _MCP_TOOLS = [{
  name: 'motor_call',
  description: 'Chama qualquer endpoint do motor (nuvem) do Melodias e Riffs. Use pra LER e CONSTRUIR na nuvem: paginas (/pagina), agentes, calendario, canais (/motor2/canais), stats, etc. Exemplos: criar pagina = {metodo:"POST", caminho:"/pagina", body:{...}}; ler canais = {metodo:"GET", caminho:"/motor2/canais"}. Acoes perigosas (apagar/publicar/disparar/gasto de anuncio/por live no ar) SO executam com confirmar:true.',
  inputSchema: {
    type: 'object',
    properties: {
      metodo: { type: 'string', enum: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'], description: 'Metodo HTTP' },
      caminho: { type: 'string', description: 'Caminho do endpoint, comecando com / (ex.: /pagina)' },
      body: { type: 'object', description: 'Corpo JSON para POST/PUT/PATCH (opcional)' },
      confirmar: { type: 'boolean', description: 'true para executar acoes perigosas (apagar/publicar/disparar/gasto/live)' }
    },
    required: ['metodo', 'caminho']
  }
}];
async function _mcpMotorCall(args: any): Promise<any> {
  const metodo = String((args && args.metodo) || 'GET').toUpperCase();
  let caminho = String((args && args.caminho) || '');
  if (!caminho.startsWith('/')) caminho = '/' + caminho;
  const confirmar = (args && args.confirmar) === true;
  const perigoso = metodo !== 'GET' && /(remover|delete|apagar|excluir|publicar|postar|disparar|enviar|cancelar|gasto|anuncio|trafego|habilitar|lives\/(agendar|testar|preparar|iniciar))/i.test(caminho);
  if (perigoso && !confirmar) {
    return { __erro: true, confirmacao_necessaria: true, aviso: 'Acao potencialmente perigosa: ' + metodo + ' ' + caminho + '. Reenvie com confirmar:true para executar.' };
  }
  const init: any = { method: metodo, headers: { 'x-api-key': ENGINE_API_KEY } };
  if (metodo !== 'GET' && metodo !== 'HEAD') { init.headers['content-type'] = 'application/json'; init.body = JSON.stringify((args && args.body) || {}); }
  try {
    const r = await fetch('http://127.0.0.1:' + PORT + caminho, init);
    const txt = await r.text();
    let dados: any; try { dados = JSON.parse(txt); } catch { dados = txt; }
    console.log('[mcp] ' + metodo + ' ' + caminho + ' -> ' + r.status);
    return { status: r.status, ok: r.ok, dados };
  } catch (e: any) {
    return { __erro: true, erro: String((e && e.message) || e) };
  }
}
async function _mcpHandler(req: any, res: any): Promise<void> {
  const _chaveEsp = _mcpChaveEsperada();
  if (!_chaveEsp) { res.status(503).json({ jsonrpc: '2.0', id: null, error: { code: -32000, message: 'MCP nao configurado (defina MCP_API_KEY no .env OU /data/mcp-api-key.txt)' } }); return; }
  if (_mcpChaveDoReq(req) !== _chaveEsp) { res.status(401).json({ jsonrpc: '2.0', id: (req.body && req.body.id) || null, error: { code: -32001, message: 'nao_autorizado' } }); return; }
  const msg: any = req.body || {};
  const id = (msg.id !== undefined) ? msg.id : null;
  const method = String(msg.method || '');
  try {
    if (method === 'initialize') {
      res.json({ jsonrpc: '2.0', id, result: { protocolVersion: (msg.params && msg.params.protocolVersion) || '2025-06-18', capabilities: { tools: {} }, serverInfo: { name: 'editor-video-motor', version: '1.0.0' } } });
      return;
    }
    if (method.startsWith('notifications/')) { res.status(202).end(); return; }
    if (method === 'ping') { res.json({ jsonrpc: '2.0', id, result: {} }); return; }
    if (method === 'tools/list') { res.json({ jsonrpc: '2.0', id, result: { tools: _MCP_TOOLS } }); return; }
    if (method === 'tools/call') {
      const p: any = msg.params || {};
      if (String(p.name || '') !== 'motor_call') { res.json({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: 'tool_desconhecida: ' + p.name }], isError: true } }); return; }
      const out = await _mcpMotorCall(p.arguments || {});
      res.json({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: JSON.stringify(out) }], isError: !!(out && out.__erro) } });
      return;
    }
    res.json({ jsonrpc: '2.0', id, error: { code: -32601, message: 'metodo_nao_suportado: ' + method } });
  } catch (e: any) {
    res.json({ jsonrpc: '2.0', id, error: { code: -32603, message: String((e && e.message) || e) } });
  }
}
app.post('/mcp', _mcpHandler);
app.post('/mcp/:key', _mcpHandler);
app.get('/mcp', (_req: any, res: any) => res.status(405).set('Allow', 'POST').json({ erro: 'use_post' }));
app.get('/mcp/:key', (_req: any, res: any) => res.status(405).set('Allow', 'POST').json({ erro: 'use_post' }));
// ============================================================================

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
  const item: FluxoProduto = { chave, porId: !!b.porId, projeto: b.projeto, fluxo: normalizaFluxo(b.fluxo),
    chamarPeloNome: b.chamarPeloNome !== false, nomeFallback: (b.nomeFallback != null ? String(b.nomeFallback) : undefined),
    instancia: (b.instancia != null && String(b.instancia).trim() ? String(b.instancia).trim() : undefined) };
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
  try { if (b.seo_titulo !== undefined || b.seo_descricao !== undefined || b.seo_imagem !== undefined) db.prepare(`UPDATE paginas SET seo_titulo=?, seo_descricao=?, seo_imagem=? WHERE id=?`).run(String(b.seo_titulo || ''), String(b.seo_descricao || ''), String(b.seo_imagem || ''), id); } catch { /* seo best-effort */ }
  res.json({ ok: true, id, slug, url: (PUBLIC_BASE_URL || '') + '/p/' + slug });
});
app.post('/pagina/remover', (req, res) => { db.prepare(`DELETE FROM paginas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/config/base', (_req, res) => { res.json({ ok: true, base: PUBLIC_BASE_URL || '' }); });
// [pagina-ia] Edita o HTML da página por instrução em português (usa o provedor de IA do motor).
// Recebe { html, instrucao } e devolve { ok, html } com o HTML completo já alterado.
app.post('/pagina/ia', async (req, res) => {
  try {
    const b = req.body || {};
    const html = String(b.html || '');
    const instrucao = String(b.instrucao || '').trim();
    const contexto = String(b.contexto || '').trim(); // opcional: dados extras (ex.: texto de um print que o Ezequias colou)
    if (!instrucao && !contexto) return res.json({ ok: false, erro: 'instrucao_vazia' });
    const sys = 'Você é um editor de páginas HTML de alta conversão (estilo landing page). Recebe o HTML atual e uma instrução em português e devolve APENAS o HTML completo já alterado — sem explicação, sem comentário, sem markdown e sem cercas de código (nada de ```html). Preserve tudo que não foi pedido para mudar: mantenha os formulários, os links de /f/ e /p/, os scripts e os estilos existentes. Se for pedido para criar algo novo, use HTML/CSS inline coerente com o que já existe. Responda somente com o HTML final.';
    const user = `HTML atual:\n\n${html || '(vazio — crie a página do zero conforme a instrução)'}\n\n---\n${contexto ? 'Dados/contexto fornecidos:\n' + contexto + '\n\n---\n' : ''}Instrução: ${instrucao}\n\nDevolva o HTML completo alterado.`;
    const msg = await _chamarLLM([{ role: 'system', content: sys }, { role: 'user', content: user }], '', 0.2, []);
    if (msg && (msg as any)._semChave) return res.json({ ok: false, erro: 'LLM sem chave (configure o provedor de IA)' });
    let out = String((msg && msg.content) || '').trim();
    out = out.replace(/^```(?:html)?\s*/i, '').replace(/\s*```$/i, '').trim(); // tira cercas se vierem
    if (!out) return res.json({ ok: false, erro: 'ia_devolveu_vazio' });
    res.json({ ok: true, html: out });
  } catch (e: any) { res.json({ ok: false, erro: String((e && e.message) || e) }); }
});
// publico: serve a pagina publicada
// [pagina-render] embrulha o HTML (fragmento) num documento com Tailwind + SEO. Se ja for doc completo, nao mexe.
function _wrapPagina(html: string, p: any): string {
  const h = String(html || '');
  if (/<html[\s>]/i.test(h) || /<!doctype/i.test(h)) return h;
  const e = (x: any) => String(x == null ? '' : x).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' } as any)[c]);
  const t = e((p && (p.seo_titulo || p.titulo)) || 'Pagina');
  const d = e((p && p.seo_descricao) || '');
  const img = e((p && p.seo_imagem) || '');
  const og = (d || img) ? `<meta property="og:title" content="${t}"><meta property="og:type" content="website">${d ? `<meta property="og:description" content="${d}">` : ''}${img ? `<meta property="og:image" content="${img}">` : ''}<meta name="twitter:card" content="summary_large_image">` : '';
  return `<!doctype html><html lang="pt"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${t}</title>${d ? `<meta name="description" content="${d}">` : ''}${og}<script src="https://cdn.tailwindcss.com"></script><style>*{box-sizing:border-box}body{margin:0;font-family:system-ui,Segoe UI,Arial,sans-serif}img{max-width:100%}</style></head><body>${h}</body></html>`;
}
app.get('/p/:slug', (req, res) => {
  const p = db.prepare(`SELECT * FROM paginas WHERE slug=? AND publicada=1`).get(req.params.slug) as any;
  if (!p) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Pagina nao encontrada</h1>');
  res.type('html').send(_wrapPagina(String(p.html || ''), p));
});

// ============================================================================
// [IMPORTADOR] POST /pagina/importar — traz uma pagina montada no desktop/cloud
//   PRA DENTRO do sistema, 100% fiel, ja integrada: rastreio (pixel/CAPI via
//   /track), player de VSL (metricas) e formularios (lead+tag+automacao+Lead).
//   A integracao entra por UM <script> injetado (wiring no navegador) — NAO mexe
//   no visual. Marcadores no HTML (voce poe ao montar a pagina aqui comigo):
//     video:  <div data-mr-vsl="oferta"></div>             -> vira player de VSL
//     form:   <form data-mr-form="principal"> ... </form>   -> salva lead no sistema
//     evento: <button data-mr-evento="Lead"> ...            -> dispara evento rastreado
//   Body JSON:
//     { projeto, slug?, titulo?, html, publicada?(=true), rastreio?(=true),
//       seo?:{titulo,descricao,imagem},
//       vsl?:[ { chave, id? | criar:{nome,fonte,ref,oferta_segundos,formato,barra,dominios,marcadagua} } ],
//       forms?:[ { chave, id? | titulo?,tag?,redirecionar?,campos?:[{rotulo,chave,tipo,obrigatorio}] } ] }
// ============================================================================
const _MR_INTEGRA_JS = `(function(){
  var el=document.getElementById('mr-integra'); if(!el) return;
  var cfg={}; try{cfg=JSON.parse(el.getAttribute('data-cfg')||'{}');}catch(e){return;}
  var base=cfg.base||'';
  if(cfg.rastreio){ try{ var s=document.createElement('script'); s.src=base+'/track/snippet.js?projeto='+encodeURIComponent(cfg.projeto||''); document.head.appendChild(s); }catch(e){} }
  function track(ev,p){ p=p||{}; var n=0; (function go(){ try{ if(window.mrTrack){ window.mrTrack(ev,p); return; } }catch(e){} if(n++<50) setTimeout(go,120); })(); }
  function run(){
    var vsl=cfg.vsl||{}; Object.keys(vsl).forEach(function(k){ var id=vsl[k]; var ns=document.querySelectorAll('[data-mr-vsl="'+k+'"]'); for(var i=0;i<ns.length;i++){ (function(box){ try{ box.innerHTML=''; var sc=document.createElement('script'); sc.src=base+'/v/'+id+'.js'; box.appendChild(sc); }catch(e){} })(ns[i]); } });
    var evs=document.querySelectorAll('[data-mr-evento]'); for(var j=0;j<evs.length;j++){ (function(bt){ bt.addEventListener('click',function(){ track(bt.getAttribute('data-mr-evento')||'Lead',{}); }); })(evs[j]); }
    var forms=cfg.forms||{}; Object.keys(forms).forEach(function(k){ var fm=forms[k]; var fs2=document.querySelectorAll('form[data-mr-form="'+k+'"]'); for(var i=0;i<fs2.length;i++){ (function(f){ f.addEventListener('submit',function(e){ e.preventDefault(); var d={}; new FormData(f).forEach(function(v,n){ d[n]=v; }); fetch(base+'/f/'+fm.id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}).then(function(r){ return r.json().catch(function(){return {};}); }).then(function(){ track('Lead',{content_name:(cfg.projeto||'')}); var red=fm.redirecionar||f.getAttribute('data-mr-redir')||''; if(red){ setTimeout(function(){ location.href=red; },800); } else { var ok=f.getAttribute('data-mr-ok'); if(ok){ var e2=document.querySelector(ok); if(e2) e2.style.display=''; } f.style.display='none'; } }).catch(function(){}); }); })(fs2[i]); } });
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',run); else run();
})();`;
app.post('/pagina/importar', (req: any, res: any) => {
  try {
    const b = req.body || {};
    let html = String(b.html || '');
    if (!html.trim()) return res.status(400).json({ ok: false, erro: 'html_vazio' });
    const projeto = String(b.projeto || 'teclado');
    const base = PUBLIC_BASE_URL || '';
    let slug = String(b.slug || '').toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '');
    let id = String(b.id || '').trim();
    if (!id && slug) { const ex = db.prepare(`SELECT id FROM paginas WHERE slug=?`).get(slug) as any; if (ex && ex.id) id = String(ex.id); }
    if (!id) id = randomUUID();
    if (!slug) slug = id.slice(0, 8);

    // 1) VSLs: cria (criar:{...}) ou linka (id) -> mapa chave->id
    const vslMap: any = {}; const vslOut: any[] = [];
    if (Array.isArray(b.vsl)) for (const v of b.vsl) {
      const chave = String((v && v.chave) || '').trim(); if (!chave) continue;
      let vid = String((v && v.id) || '').trim();
      if (!vid && v && v.criar) {
        vid = randomUUID(); const c = v.criar || {};
        try {
          db.prepare(`INSERT OR REPLACE INTO vsl_videos (id,projeto,nome,fonte,ref,oferta_segundos,barra,dominios,marcadagua,formato,criado_em) VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT criado_em FROM vsl_videos WHERE id=?),?))`)
            .run(vid, projeto, String(c.nome || 'VSL'), String(c.fonte || 'youtube'), String(c.ref || ''), (c.oferta_segundos != null ? (Number(c.oferta_segundos) || 0) : 0), String(c.barra || 'inteligente'), String(c.dominios || ''), (c.marcadagua ? 1 : 0), String(c.formato || '16:9'), vid, agora());
        } catch { /* best-effort */ }
      }
      if (vid) { vslMap[chave] = vid; vslOut.push({ chave, id: vid, player: base + '/v/' + vid }); }
    }

    // 2) Formularios: cria ou linka -> mapa chave->{id,redirecionar}
    const formMap: any = {}; const formOut: any[] = [];
    if (Array.isArray(b.forms)) for (const f of b.forms) {
      const chave = String((f && f.chave) || '').trim(); if (!chave) continue;
      let fid = String((f && f.id) || '').trim();
      if (!fid) {
        fid = randomUUID();
        try {
          const campos = JSON.stringify(Array.isArray(f.campos) ? f.campos : []);
          db.prepare(`INSERT INTO formularios(id,projeto,titulo,campos,tag,redirecionar,criado_em) VALUES(?,?,?,?,?,?,?)`)
            .run(fid, projeto, String(f.titulo || b.titulo || 'Formulario'), campos, String(f.tag || ''), String(f.redirecionar || ''), agora());
        } catch { /* best-effort */ }
      }
      formMap[chave] = { id: fid, redirecionar: String((f && f.redirecionar) || '') };
      formOut.push({ chave, id: fid, url: base + '/f/' + fid });
    }

    // 3) Script integrador (wiring no navegador; nao altera o visual)
    const rastreio = b.rastreio !== false;
    const cfg = { base, projeto, rastreio, vsl: vslMap, forms: formMap };
    const cfgAttr = JSON.stringify(cfg).replace(/</g, '\\u003c').replace(/'/g, '&#39;');
    const loader = '<script id="mr-integra" data-cfg=\'' + cfgAttr + '\'>' + _MR_INTEGRA_JS + '</scr' + 'ipt>';
    if (/<\/body>/i.test(html)) html = html.replace(/<\/body>/i, loader + '</body>'); else html = html + loader;

    // 4) Salva/publica (reusa tabela paginas)
    const publicada = b.publicada === false ? 0 : 1;
    const existe = db.prepare(`SELECT id FROM paginas WHERE id=?`).get(id);
    try {
      if (existe) db.prepare(`UPDATE paginas SET slug=?, titulo=?, html=?, publicada=?, atualizado_em=? WHERE id=?`).run(slug, String(b.titulo || ''), html, publicada, agora(), id);
      else db.prepare(`INSERT INTO paginas(id,projeto,slug,titulo,html,publicada,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`).run(id, projeto, slug, String(b.titulo || ''), html, publicada, agora(), agora());
    } catch (e: any) { return res.status(400).json({ ok: false, erro: 'slug_duplicado_ou_invalido', detalhe: String((e && e.message) || e) }); }
    try { if (b.seo) db.prepare(`UPDATE paginas SET seo_titulo=?, seo_descricao=?, seo_imagem=? WHERE id=?`).run(String(b.seo.titulo || ''), String(b.seo.descricao || ''), String(b.seo.imagem || ''), id); } catch { /* seo best-effort */ }

    res.json({ ok: true, id, slug, url: base + '/p/' + slug, publicada: !!publicada, rastreio, vsl: vslOut, forms: formOut });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String((e && e.message) || e) }); }
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

// ---- E-MAIL: templates, dominios (SMTP) e fila (ENVIO REAL ligado) ----
// [email] colunas add-only: senha SMTP criptografada + seguranca + controle da fila
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN smtp_senha TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN smtp_seg TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_fila ADD COLUMN erro TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_fila ADD COLUMN tentativas INTEGER DEFAULT 0`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_fila ADD COLUMN enviado_em TEXT`); } catch { /* existe */ }
// [email-warmup] remetente escolhivel + aquecimento
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN apelido TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN ativo INTEGER DEFAULT 1`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN padrao INTEGER DEFAULT 0`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN warmup_on INTEGER DEFAULT 0`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN warmup_inicio TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_dominios ADD COLUMN warmup_max INTEGER DEFAULT 200`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE email_fila ADD COLUMN dominio_id TEXT`); } catch { /* existe */ }

// [email] enviador SMTP self-contido (net/tls do Node, sem dependencia nova). Suporta
// TLS implicito (465) e STARTTLS (587), AUTH LOGIN. Usado pelo worker da fila e pelo teste.
function _mimeEnc(s: string): string { s = String(s || ''); return /^[\x00-\x7F]*$/.test(s) ? s : '=?UTF-8?B?' + Buffer.from(s, 'utf8').toString('base64') + '?='; }
function _smtpConn(sock: any): { wait: () => Promise<any>; send: (l: string) => void } {
  let buffer = ''; let resolver: any = null; let rejecter: any = null;
  const settleErr = (e: any) => { const rj = rejecter; resolver = null; rejecter = null; if (rj) rj(e instanceof Error ? e : new Error(String(e))); };
  sock.on('data', (d: any) => {
    buffer += d.toString('utf8');
    const lines = buffer.split(/\r?\n/).filter((x: string) => x.length);
    const last = lines[lines.length - 1] || '';
    if (/^\d{3} /.test(last)) { const r = { code: Number(last.slice(0, 3)), text: buffer.trim() }; buffer = ''; const fn = resolver; resolver = null; rejecter = null; if (fn) fn(r); }
  });
  sock.on('error', (e: any) => settleErr(e));
  sock.on('close', () => settleErr(new Error('conexao SMTP fechada')));
  return { wait: () => new Promise((res, rej) => { resolver = res; rejecter = rej; }), send: (l: string) => { try { sock.write(l + '\r\n'); } catch (e) { settleErr(e); } } };
}
async function _smtpEnviar(dom: any, para: string, assunto: string, html: string): Promise<void> {
  const host = String(dom.smtp_host || '').trim();
  const port = Number(dom.smtp_porta) || 587;
  const user = String(dom.smtp_usuario || '').trim();
  const pass = decifra(String(dom.smtp_senha || ''));
  if (!host || !user || !pass) throw new Error('SMTP incompleto (host/usuario/senha)');
  let fromAddr = user; let fromName = 'Melodias e Riffs';
  const rem = String(dom.remetente || '').trim();
  if (rem) { const m = rem.match(/<([^>]+)>/); if (m) { fromAddr = m[1]; const nm = rem.replace(/<[^>]+>/, '').trim().replace(/^"|"$/g, '').trim(); if (nm) fromName = nm; } else if (rem.includes('@')) { fromAddr = rem; } }
  const implicito = port === 465 || String(dom.smtp_seg || '') === 'tls';
  let sock: any = implicito ? tls.connect({ host, port, servername: host, rejectUnauthorized: false }) : net.connect({ host, port });
  sock.setTimeout(20000, () => { try { sock.destroy(new Error('timeout SMTP')); } catch { /* */ } });
  await new Promise<void>((res, rej) => { sock.once(implicito ? 'secureConnect' : 'connect', () => res()); sock.once('error', rej); });
  let conn = _smtpConn(sock); let r = await conn.wait(); if (r.code !== 220) throw new Error('greeting: ' + r.text);
  conn.send('EHLO melodiaseriffs.com.br'); r = await conn.wait();
  if (!implicito) {
    conn.send('STARTTLS'); r = await conn.wait(); if (r.code !== 220) throw new Error('STARTTLS: ' + r.text);
    sock = tls.connect({ socket: sock, servername: host, rejectUnauthorized: false });
    await new Promise<void>((res, rej) => { sock.once('secureConnect', () => res()); sock.once('error', rej); });
    conn = _smtpConn(sock); conn.send('EHLO melodiaseriffs.com.br'); r = await conn.wait();
  }
  conn.send('AUTH LOGIN'); r = await conn.wait(); if (r.code !== 334) throw new Error('AUTH: ' + r.text);
  conn.send(Buffer.from(user, 'utf8').toString('base64')); r = await conn.wait(); if (r.code !== 334) throw new Error('usuario: ' + r.text);
  conn.send(Buffer.from(pass, 'utf8').toString('base64')); r = await conn.wait(); if (r.code !== 235) throw new Error('login falhou (usuario/senha): ' + r.text);
  conn.send('MAIL FROM:<' + fromAddr + '>'); r = await conn.wait(); if (r.code !== 250) throw new Error('MAIL FROM: ' + r.text);
  conn.send('RCPT TO:<' + para + '>'); r = await conn.wait(); if (r.code !== 250 && r.code !== 251) throw new Error('RCPT TO: ' + r.text);
  conn.send('DATA'); r = await conn.wait(); if (r.code !== 354) throw new Error('DATA: ' + r.text);
  const corpo = [
    'From: ' + _mimeEnc(fromName) + ' <' + fromAddr + '>',
    'To: ' + para,
    'Subject: ' + _mimeEnc(assunto),
    'MIME-Version: 1.0',
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
    '', String(html || '').replace(/\r?\n/g, '\r\n').replace(/^\./gm, '..'), '.'
  ].join('\r\n');
  conn.send(corpo); r = await conn.wait(); if (r.code !== 250) throw new Error('envio recusado: ' + r.text);
  try { conn.send('QUIT'); } catch { /* */ } try { sock.end(); } catch { /* */ }
}
// [email-warmup] limite de envios de HOJE do remetente (aquecimento gradual). Sem aquecimento = sem limite.
function _emailCapHoje(dom: any): number {
  if (!dom || !Number(dom.warmup_on)) return Number.MAX_SAFE_INTEGER;
  const ini = String(dom.warmup_inicio || '').trim();
  if (!ini) return 20;
  const base = Date.parse(ini.length <= 10 ? ini + 'T00:00:00-03:00' : ini);
  if (!base) return 20;
  const dias = Math.max(0, Math.floor((Date.now() - base) / 86400000));
  return Math.min(Number(dom.warmup_max) || 200, 20 + dias * 30); // dia0=20, +30/dia
}
function _emailEnviadosHoje(dominioId: string): number {
  try {
    const nowBRT = new Date(Date.now() - 3 * 3600000);
    const diaBRT = `${nowBRT.getUTCFullYear()}-${String(nowBRT.getUTCMonth() + 1).padStart(2, '0')}-${String(nowBRT.getUTCDate()).padStart(2, '0')}`;
    const limiteUtc = new Date(Date.parse(diaBRT + 'T00:00:00-03:00')).toISOString();
    const r = db.prepare(`SELECT COUNT(*) n FROM email_fila WHERE dominio_id=? AND status='enviado' AND enviado_em>=?`).get(dominioId, limiteUtc) as any;
    return (r && r.n) || 0;
  } catch { return 0; }
}
// escolhe o remetente de um item: o escolhido (dominio_id) -> o PADRAO do projeto -> o mais recente
function _emailDomDoItem(m: any): any {
  const okSmtp = `smtp_host IS NOT NULL AND smtp_host<>'' AND smtp_senha IS NOT NULL AND smtp_senha<>''`;
  if (m && m.dominio_id) { const d = db.prepare(`SELECT * FROM email_dominios WHERE id=? AND ${okSmtp}`).get(String(m.dominio_id)) as any; if (d) return d; }
  const pad = db.prepare(`SELECT * FROM email_dominios WHERE projeto=? AND padrao=1 AND ${okSmtp} ORDER BY criado_em DESC LIMIT 1`).get(String(m.projeto || '')) as any;
  if (pad) return pad;
  return db.prepare(`SELECT * FROM email_dominios WHERE projeto=? AND ${okSmtp} ORDER BY criado_em DESC LIMIT 1`).get(String(m.projeto || '')) as any;
}
async function _tickEmail(): Promise<void> {
  try {
    const rows = db.prepare(`SELECT * FROM email_fila WHERE status='pendente' ORDER BY criado_em ASC LIMIT 5`).all() as any[];
    for (const m of rows) {
      const dom = _emailDomDoItem(m);
      if (!dom) { try { db.prepare(`UPDATE email_fila SET erro=? WHERE id=?`).run('aguardando SMTP do projeto', m.id); } catch { /* */ } continue; }
      if (Number(dom.warmup_on) && _emailEnviadosHoje(String(dom.id)) >= _emailCapHoje(dom)) { try { db.prepare(`UPDATE email_fila SET erro=? WHERE id=?`).run('aguardando aquecimento (limite diario do remetente atingido)', m.id); } catch { /* */ } continue; }
      try {
        await _smtpEnviar(dom, String(m.para), String(m.assunto || ''), String(m.html || ''));
        db.prepare(`UPDATE email_fila SET status='enviado', enviado_em=?, erro=NULL, dominio_id=? WHERE id=?`).run(agora(), String(dom.id), m.id);
      } catch (e: any) {
        const t = (Number(m.tentativas) || 0) + 1; const st = t >= 5 ? 'erro' : 'pendente';
        db.prepare(`UPDATE email_fila SET status=?, tentativas=?, erro=? WHERE id=?`).run(st, t, String(e?.message || e), m.id);
      }
    }
  } catch { /* best-effort */ }
}
setInterval(() => { void _tickEmail(); }, 20000);
console.log('[email] enviador SMTP + worker da fila carregados');

app.get('/email/templates', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, templates: db.prepare(`SELECT * FROM email_templates WHERE projeto=? ORDER BY nome ASC`).all(projeto) }); });
app.post('/email/template', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const existe = db.prepare(`SELECT id FROM email_templates WHERE id=?`).get(id);
  if (existe) db.prepare(`UPDATE email_templates SET nome=?, assunto=?, html=? WHERE id=?`).run(String(b.nome || ''), String(b.assunto || ''), String(b.html || ''), id);
  else db.prepare(`INSERT INTO email_templates(id,projeto,nome,assunto,html,criado_em) VALUES(?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.nome || ''), String(b.assunto || ''), String(b.html || ''), agora());
  res.json({ ok: true, id });
});
app.post('/email/template/remover', (req, res) => { db.prepare(`DELETE FROM email_templates WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/email/dominios', (req, res) => { const projeto = String(req.query.projeto || ''); const rows = (db.prepare(`SELECT * FROM email_dominios WHERE projeto=? ORDER BY dominio ASC`).all(projeto) as any[]).map((d) => { const tem_senha = !!(d.smtp_senha && String(d.smtp_senha).length); delete d.smtp_senha; d.tem_senha = tem_senha; d.capHoje = Number(d.warmup_on) ? _emailCapHoje(d) : null; d.enviadosHoje = _emailEnviadosHoje(String(d.id)); d.diaWarmup = (Number(d.warmup_on) && d.warmup_inicio) ? (Math.max(0, Math.floor((Date.now() - Date.parse(String(d.warmup_inicio).length <= 10 ? d.warmup_inicio + 'T00:00:00-03:00' : d.warmup_inicio)) / 86400000)) + 1) : 0; return d; }); res.json({ ok: true, dominios: rows }); });
app.post('/email/dominio', (req, res) => {
  const b = req.body || {}; const id = String(b.id || randomUUID());
  const seg = String(b.smtp_seg || '').trim();
  const senhaCif = (b.smtp_senha && String(b.smtp_senha).trim()) ? cifra(String(b.smtp_senha)) : null; // só grava se veio senha nova
  const existe = db.prepare(`SELECT id FROM email_dominios WHERE id=?`).get(id);
  if (existe) {
    if (senhaCif) db.prepare(`UPDATE email_dominios SET dominio=?, smtp_host=?, smtp_porta=?, smtp_usuario=?, remetente=?, smtp_seg=?, smtp_senha=? WHERE id=?`).run(String(b.dominio || ''), String(b.smtp_host || ''), Number(b.smtp_porta || 587), String(b.smtp_usuario || ''), String(b.remetente || ''), seg, senhaCif, id);
    else db.prepare(`UPDATE email_dominios SET dominio=?, smtp_host=?, smtp_porta=?, smtp_usuario=?, remetente=?, smtp_seg=? WHERE id=?`).run(String(b.dominio || ''), String(b.smtp_host || ''), Number(b.smtp_porta || 587), String(b.smtp_usuario || ''), String(b.remetente || ''), seg, id);
  } else {
    db.prepare(`INSERT INTO email_dominios(id,projeto,dominio,smtp_host,smtp_porta,smtp_usuario,remetente,smtp_seg,smtp_senha,criado_em) VALUES(?,?,?,?,?,?,?,?,?,?)`).run(id, String(b.projeto || ''), String(b.dominio || ''), String(b.smtp_host || ''), Number(b.smtp_porta || 587), String(b.smtp_usuario || ''), String(b.remetente || ''), seg, senhaCif, agora());
  }
  res.json({ ok: true, id });
});
// [email-warmup] config do remetente: apelido, ativo, PADRAO do projeto, aquecimento on/off + limite alvo
app.post('/email/dominio/config', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || ''); if (!id) return res.json({ ok: false, erro: 'sem id' });
    const dom = db.prepare(`SELECT projeto, warmup_inicio FROM email_dominios WHERE id=?`).get(id) as any;
    if (!dom) return res.json({ ok: false, erro: 'nao_encontrado' });
    if (b.apelido !== undefined) db.prepare(`UPDATE email_dominios SET apelido=? WHERE id=?`).run(String(b.apelido || ''), id);
    if (b.ativo !== undefined) db.prepare(`UPDATE email_dominios SET ativo=? WHERE id=?`).run(b.ativo ? 1 : 0, id);
    if (b.warmup_max !== undefined) db.prepare(`UPDATE email_dominios SET warmup_max=? WHERE id=?`).run(Number(b.warmup_max) || 200, id);
    if (b.warmup_on !== undefined) {
      const won = b.warmup_on ? 1 : 0;
      const ini = won ? (dom.warmup_inicio || agora()) : dom.warmup_inicio || null; // comeca a contar quando liga
      db.prepare(`UPDATE email_dominios SET warmup_on=?, warmup_inicio=? WHERE id=?`).run(won, ini, id);
    }
    if (b.padrao) { db.prepare(`UPDATE email_dominios SET padrao=0 WHERE projeto=?`).run(String(dom.projeto || '')); db.prepare(`UPDATE email_dominios SET padrao=1 WHERE id=?`).run(id); }
    res.json({ ok: true });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});
// [email] teste de envio: valida o SMTP mandando um e-mail de verdade
app.post('/email/teste', async (req, res) => {
  try {
    const b = req.body || {}; const projeto = String(b.projeto || ''); const para = String(b.para || '').trim();
    if (!para) return res.json({ ok: false, erro: 'Informe o e-mail de destino do teste.' });
    const dom = b.dominioId
      ? db.prepare(`SELECT * FROM email_dominios WHERE id=?`).get(String(b.dominioId)) as any
      : db.prepare(`SELECT * FROM email_dominios WHERE projeto=? AND smtp_host<>'' ORDER BY criado_em DESC LIMIT 1`).get(projeto) as any;
    if (!dom) return res.json({ ok: false, erro: 'Nenhum domínio SMTP configurado.' });
    if (!dom.smtp_senha) return res.json({ ok: false, erro: 'Senha SMTP não configurada.' });
    await _smtpEnviar(dom, para, 'Teste de e-mail — Melodias e Riffs ✅', '<div style="font:16px/1.5 system-ui,Arial;color:#111"><h2>Funcionou! ✅</h2><p>Seu SMTP está configurado certinho. Já dá pra enviar as confirmações por e-mail.</p></div>');
    res.json({ ok: true });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});
app.post('/email/dominio/remover', (req, res) => { db.prepare(`DELETE FROM email_dominios WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); });
app.get('/email/fila', (req, res) => { const projeto = String(req.query.projeto || ''); res.json({ ok: true, fila: db.prepare(`SELECT * FROM email_fila WHERE projeto=? ORDER BY criado_em DESC LIMIT 300`).all(projeto) }); });
app.post('/email/enviar', (req, res) => {
  const b = req.body || {};
  let assunto = String(b.assunto || ''); let html = String(b.html || '');
  if (b.templateId) { const t = db.prepare(`SELECT * FROM email_templates WHERE id=?`).get(String(b.templateId)) as any; if (t) { assunto = assunto || t.assunto; html = html || t.html; } }
  db.prepare(`INSERT INTO email_fila(id,projeto,para,assunto,html,status,criado_em,dominio_id) VALUES(?,?,?,?,?,?,?,?)`).run(randomUUID(), String(b.projeto || ''), String(b.para || ''), assunto, html, 'pendente', agora(), b.dominio_id ? String(b.dominio_id) : null);
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

// ======================= [motor2] Trilha por tag + bump/upsell + gate do agente =======================
// Tudo aditivo e INERTE por padrão. O fluxo de compra/entrega antigo continua igual; os blocos novos
// só agem quando pubFlag('motor2_ativo')==='1'. O gate do agente (8151) é LIGADO por padrão (seguro):
// o agente só responde a lead com tag gatilho; sem tag, fica na fila pra humano.

/** [motor2] Extrai TODOS os itens de uma compra (produto principal + order bump + upsell). */
function _itensPagamento(provider: string, b: any): { produtoId: string; produtoNome: string; valor: number }[] {
  const out: { produtoId: string; produtoNome: string; valor: number }[] = [];
  const push = (id: any, nome: any, valor: any) => {
    const pid = String(id || '').trim(); const pn = String(nome || '').trim();
    if (!pid && !pn) return;
    if (out.some((x) => (pid && x.produtoId === pid) || (pn && x.produtoNome.toLowerCase() === pn.toLowerCase()))) return; // dedupe
    out.push({ produtoId: pid, produtoNome: pn, valor: Number(valor || 0) });
  };
  try {
    if (provider === 'lastlink') {
      const d = b?.Data || b?.data || b;
      const arr = d?.Products || d?.products || [];
      for (const pr of (Array.isArray(arr) ? arr : [])) push(pr?.Id || pr?.id, pr?.Name || pr?.name, pr?.Price || pr?.price);
      for (const k of ['Bumps', 'bumps', 'OrderBumps', 'orderBumps', 'Upsell', 'upsell', 'Upsells', 'upsells']) {
        const ex = d?.[k]; if (Array.isArray(ex)) for (const pr of ex) push(pr?.Id || pr?.id || pr?.ProductId, pr?.Name || pr?.name, pr?.Price || pr?.price);
      }
    } else {
      const arr = b?.items || b?.products || [];
      for (const pr of (Array.isArray(arr) ? arr : [])) push(pr?.id || pr?.marketplace_id || pr?.product_id, pr?.name, pr?.total_value || pr?.unit_value);
      const prod = b?.product; if (prod) push(prod?.id || prod?.marketplace_id, prod?.name, b?.payment?.total);
      for (const k of ['bumps', 'order_bumps', 'orderBumps', 'upsell', 'upsells']) {
        const ex = b?.[k]; if (Array.isArray(ex)) for (const pr of ex) push(pr?.id || pr?.marketplace_id, pr?.name, pr?.total_value || pr?.unit_value);
      }
    }
  } catch { /* best-effort */ }
  return out;
}

/** [motor2] Tags da trilha configuradas pro produto (casa por id/nome, igual à entrega). */
function _produtoTags(produtoId?: string, produtoNome?: string): string[] {
  const alvo = `${produtoId || ''} ${produtoNome || ''}`.toLowerCase();
  const id = String(produtoId || '').toLowerCase();
  const out: string[] = [];
  for (const pt of ((CONFIG as any).produtoTags || []) as { chave: string; porId?: boolean; tags: string[] }[]) {
    const chave = String(pt?.chave || '').toLowerCase(); if (!chave) continue;
    const casa = pt?.porId ? (id && id === chave) : alvo.includes(chave);
    if (casa && Array.isArray(pt.tags)) for (const t of pt.tags) { const tn = String(t || '').trim(); if (tn && !out.includes(tn)) out.push(tn); }
  }
  return out;
}

/** [motor2] Lista de tags-gatilho (padrão do código + CONFIG.agenteTagsGatilho). */
const _AGENTE_TAGS_GATILHO_PADRAO = ['low-ticket', 'lowticket', 'grupo-vip', 'qualificar', 'qualificacao', 'qualificação'];
function _agenteTagsGatilho(): string[] {
  const extra = Array.isArray((CONFIG as any).agenteTagsGatilho) ? (CONFIG as any).agenteTagsGatilho.map((s: any) => String(s).toLowerCase()) : [];
  return [..._AGENTE_TAGS_GATILHO_PADRAO, ...extra];
}
/** [motor2] O contato tem alguma tag que autoriza o agente? (casa por pedaço, minúsculas) */
function _contatoTemTagGatilho(projeto: string, contatoId: string): boolean {
  try {
    const tags = db.prepare(`SELECT t.nome FROM contato_tags ct JOIN tags t ON t.id=ct.tag_id WHERE ct.contato_id=? AND t.projeto=?`).all(contatoId, projeto) as any[];
    const alvo = _agenteTagsGatilho();
    for (const row of tags) { const n = String(row?.nome || '').toLowerCase(); if (alvo.some((g) => g && n.includes(g))) return true; }
  } catch { /* best-effort */ }
  return false;
}
/** [motor2] O agente PODE responder a esse contato? Gate LIGADO (padrão) exige tag-gatilho. */
function _agenteDevResponder(projeto: string, contatoId: string): boolean {
  if (pubFlag('agente_tag_gate', '1') !== '1') return true; // gate desligado => comportamento antigo (responde a todos)
  return _contatoTemTagGatilho(projeto, contatoId);
}
// [#2-guard] Tags que marcam ALUNO (regra do prompt: nunca qualificar/atender quem já é aluno). Confere SÓ no projeto atual.
const _ALUNO_TAG_PADROES = ['grupo-de-alunos', 'black-alunos', 'alunos-desafio-hc', 'desafio-hc-alunos'];
function _contatoEhAluno(projeto: string, contatoId: string): boolean {
  try {
    const tags = db.prepare(`SELECT t.nome FROM contato_tags ct JOIN tags t ON t.id=ct.tag_id WHERE ct.contato_id=? AND t.projeto=?`).all(contatoId, projeto) as any[];
    for (const row of tags) {
      const n = String(row?.nome || '').toLowerCase().trim();
      if (!n) continue;
      if (n === 'alunos' || n === 'aluno' || n.startsWith('aluno-') || /^t\d+-aluno/.test(n) || _ALUNO_TAG_PADROES.some((p) => n.includes(p))) return true;
    }
  } catch { /* best-effort */ }
  return false;
}
// [#2-guard] É um dos NOSSOS próprios números conectados? (evita o agente engajar a si mesmo = eco/loop entre instâncias)
function _ehNumeroNosso(telefone: string): boolean {
  try {
    const t = String(telefone || '').replace(/\D/g, '');
    if (!t) return false;
    const rows = db.prepare(`SELECT telefone FROM numeros`).all() as any[];
    return rows.some((r) => String(r?.telefone || '').replace(/\D/g, '') === t);
  } catch { return false; }
}

// ---- Webhook da Evolution: mensagem recebida -> salva + auto-tag ----
app.post('/webhook/evolution', (req, res) => {
  try {
    const ev = req.body;
    const data = ev?.data;
    if (ev?.event === 'messages.upsert' && data) {
      const projeto = projetoDaInstancia(String(ev.instance || ''));
      const telefone = String(data.key?.remoteJid || '').replace(/@.*/, '');
      const ehGrupo = String(data.key?.remoteJid || '').includes('@g.us'); // etapa19c: NUNCA responder em GRUPOS (evita flood) — agente/automações só 1-a-1
      const texto = data.message?.conversation || data.message?.extendedTextMessage?.text || '';
      if (telefone && !data.key?.fromMe) {
        const cid = upsertContato(projeto, telefone, data.pushName);
        try { db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,instancia,criado_em) VALUES(?,?,?,?,?,?,?)`).run(randomUUID(), projeto, cid, 'entrada', texto, String(ev.instance||''), agora()); }
        catch { db.prepare(`INSERT INTO mensagens(id,projeto,contato_id,direcao,texto,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), projeto, cid, 'entrada', texto, agora()); }
        // Automacoes: gatilho "mensagem_recebida" (palavra-chave opcional na condicao).
        if (!ehGrupo) rodarAutomacoes(projeto, 'mensagem_recebida', { contatoId: cid, telefone, nome: data.pushName, texto }).catch(() => {});
        try { if (!ehGrupo && ((CONFIG as any).agentesHabilitados || {})[projeto] && (_agenteDevResponder(projeto, cid) || _temAgenteAtribuido(cid)) && !_contatoEhAluno(projeto, cid) && !_ehNumeroNosso(telefone)) processarMensagemAgente(projeto, telefone, texto, data.pushName, cid, { instancia: String(ev.instance || '') }).catch(() => {}); } catch { /* agente IA best-effort, etapa14 — e etapa19c: nunca em grupo — [motor2] + gate por tag (8151): sem tag-gatilho fica na fila (ou agente atribuído manual/handoff); [#2-guard] nunca atende aluno nem número nosso; responde pela MESMA instância que recebeu */ }
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

// [etapa31] gancho do CAPI: é preenchido dentro do bloco CAPI (etapa29) no boot.
// Serve pra espelhar a MESMA venda que chega pela entrega (Guru/LastLink) no pixel Meta,
// sem precisar criar webhook novo. Fica null até o bloco CAPI rodar no startup.
let capiHookPagamento: ((fonte: string, dados: { email?: string | null; telefone?: string | null; valorCentavos?: number | null; produto?: string | null; produtoId?: string | null; evento?: string }) => Promise<void>) | null = null;
// [#9 VSL camada2 batch B] gancho de rastreio por MARCO do player de VSL (play/oferta/cta_click ->
// Meta CAPI + GA4, reusa track_config do projeto). Preenchido no bloco CAPI (etapa29) no boot.
// Inerte até pubFlag('vsl_capi_ativo')='1'.
let vslTrackHook: ((projeto: string, eventName: string, dados: { email?: string | null; telefone?: string | null; pagina?: string | null }) => Promise<void>) | null = null;

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
    // [etapa31] ADD: espelha a venda no CAPI (pixel Meta) reusando ESTE webhook de entrega.
    // Best-effort e envolto em try/catch: NUNCA pode quebrar a entrega. Só Purchase (aprovado)
    // e InitiateCheckout (perdido). Enquanto capi_ativo=0 no motor, isto só grava log 'inerte'.
    try {
      const _capiEv = categoria === 'aprovado' ? 'Purchase' : categoria === 'perdido' ? 'InitiateCheckout' : null;
      if (_capiEv && capiHookPagamento) {
        await capiHookPagamento(provider, {
          email: p.email, telefone: p.telefone,
          valorCentavos: Math.round(Number(p.valor || 0) * 100),
          produto: p.produtoNome, produtoId: p.produtoId, evento: _capiEv,
        }).catch(() => {});
      }
    } catch { /* CAPI best-effort: nunca quebra a entrega */ }
    if (p.telefone) {
      const cid = upsertContato(projeto, p.telefone, p.nome, p.email, `pagamento:${provider}`);
      if (categoria === 'aprovado') {
        aplicarTag(projeto, cid, 'comprou');
        await dispararEntrega(projeto, p.telefone, p.nome, p.produtoId, p.produtoNome).catch(() => {});
        // [motor2] Trilha por produto + order bump/upsell. INERTE até pubFlag('motor2_ativo')==='1'.
        // Aplica as tags da trilha de CADA item (principal + bump + upsell), dispara os fluxos pela tag,
        // e entrega no WhatsApp os itens EXTRAS que tenham fluxo próprio (o principal já foi entregue acima).
        if (pubFlag('motor2_ativo', '0') === '1') {
          try {
            const itens = _itensPagamento(provider, b);
            for (let i = 0; i < itens.length; i++) {
              const it = itens[i];
              const projItem = projetoDoProduto(it.produtoId, it.produtoNome);
              const cidItem = (projItem === projeto) ? cid : upsertContato(projItem, p.telefone, p.nome, p.email, `pagamento:${provider}`);
              for (const tg of _produtoTags(it.produtoId, it.produtoNome)) {
                aplicarTag(projItem, cidItem, tg);
                await rodarAutomacoes(projItem, 'tag_adicionada', { contatoId: cidItem, tag: tg, telefone: p.telefone, nome: p.nome }).catch(() => {});
              }
              const ehPrincipal = (projItem === projeto) && (
                (it.produtoId && it.produtoId === String(p.produtoId || '')) ||
                (it.produtoNome && it.produtoNome.toLowerCase() === String(p.produtoNome || '').toLowerCase())
              );
              if (!ehPrincipal && produtoFluxoMatch(it.produtoId, it.produtoNome)) {
                await dispararEntrega(projItem, p.telefone, p.nome, it.produtoId, it.produtoNome).catch(() => {});
              }
            }
          } catch { /* [motor2] best-effort: nunca quebra a entrega padrão */ }
        }
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
        // [num-escolha] respeita o número de envio gravado na fila (item.instancia); sem ele, usa o padrão do projeto.
        if (tipo === 'texto') {
          await enviarTexto(item.projeto, item.para, item.texto || '', item.instancia || undefined);
        } else {
          await enviarMidia(item.projeto, item.para, tipo, item.url || '', item.legenda || '', item.instancia || undefined);
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
// [#2 provedor trocável] Provedor efetivo: usa CONFIG.llmProvider (base_url/chave cifrada/modelo) se setado, senão o .env.
function _llmEfetivo(): { baseUrl: string; apiKey: string; modeloPadrao: string } {
  const p = (CONFIG as any).llmProvider || {};
  let key = LLM_API_KEY;
  if (p.apiKeyEnc) { try { key = decifra(p.apiKeyEnc) || LLM_API_KEY; } catch { key = LLM_API_KEY; } }
  return {
    baseUrl: (p.baseUrl && String(p.baseUrl).trim()) || LLM_BASE_URL,
    apiKey: key,
    modeloPadrao: (p.modelo && String(p.modelo).trim()) || LLM_MODEL_PADRAO,
  };
}
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
CREATE TABLE IF NOT EXISTS contato_agente (
  contato_id TEXT PRIMARY KEY,
  agente_id TEXT,
  origem TEXT,
  atualizado_em TEXT
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
  // [comunidade] Porteiro genérico: só descobre o instrumento e direciona pro agente dele (não tem funil próprio).
  comunidade: {
    nome: 'Comunidade (porteiro)',
    corteIdade: 24,
  },
};

// Garante CONFIG.agentesHabilitados (padrao: tudo desligado) sem apagar config existente.
if (!(CONFIG as any).agentesHabilitados) {
  (CONFIG as any).agentesHabilitados = { baixo: false, violao: false, teclado: false, comunidade: false };
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
      'gpt-4o-mini', 0.7, 0, 1, JSON.stringify(seed), agora(), agora()
    );
  }
}
try { _seedAgentes(); } catch (e) { console.log('[agentes] seed falhou:', e); }
// [etapa27] troca de modelo pra economizar: agentes que estavam no gpt-5.4-mini (caro) passam pro gpt-4o-mini (mesmo da Sellflux, bem mais barato). Idempotente — só mexe em quem ainda está no 5.4-mini.
try { const _mig = db.prepare(`UPDATE agentes_ia SET modelo='gpt-4o-mini', atualizado_em=? WHERE modelo='gpt-5.4-mini'`).run(agora()); if (_mig.changes) console.log('[etapa27] modelo dos agentes: gpt-5.4-mini -> gpt-4o-mini em', _mig.changes, 'agente(s)'); } catch (e) { console.log('[etapa27] migracao de modelo falhou:', e); }

function agenteDoProjeto(projeto: string): any {
  return db.prepare(`SELECT * FROM agentes_ia WHERE projeto=? ORDER BY atualizado_em DESC LIMIT 1`).get(projeto) as any;
}

// [multi-agente] tags do contato (minúsculas) no projeto.
function _tagsDoContato(projeto: string, contatoId: string): string[] {
  try {
    const rows = db.prepare(`SELECT t.nome FROM contato_tags ct JOIN tags t ON t.id=ct.tag_id WHERE ct.contato_id=? AND t.projeto=?`).all(contatoId, projeto) as any[];
    return rows.map((r) => String(r?.nome || '').toLowerCase()).filter(Boolean);
  } catch { return []; }
}
// [multi-agente] O contato tem algum agente atribuído (manual em Atendimentos ou handoff anterior)?
function _temAgenteAtribuido(contatoId: string): boolean {
  try { return !!db.prepare(`SELECT 1 FROM contato_agente WHERE contato_id=?`).get(contatoId); } catch { return false; }
}
// [multi-agente] Resolve QUAL agente atende: (1) override desta chamada (handoff); (2) agente atribuído ao
// contato (atribuição manual em Atendimentos ou handoff anterior, se ainda ativo); (3) roteamento por TAG
// dentro do projeto (agente ativo com config.tagsGatilho que casa, por config.prioridade asc — menor = antes).
// Se nada casar, retorna null => o chamador cai no agenteDoProjeto (comportamento atual).
function _resolverAgente(projeto: string, contatoId?: string, opts?: any): any {
  if (opts && opts.agenteOverride) return opts.agenteOverride;
  if (contatoId) {
    try {
      const at = db.prepare(`SELECT agente_id FROM contato_agente WHERE contato_id=?`).get(contatoId) as any;
      if (at && at.agente_id) {
        const a = db.prepare(`SELECT * FROM agentes_ia WHERE id=?`).get(at.agente_id) as any;
        if (a && a.ativo) return a;
      }
    } catch { /* best-effort */ }
    try {
      const tags = _tagsDoContato(projeto, contatoId);
      if (tags.length) {
        const cands = (db.prepare(`SELECT * FROM agentes_ia WHERE projeto=? AND ativo=1`).all(projeto) as any[])
          .map((a) => { let c: any = {}; try { c = JSON.parse(a.config || '{}'); } catch { c = {}; } const tg = (Array.isArray(c.tagsGatilho) ? c.tagsGatilho : []).map((x: any) => String(x || '').toLowerCase()).filter(Boolean); const pr = Number(c.prioridade == null ? 100 : c.prioridade); return { a, tg, pr }; })
          .filter((x) => x.tg.length && x.tg.some((t: string) => tags.some((ct) => ct.includes(t))))
          .sort((x, y) => x.pr - y.pr);
        if (cands.length) return cands[0].a;
      }
    } catch { /* best-effort */ }
  }
  return null;
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

// [gcal-fonte] liga/desliga sincronizacao com Google Agenda POR FONTE (default ligado). Controle do Ezequias.
function _gcalFonteLigada(fonte: string): boolean { try { return pubFlag('gcal_sync_' + fonte, '1') === '1'; } catch { return true; } }

// [disp-janelas] oferta de horarios lendo a tela "Minha Disponibilidade" (#6). Se a tabela estiver vazia,
// cai no comportamento antigo (_slotsLivres, grade fixa do seed) — nada quebra.
function _slotsLivresV2(projeto: string, periodo: string, limite = 10): any[] {
  let janelas: any[] = [];
  try { janelas = db.prepare(`SELECT dia_semana, hora, tipo, vagas FROM disponibilidade_janelas WHERE projeto=? AND ativo=1`).all(projeto) as any[]; } catch { janelas = []; }
  if (!janelas || !janelas.length) return _slotsLivres(projeto, periodo, limite);
  const seed = AGENTE_SEED[projeto] || {};
  const antecedenciaH = (seed.grade && seed.grade.antecedenciaH) || 2;
  const noPeriodo = (h: string): boolean => {
    const hh = Number(String(h).split(':')[0]);
    if (periodo === 'manha') return hh < 12;
    if (periodo === 'tarde') return hh >= 12 && hh < 18;
    if (periodo === 'noite') return hh >= 18;
    return true;
  };
  const agoraMs = Date.now();
  const minMs = agoraMs + antecedenciaH * 3600 * 1000;
  const ocup: any = {};
  try { (db.prepare(`SELECT inicio, COUNT(*) c FROM eventos WHERE projeto=? GROUP BY inicio`).all(projeto) as any[]).forEach((e) => { ocup[String(e.inicio)] = Number(e.c || 0); }); } catch { /* best-effort */ }
  const porDia: any = {};
  for (const j of janelas) { const d = Number(j.dia_semana); if (!porDia[d]) porDia[d] = []; porDia[d].push(j); }
  const out: any[] = [];
  for (let d = 0; d < 21 && out.length < limite; d++) {
    const brt = new Date(agoraMs + d * 86400000 - 3 * 3600000);
    const dow = brt.getUTCDay();
    const doDia = porDia[dow]; if (!doDia || !doDia.length) continue;
    doDia.sort((a: any, b: any) => String(a.hora).localeCompare(String(b.hora)));
    for (const j of doDia) {
      const h = String(j.hora);
      if (!noPeriodo(h)) continue;
      const parts = h.split(':'); const hh = Number(parts[0]); const mm = Number(parts[1] || 0);
      const inicioUtcMs = Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate(), hh + 3, mm);
      if (inicioUtcMs < minMs) continue;
      const iso = new Date(inicioUtcMs).toISOString();
      const vagas = Math.max(1, Number(j.vagas || 1));
      if (Number(ocup[iso] || 0) >= vagas) continue;
      out.push({ iso, label: _DIAS_PT[dow] + ' ' + String(brt.getUTCDate()).padStart(2, '0') + '/' + String(brt.getUTCMonth() + 1).padStart(2, '0') + ' as ' + h, tipo: j.tipo || '', vagas });
      if (out.length >= limite) break;
    }
  }
  return out;
}

// --------- Ferramentas (function-calling) que o agente pode chamar ---------
function _toolsDef(projeto?: string): any[] {
  const base: any[] = [
    { type: 'function', function: { name: 'listar_horarios_livres', description: 'Lista horarios livres reais da agenda do professor no periodo pedido. Use SEMPRE antes de oferecer horario; nunca invente horario.', parameters: { type: 'object', properties: { periodo: { type: 'string', enum: ['manha', 'tarde', 'noite'] } }, required: ['periodo'] } } },
    { type: 'function', function: { name: 'salvar_whatsapp', description: 'Grava o numero de WhatsApp da pessoa no cadastro (so digitos, comecando com 55 e DDD).', parameters: { type: 'object', properties: { telefone: { type: 'string' } }, required: ['telefone'] } } },
    { type: 'function', function: { name: 'marcar_agendamento', description: 'Cria o compromisso na agenda. Use o iso exato de um horario que a ferramenta de horarios retornou como livre.', parameters: { type: 'object', properties: { inicio_iso: { type: 'string' }, nome: { type: 'string' }, observacao: { type: 'string' } }, required: ['inicio_iso', 'nome'] } } },
    { type: 'function', function: { name: 'registrar_card_crm', description: 'Cria/atualiza o card do lead no CRM com um resumo da qualificacao (horario, instrumento, igreja/gosto, dor, o que perguntou).', parameters: { type: 'object', properties: { resumo: { type: 'string' } }, required: ['resumo'] } } },
    { type: 'function', function: { name: 'adicionar_tag', description: 'Marca o lead com uma tag (ex: sdr-instagram).', parameters: { type: 'object', properties: { tag: { type: 'string' } }, required: ['tag'] } } },
    { type: 'function', function: { name: 'transferir_fila', description: 'Transfere a conversa pra fila humana (o professor assume). O agente para de responder esse lead depois disso.', parameters: { type: 'object', properties: { motivo: { type: 'string' } }, required: ['motivo'] } } },
  ];
  // [comunidade] só o porteiro ganha a ferramenta de direcionar pro instrumento (os agentes de instrumento não precisam).
  if (projeto === 'comunidade') {
    base.push({ type: 'function', function: { name: 'direcionar_instrumento', description: 'Use SOMENTE quando descobrir qual instrumento a pessoa toca ou quer aprender. Direciona a conversa pro agente daquele instrumento, que assume a qualificacao do ponto em que esta. instrumento: violao, teclado ou baixo.', parameters: { type: 'object', properties: { instrumento: { type: 'string', enum: ['violao', 'teclado', 'baixo'] } }, required: ['instrumento'] } } });
  }
  return base;
}

async function _execTool(projeto: string, conversa: any, nomeFerr: string, args: any): Promise<any> {
  const seed = AGENTE_SEED[projeto] || {};
  try {
    if (nomeFerr === 'listar_horarios_livres') {
      const slots = _slotsLivresV2(projeto, String(args.periodo || ''), 10); // [disp-janelas] le a tela de Disponibilidade; fallback no seed
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
      // [gcal-ia] se o Google Agenda (nuvem) estiver ligado E a fonte "ia" marcada pelo Ezequias, cria tambem no Google
      let _gcalIaId: string | null = null; let _gcalIaAviso = '';
      if (pubFlag('gcal_ativo') === '1' && _gcalFonteLigada('ia')) {
        try { _gcalIaId = await _gcalCriarEvento({ tipo: 'ia', refId: id, titulo: 'Sessao ' + projeto + ' - ' + String(args.nome || conversa.telefone), inicio, fim, descricao: String(args.observacao || '') }); }
        catch (e: any) { _gcalIaAviso = 'google_falhou: ' + String(e && e.message || e); }
      } else if (pubFlag('gcal_ativo') === '1') { _gcalIaAviso = 'fonte_ia_desligada'; }
      return { ok: true, id, inicio, fim, sala: seed.sala || '', googleEventId: _gcalIaId, avisoGoogle: _gcalIaAviso };
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
    if (nomeFerr === 'direcionar_instrumento') {
      const inst = String(args.instrumento || '').toLowerCase();
      if (!['violao', 'teclado', 'baixo'].includes(inst)) return { ok: false, erro: 'instrumento_invalido' };
      const alvo = agenteDoProjeto(inst);
      if (!alvo) return { ok: false, erro: 'sem_agente_' + inst };
      if (conversa.contato_id) {
        try { db.prepare(`INSERT INTO contato_agente(contato_id,agente_id,origem,atualizado_em) VALUES(?,?,?,?) ON CONFLICT(contato_id) DO UPDATE SET agente_id=excluded.agente_id, origem=excluded.origem, atualizado_em=excluded.atualizado_em`).run(conversa.contato_id, alvo.id, 'comunidade', agora()); } catch { /* best-effort */ }
      }
      return { ok: true, direcionado: true, instrumento: inst, agenteId: alvo.id };
    }
  } catch (e: any) {
    return { ok: false, erro: String(e && e.message || e) };
  }
  return { ok: false, erro: 'ferramenta desconhecida' };
}

// --------- Chamada ao LLM (API compativel com OpenAI chat/completions) ---------
async function _chamarLLM(messages: any[], modelo: string, temperatura: number, tools: any[]): Promise<any> {
  const _prov = _llmEfetivo();
  if (!_prov.apiKey) return { content: '', _semChave: true };
  const body: any = { model: modelo || _prov.modeloPadrao, messages, temperature: (temperatura == null ? 0.7 : temperatura) };
  if (tools && tools.length) { body.tools = tools; body.tool_choice = 'auto'; }
  const res = await fetch(`${_prov.baseUrl}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${_prov.apiKey}` },
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
  const agente = _resolverAgente(projeto, contatoId, opts) || agenteDoProjeto(projeto);
  if (!agente || !agente.ativo || !String(agente.prompt || '').trim()) {
    return { ok: false, motivo: 'sem agente ativo/ prompt' };
  }
  let conversa = db.prepare(`SELECT * FROM agente_conversas WHERE agente_id=? AND telefone=?`).get(agente.id, telefone) as any;
  if (!conversa) {
    const id = randomUUID();
    const histIni = (opts && opts._contextoInicial) ? JSON.stringify([{ role: 'user', content: String(opts._contextoInicial) }]) : '[]';
    db.prepare(`INSERT INTO agente_conversas(id,agente_id,projeto,telefone,contato_id,status,estado,historico,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?)`)
      .run(id, agente.id, projeto, telefone, contatoId || null, 'ativo', '{}', histIni, agora(), agora());
    conversa = db.prepare(`SELECT * FROM agente_conversas WHERE id=?`).get(id) as any;
  }
  if (conversa.status !== 'ativo') return { ok: false, motivo: 'conversa nao-ativa (' + conversa.status + ')' };

  let historico: any[] = [];
  try { historico = JSON.parse(conversa.historico || '[]'); } catch { historico = []; }
  historico.push({ role: 'user', content: texto });

  const tools = agente.tools_on ? _toolsDef(projeto) : [];
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
          // [comunidade] handoff: porteiro identificou o instrumento -> o agente do instrumento ASSUME e responde AGORA
          if (tc.function.name === 'direcionar_instrumento' && resultado && (resultado as any).direcionado) {
            try { db.prepare(`UPDATE agente_conversas SET historico=?, atualizado_em=? WHERE id=?`).run(JSON.stringify(historico).slice(0, 200000), agora(), conversa.id); } catch { /* best-effort */ }
            const _prof = Number((opts && opts._profundidade) || 0);
            const _alvoProj = String((resultado as any).instrumento || '');
            const _alvoAgente = agenteDoProjeto(_alvoProj);
            if (_prof < 2 && _alvoAgente) {
              const _ctx = `(contexto interno: lead chegou pela Comunidade e ja identificamos que toca/aprende ${_alvoProj}. Continue a qualificacao de ${_alvoProj} daqui, sem perguntar o instrumento de novo.)`;
              return await processarMensagemAgente(_alvoProj, telefone, texto, nome, contatoId, Object.assign({}, opts, { agenteOverride: _alvoAgente, _contextoInicial: _ctx, _profundidade: _prof + 1 }));
            }
          }
        }
        continue; // volta pro LLM com os resultados das ferramentas
      }

      const conteudo = String(msg.content || '').trim();
      if (conteudo) {
        historico.push({ role: 'assistant', content: conteudo });
        respostasEnviadas.push(conteudo);
        if (!teste && !(opts && opts.naoEnviar)) { try { await enviarTexto(projeto as any, telefone, conteudo, (opts && opts.instancia) ? String(opts.instancia) : undefined); } catch (e) { console.log('[agente] enviarTexto falhou', e); } }
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
        .run(String(b.nome || ''), String(b.prompt || ''), String(b.modelo || 'gpt-4o-mini'), Number(b.temperatura == null ? 0.7 : b.temperatura), b.ativo ? 1 : 0, b.toolsOn === false ? 0 : 1, JSON.stringify(b.config || {}), agora(), id);
      return res.json({ ok: true, id });
    }
  }
  const novo = id || randomUUID();
  db.prepare(`INSERT INTO agentes_ia(id,projeto,nome,prompt,modelo,temperatura,ativo,tools_on,config,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?)`)
    .run(novo, String(b.projeto || ''), String(b.nome || ''), String(b.prompt || ''), String(b.modelo || 'gpt-4o-mini'), Number(b.temperatura == null ? 0.7 : b.temperatura), b.ativo ? 1 : 0, b.toolsOn === false ? 0 : 1, JSON.stringify(b.config || {}), agora(), agora());
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
// [#2 IG] Canal de Instagram ligado para o projeto? (navegador ou api). INDEPENDENTE do WhatsApp:
// o /agente/ig passa a depender SO deste canal, nao do agentesHabilitados (que controla o WhatsApp).
function _canalIgLigado(projeto: string): boolean {
  const m = String(((CONFIG as any).canalIg || {})[projeto] || 'off');
  return m === 'navegador' || m === 'api';
}
// [#2 IG] Recebe uma mensagem de Instagram (do PC2 por navegador, OU via API oficial no futuro),
// roda o agente e DEVOLVE a resposta (naoEnviar) pra quem chamou entregar no Direct.
// Conversa identificada por 'ig:'+ident. PULA o gate por tag (quem responde a nossa DM fria é lead-alvo);
// mantém o guard de aluno quando o contato já é conhecido.
app.post('/agente/ig', async (req, res) => {
  const b = req.body || {};
  const projeto = String(b.projeto || '');
  const ident = String(b.ident || b.igUser || b.usuario || '').trim();
  const texto = String(b.texto || '');
  if (!PROJETOS.includes(projeto as any)) return res.status(400).json({ ok: false, erro: 'projeto_invalido' });
  if (!ident) return res.status(400).json({ ok: false, erro: 'ident_obrigatorio' });
  if (!_canalIgLigado(projeto)) return res.json({ ok: true, respostas: [], motivo: 'canal ig desligado' });
  const idKey = 'ig:' + ident;
  let cid: string | undefined;
  try { cid = upsertContato(projeto, idKey, b.nome); } catch { cid = undefined; }
  if (cid && _contatoEhAluno(projeto, cid)) return res.json({ ok: true, respostas: [], motivo: 'ja e aluno' });
  try {
    const r = await processarMensagemAgente(projeto, idKey, texto, b.nome, cid, { naoEnviar: true, canal: 'ig' });
    res.json({ ok: true, respostas: (r && (r as any).respostas) || [], status: (r && (r as any).status) || 'ativo', motivo: (r && (r as any).motivo) || undefined });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e && e.message || e) }); }
});


// [multi-agente] Atribuir manualmente um agente a um contato (direcionar atendimento na tela de Atendimentos).
// Vale pra WhatsApp e Instagram; o agente escolhido ASSUME e responde a partir da proxima mensagem do lead.
// agenteId vazio = limpa a atribuicao (volta ao roteamento automatico por tag / padrao).
app.get('/atendimento/agente-atual', (req, res) => {
  const contatoId = String(req.query.contatoId || req.query.contato_id || '');
  if (!contatoId) return res.status(400).json({ ok: false, erro: 'contato_obrigatorio' });
  const at = db.prepare(`SELECT agente_id, origem, atualizado_em FROM contato_agente WHERE contato_id=?`).get(contatoId) as any;
  if (!at) return res.json({ ok: true, agente: null });
  const a = db.prepare(`SELECT id,projeto,nome FROM agentes_ia WHERE id=?`).get(at.agente_id) as any;
  res.json({ ok: true, agente: a || null, origem: at.origem });
});
app.post('/atendimento/atribuir-agente', (req, res) => {
  const b = req.body || {};
  const contatoId = String(b.contatoId || b.contato_id || '');
  const agenteId = String(b.agenteId || b.agente_id || '');
  if (!contatoId) return res.status(400).json({ ok: false, erro: 'contato_obrigatorio' });
  if (!agenteId) { try { db.prepare(`DELETE FROM contato_agente WHERE contato_id=?`).run(contatoId); } catch { /* best-effort */ } return res.json({ ok: true, limpo: true }); }
  const a = db.prepare(`SELECT id,projeto,nome FROM agentes_ia WHERE id=?`).get(agenteId) as any;
  if (!a) return res.status(400).json({ ok: false, erro: 'agente_inexistente' });
  try { db.prepare(`INSERT INTO contato_agente(contato_id,agente_id,origem,atualizado_em) VALUES(?,?,?,?) ON CONFLICT(contato_id) DO UPDATE SET agente_id=excluded.agente_id, origem=excluded.origem, atualizado_em=excluded.atualizado_em`).run(contatoId, agenteId, 'manual', agora()); } catch (e: any) { return res.status(500).json({ ok: false, erro: String(e && e.message || e) }); }
  // reativa a conversa mais recente desse contato (se estava em humano/encerrada) pro agente assumir
  try { const ac = db.prepare(`SELECT id FROM agente_conversas WHERE contato_id=? ORDER BY atualizado_em DESC LIMIT 1`).get(contatoId) as any; if (ac) db.prepare(`UPDATE agente_conversas SET status='ativo', atualizado_em=? WHERE id=?`).run(agora(), ac.id); } catch { /* best-effort */ }
  res.json({ ok: true, agente: { id: a.id, projeto: a.projeto, nome: a.nome } });
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
    const d = new Date(iso); // [+hora-brasilia] container roda em UTC -> formatar sempre no fuso de Brasília
    const hojeBR = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Sao_Paulo' });
    const dBR = d.toLocaleDateString('en-CA', { timeZone: 'America/Sao_Paulo' });
    if (dBR !== hojeBR) return d.toLocaleDateString('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit' });
    return d.toLocaleTimeString('pt-BR', { timeZone: 'America/Sao_Paulo', hour: '2-digit', minute: '2-digit' });
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

// [grupo-link] Tenta pegar o LINK de convite de um grupo (pra identificar/abrir). Precisa que o número seja ADMIN
// do grupo; se não for, o Evolution recusa — e aí o motivo já volta explicado (mesma causa do erro ao mudar nome/foto).
app.get('/disparo/grupo/link', async (req, res) => {
  try {
    const jid = String((req.query as any).jid || (req.query as any).id || '').trim();
    if (!jid) return res.json({ ok: false, erro: 'jid_obrigatorio' });
    let instancia = String((req.query as any).instancia || '').trim();
    if (!instancia) { const g = db.prepare(`SELECT instancia FROM grupos_wpp WHERE id=?`).get(jid) as any; instancia = String((g && g.instancia) || ''); }
    if (!instancia) return res.json({ ok: false, erro: 'instancia_nao_encontrada' });
    try {
      const r: any = await evo(`/group/inviteCode/${instancia}?groupJid=${encodeURIComponent(jid)}`, 'GET');
      const code = (r && (r.inviteCode || r.code)) || '';
      const url = (r && r.inviteUrl) || (code ? 'https://chat.whatsapp.com/' + code : '');
      if (!url) return res.json({ ok: false, erro: 'sem_link', detalhe: r });
      return res.json({ ok: true, link: url, jid, instancia });
    } catch (e: any) {
      return res.json({ ok: false, erro: 'evolution_recusou', detalhe: String((e && e.message) || e), dica: 'Quase sempre é porque esse número não é ADMIN do grupo.' });
    }
  } catch (e: any) { res.json({ ok: false, erro: String((e && e.message) || e) }); }
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

// [+tag-busca] índice pra acelerar contagem/junção por tag (tag_id não era indexado; a PK começa por contato_id).
try { db.exec(`CREATE INDEX IF NOT EXISTS ix_ct_tagid ON contato_tags(tag_id)`); } catch (e) { /* ja existe */ }

// [+tag-busca] busca RÁPIDA de tags por prefixo/contendo (autocomplete) — sem contar a base inteira.
// GET /disparo/tags-busca?projeto=&q=   -> { ok, tags:[{nome,n}] }  (prefixo primeiro, limite 20)
app.get('/disparo/tags-busca', (req, res) => {
  try {
    const projeto = String(req.query.projeto || '');
    const q = String(req.query.q || '').trim();
    const conds: string[] = []; const args: any[] = [];
    if (projeto) { conds.push('projeto=?'); args.push(projeto); }
    if (q) { conds.push('nome LIKE ?'); args.push('%' + q + '%'); }
    const where = conds.length ? ('WHERE ' + conds.join(' AND ')) : '';
    const rows = db.prepare(`SELECT id, nome FROM tags ${where} ORDER BY CASE WHEN nome LIKE ? THEN 0 ELSE 1 END, nome LIMIT 20`).all(...args, q + '%') as any[];
    const cnt = db.prepare(`SELECT COUNT(*) n FROM contato_tags WHERE tag_id=?`);
    const tags = rows.map((r) => ({ nome: r.nome, n: ((cnt.get(r.id) as any)?.n) || 0 }));
    res.json({ ok: true, tags });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[+tag-busca] /disparo/tags-busca (autocomplete rápido) + índice ix_ct_tagid');

function _alvosDisparo(p: any): { projeto: string; para: string; grupo: boolean }[] {
  const out: { projeto: string; para: string; grupo: boolean }[] = [];
  const padrao = (CONFIG as any).projetoPadrao || 'teclado';
  if (Array.isArray(p.telefones) && p.telefones.length) { // [etapa27] lista explícita de contatos (números) — disparo individual por seleção
    return p.telefones.filter(Boolean).map((tel: any) => ({ projeto: String(p.projeto || padrao), para: String(tel), grupo: false }));
  }
  if (p.alvoTipo === 'grupos') {
    for (const jid of (p.grupos || [])) {
      const g = db.prepare(`SELECT projeto FROM grupos_wpp WHERE id=?`).get(jid) as any;
      out.push({ projeto: g?.projeto || p.projeto || padrao, para: String(jid), grupo: true });
    }
  } else {
    const inc = (p.incluirTags || []); const exc = (p.excluirTags || []);
    if (!inc.length) return out;
    const ph = (a: any[]) => a.map(() => '?').join(',');
    let sql = `SELECT c.id, c.telefone, c.projeto FROM contatos c
      JOIN contato_tags ct ON ct.contato_id=c.id JOIN tags t ON t.id=ct.tag_id
      WHERE t.nome IN (${ph(inc)})`;
    const args: any[] = [...inc];
    if (exc.length) { sql += ` AND c.id NOT IN (SELECT ct2.contato_id FROM contato_tags ct2 JOIN tags t2 ON t2.id=ct2.tag_id WHERE t2.nome IN (${ph(exc)}))`; args.push(...exc); }
    if (p.projeto) { sql += ` AND c.projeto=?`; args.push(p.projeto); }
    sql += ` GROUP BY c.id HAVING COUNT(DISTINCT t.nome)=${inc.length}`; // [+and-tags] precisa ter TODAS as tags incluidas (antes era OR)
    const rows = db.prepare(sql).all(...args) as any[];
    for (const r of rows) if (r.telefone) out.push({ projeto: r.projeto || p.projeto || padrao, para: String(r.telefone), grupo: false });
  }
  return out;
}

// [+envios-programados] colunas/tabela de "lote" pra LISTAR/CANCELAR/REABRIR agendamentos
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN lote_id TEXT`); } catch { /* ja existe */ }
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN lote_nome TEXT`); } catch { /* ja existe */ }
db.exec(`CREATE TABLE IF NOT EXISTS envios_programados (
  id TEXT PRIMARY KEY, projeto TEXT, is_grupo INTEGER DEFAULT 0, canal TEXT DEFAULT 'whatsapp',
  nome TEXT, quando TEXT, total INTEGER DEFAULT 0, payload_json TEXT, status TEXT DEFAULT 'agendado', criado_em TEXT
)`);
try { db.exec(`CREATE INDEX IF NOT EXISTS ix_fila_lote ON fila_envio(lote_id)`); } catch { /* ja existe */ }

function _enfileiraDisparo(p: any): number {
  const alvos = _alvosDisparo(p);
  const blocos = _resolverBlocosOd(p.blocos || []).filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
  if (!alvos.length || !blocos.length) return 0;
  const intervalo = Math.max(1, Number(p.intervaloSegundos || 12));
  const base = p.agendamento ? new Date(p.agendamento).getTime() : Date.now();
  const selInst: string[] = Array.isArray(p.instancias) ? p.instancias.map((x: any) => String(x)).filter(Boolean) : []; // [+seletor-numero]
  const loteId = randomUUID(); // [+envios-programados] identifica este lote pra achar/cancelar depois
  const loteNome = (String(p.loteNome || p.nomeEnvio || '').trim() || null); // [+envios-programados]
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,agendado_para,criado_em,lote_id,lote_nome) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  let n = 0; let primeiroQuando: string | null = null; // [+envios-programados]
  const tx = db.transaction(() => {
    alvos.forEach((a, i) => {
      blocos.forEach((b: any, j: number) => {
        const quando = new Date(base + i * intervalo * 1000 + j * 2000).toISOString();
        if (!primeiroQuando || quando < primeiroQuando) primeiroQuando = quando; // [+envios-programados]
        const tipo = b.tipo === 'texto' ? 'texto' : b.tipo;
        const _inst = (!a.grupo && selInst.length) ? selInst[i % selInst.length] : null; // [+seletor-numero]
        const _st = _inst ? 'cmp_multi' : 'camp_pend';
        ins.run(randomUUID(), a.projeto, a.para, a.grupo ? 1 : 0, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), _st, _inst, quando, agora(), loteId, loteNome);
        n++;
      });
    });
  });
  tx();
  // [+envios-programados] registra como "programado" só quando é pro FUTURO (agendado); imediato não entra
  if (n > 0 && base > Date.now() + 1000) {
    try {
      const ehGrupo = alvos.some((a: any) => a.grupo) ? 1 : 0;
      db.prepare(`INSERT OR REPLACE INTO envios_programados(id,projeto,is_grupo,canal,nome,quando,total,payload_json,status,criado_em) VALUES(?,?,?,?,?,?,?,?,'agendado',?)`)
        .run(loteId, String(p.projeto || alvos[0]?.projeto || ''), ehGrupo, String(p.canal || 'whatsapp'), loteNome, primeiroQuando || new Date(base).toISOString(), n, JSON.stringify(p || {}), agora());
    } catch { /* meta é best-effort */ }
  }
  return n;
}

app.post('/disparo/enviar', (req, res) => {
  try { const n = _enfileiraDisparo(req.body || {}); _salvarEnvio({ canal: 'whatsapp', tipo: 'disparo-rapido', payload: req.body || {} }); res.json({ ok: true, enfileirados: n }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.post('/disparo/sequencia', (req, res) => {
  try { let n = 0; for (const passo of (((req.body || {}).sequencia) || [])) n += _enfileiraDisparo(passo); _salvarEnvio({ canal: 'whatsapp', tipo: 'sequencia', payload: req.body || {} }); res.json({ ok: true, enfileirados: n }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// resumo do disparo (pendentes/enviados/falhou)
app.get('/disparo/resumo', (_req, res) => {
  res.json({ ok: true, porStatus: db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE status IN ('camp_pend','enviado','falhou') GROUP BY status`).all() });
});

// [etapa27] resumo SÓ dos individuais (is_grupo=0) — pra fila ao vivo do Disparo Individual
app.get('/disparo/resumo-individual', (_req, res) => {
  res.json({ ok: true, porStatus: db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE is_grupo=0 AND status IN ('camp_pend','enviado','falhou') GROUP BY status`).all() });
});

// [+envios-programados] LISTAR os disparos deixados AGENDADOS (pro futuro) — achar/reeditar/cancelar
app.get('/disparo/programados', (req, res) => {
  try {
    const conds: string[] = [`ep.status='agendado'`]; const args: any[] = [];
    const projeto = String(req.query.projeto || '');
    if (projeto) { conds.push('ep.projeto=?'); args.push(projeto); }
    if (req.query.is_grupo != null && String(req.query.is_grupo) !== '') { conds.push('ep.is_grupo=?'); args.push(Number(req.query.is_grupo) ? 1 : 0); }
    const rows = db.prepare(`SELECT id,projeto,is_grupo,canal,nome,quando,total,status,criado_em FROM envios_programados ep WHERE ${conds.join(' AND ')} ORDER BY ep.quando ASC LIMIT 500`).all(...args) as any[];
    const cnt = db.prepare(`SELECT COUNT(*) pend, MIN(agendado_para) prox FROM fila_envio WHERE lote_id=? AND status IN ('camp_pend','cmp_multi','cmp_mmk')`);
    const envg = db.prepare(`SELECT SUM(CASE WHEN status='enviado' THEN 1 ELSE 0 END) enviados, SUM(CASE WHEN status='falhou' THEN 1 ELSE 0 END) falhou FROM fila_envio WHERE lote_id=?`);
    const programados = rows.map((r) => {
      const c = cnt.get(r.id) as any; const e = envg.get(r.id) as any;
      const pend = (c?.pend) || 0;
      return { ...r, pendentes: pend, proximo: (c?.prox) || r.quando, enviados: (e?.enviados) || 0, falhou: (e?.falhou) || 0, concluido: pend === 0 };
    });
    res.json({ ok: true, programados });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [+envios-programados] detalhes de 1 programado (pra REABRIR no formulário e editar)
app.get('/disparo/programado/:id', (req, res) => {
  try {
    const r = db.prepare(`SELECT * FROM envios_programados WHERE id=?`).get(String(req.params.id)) as any;
    if (!r) { res.json({ ok: false, erro: 'nao encontrado' }); return; }
    let payload: any = {}; try { payload = JSON.parse(r.payload_json || '{}'); } catch { payload = {}; }
    res.json({ ok: true, programado: { id: r.id, projeto: r.projeto, is_grupo: r.is_grupo, canal: r.canal, nome: r.nome, quando: r.quando, total: r.total, status: r.status, payload } });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [+envios-programados] CANCELAR um programado (remove as mensagens ainda não enviadas desse lote)
app.post('/disparo/programado/cancelar', (req, res) => {
  try {
    const id = String((req.body || {}).id || '');
    if (!id) { res.json({ ok: false, erro: 'sem id' }); return; }
    const removidos = db.prepare(`DELETE FROM fila_envio WHERE lote_id=? AND status IN ('camp_pend','cmp_multi','cmp_mmk')`).run(id).changes;
    db.prepare(`UPDATE envios_programados SET status='cancelado' WHERE id=?`).run(id);
    res.json({ ok: true, removidos });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[+envios-programados] /disparo/programados (listar) + /disparo/programado/:id + /disparo/programado/cancelar');

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



// ============================================================================
// ETAPA 16 — Publicação na nuvem (espelhamento + migração do PC) — ADITIVO
// Espelha no motor o sistema de postagem/calendário/limpeza do app do PC, +
// as automações do PC2 (boas-vindas a novos seguidores, prospecção/directs) +
// a ponte "reunião qualificada -> Google Agenda" (que NÃO existia no PC).
// Nada aqui posta de verdade ainda: uploaders/sessões/tokens entram amanhã.
// Tudo gated por flags; schema em try/catch pra nunca quebrar o engine no boot.
// Namespaces: /pub/*  /sdr/*  /agenda/*
// ============================================================================
try {
  db.exec(`
  CREATE TABLE IF NOT EXISTS canais_publicacao (
    id TEXT PRIMARY KEY, nome TEXT, projeto TEXT, instrumento TEXT, cor TEXT,
    ativo INTEGER DEFAULT 1, plataformas_json TEXT DEFAULT '[]',
    ig_business_id TEXT, fb_page_id TEXT, yt_channel_id TEXT, tiktok_open_id TEXT,
    threads_user_id TEXT, pinterest_board_id TEXT,
    descricao_template TEXT, descricao_social_template TEXT, hashtags_padrao_json TEXT,
    usar_horario_otimo INTEGER DEFAULT 0, mover_publicados INTEGER DEFAULT 1,
    criado_em TEXT, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS horarios_preferidos (
    id TEXT PRIMARY KEY, canal_id TEXT, dia_semana INTEGER, horario TEXT,
    ativo INTEGER DEFAULT 1, formato TEXT, trilha TEXT
  );
  CREATE TABLE IF NOT EXISTS fila_publicacao (
    id TEXT PRIMARY KEY, canal_id TEXT, projeto TEXT,
    onedrive_ref TEXT, onedrive_item_id TEXT, arquivo_nome TEXT, thumb_ref TEXT,
    formato TEXT, plataformas_json TEXT DEFAULT '[]',
    titulo TEXT, descricao TEXT, descricao_social TEXT,
    data_agendada TEXT, status TEXT DEFAULT 'agendado',
    resultado_por_plataforma_json TEXT, erro TEXT,
    movido_para_publicados INTEGER DEFAULT 0, data_movido TEXT,
    arquivo_apagado INTEGER DEFAULT 0, data_arquivo_apagado TEXT,
    video_ids_externos_json TEXT, stats_json TEXT,
    tentativas_consecutivas INTEGER DEFAULT 0, prioridade INTEGER DEFAULT 0,
    aguardando_instrumento INTEGER DEFAULT 0, trial_reels INTEGER DEFAULT 0,
    criado_em TEXT, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS conexoes_sociais (
    id TEXT PRIMARY KEY, canal_id TEXT, plataforma TEXT, metodo TEXT,
    status TEXT DEFAULT 'desconhecido', detalhe TEXT,
    ultimo_sucesso TEXT, ultimo_check TEXT, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS oauth_tokens_pub (
    canal_id TEXT, plataforma TEXT, access_token_enc TEXT, refresh_token_enc TEXT,
    expires_at TEXT, scope TEXT, conta TEXT, atualizado_em TEXT,
    PRIMARY KEY (canal_id, plataforma)
  );
  CREATE TABLE IF NOT EXISTS playwright_sessoes (
    canal_id TEXT, plataforma TEXT, storage_state_enc TEXT, user_agent TEXT,
    handle_logado TEXT, logado_em TEXT, ultima_validacao TEXT,
    sessao_valida INTEGER DEFAULT 0, motivo_ultima_falha TEXT,
    PRIMARY KEY (canal_id, plataforma)
  );
  CREATE TABLE IF NOT EXISTS pub_kv (chave TEXT PRIMARY KEY, valor TEXT, atualizado_em TEXT);
  -- Boas-vindas a novos seguidores + prospecção (espelho do PC2)
  CREATE TABLE IF NOT EXISTS bv_config (
    canal_id TEXT PRIMARY KEY, plataforma TEXT DEFAULT 'instagram',
    daily_limit INTEGER DEFAULT 15, limit_jitter INTEGER DEFAULT 3,
    batch_size INTEGER DEFAULT 5, batch_pause_min INTEGER DEFAULT 30,
    janela_hora_inicio INTEGER DEFAULT 8, janela_hora_fim INTEGER DEFAULT 21,
    warmup_enabled INTEGER DEFAULT 1, warmup_start_limit INTEGER DEFAULT 5, warmup_step_per_day INTEGER DEFAULT 2,
    day_off_weekday INTEGER, first_use_date TEXT, stop_words TEXT DEFAULT 'parar,stop,sair,cancelar',
    mensagens_json TEXT, dry_run INTEGER DEFAULT 1, paused_by_user INTEGER DEFAULT 0, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS bv_fila (
    id TEXT PRIMARY KEY, canal_id TEXT, plataforma TEXT, username TEXT, display_name TEXT,
    tipo TEXT DEFAULT 'welcome', seq_index INTEGER DEFAULT 0, texto_pronto TEXT,
    not_before TEXT, status TEXT DEFAULT 'pendente', attempts INTEGER DEFAULT 0,
    motivo_falha TEXT, enviado_em TEXT, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS bv_enviados (
    id TEXT PRIMARY KEY, canal_id TEXT, plataforma TEXT, username TEXT, tipo TEXT,
    seq_index INTEGER DEFAULT 0, texto TEXT, sent_at TEXT, respondeu_em TEXT, followup_enviado INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS bv_seguidores (
    canal_id TEXT, plataforma TEXT, username TEXT, display_name TEXT, visto_em TEXT,
    PRIMARY KEY (canal_id, plataforma, username)
  );
  CREATE TABLE IF NOT EXISTS bv_blocklist (
    canal_id TEXT, plataforma TEXT, username TEXT, reason TEXT, criado_em TEXT,
    PRIMARY KEY (canal_id, plataforma, username)
  );
  -- Ponte reunião -> Google Agenda (NOVO: não existia no PC)
  CREATE TABLE IF NOT EXISTS reunioes_sdr (
    id TEXT PRIMARY KEY, projeto TEXT, lead_nome TEXT, lead_telefone TEXT, lead_email TEXT,
    origem TEXT, titulo TEXT, inicio TEXT, fim TEXT, observacao TEXT,
    status TEXT DEFAULT 'agendada', google_event_id TEXT, criado_em TEXT, atualizado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS gcal_event_map (
    tipo TEXT, ref_id TEXT, google_event_id TEXT, atualizado_em TEXT, PRIMARY KEY (tipo, ref_id)
  );
  CREATE INDEX IF NOT EXISTS ix_fila_pub_status ON fila_publicacao(status, data_agendada);
  CREATE INDEX IF NOT EXISTS ix_fila_pub_canal ON fila_publicacao(canal_id, data_agendada);
  CREATE INDEX IF NOT EXISTS ix_bv_fila_status ON bv_fila(status, not_before);
  `);
} catch (e) { console.error('[etapa16] schema erro (ignorado p/ não derrubar o engine):', e); }

// ---- flags de operação (só ATIVA de verdade quando a gente ligar amanhã) ----
function pubFlag(chave: string, padrao = '0'): string {
  try { const r = db.prepare(`SELECT valor FROM pub_kv WHERE chave=?`).get(chave) as any; return r ? String(r.valor) : padrao; }
  catch { return padrao; }
}
function setPubFlag(chave: string, valor: string) {
  db.prepare(`INSERT INTO pub_kv(chave,valor,atualizado_em) VALUES(?,?,?) ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor, atualizado_em=excluded.atualizado_em`).run(chave, valor, agora());
}

// ---- cripto AES-256-GCM p/ tokens/sessões (substitui o safeStorage do Electron) ----
function _chaveMestra(): Buffer {
  const base = process.env.MOTOR_SECRET_KEY || (CONFIG as any).apiKey || 'motor-chave-provisoria-trocar';
  return createHash('sha256').update(String(base)).digest();
}
function cifra(texto: string): string {
  try {
    const iv = randomBytes(12); const c = createCipheriv('aes-256-gcm', _chaveMestra(), iv);
    const enc = Buffer.concat([c.update(String(texto), 'utf8'), c.final()]); const tag = c.getAuthTag();
    return 'v1:' + Buffer.concat([iv, tag, enc]).toString('base64');
  } catch { return ''; }
}
function decifra(blob: string): string {
  try {
    if (!blob || !blob.startsWith('v1:')) return '';
    const raw = Buffer.from(blob.slice(3), 'base64');
    const iv = raw.subarray(0, 12), tag = raw.subarray(12, 28), enc = raw.subarray(28);
    const d = createDecipheriv('aes-256-gcm', _chaveMestra(), iv); d.setAuthTag(tag);
    return Buffer.concat([d.update(enc), d.final()]).toString('utf8');
  } catch { return ''; }
}

// ---- saúde de conexão: deriva status por rede/canal (tokens + sessões + último sucesso) ----
function _statusConexao(canalId: string, plataforma: string): any {
  const tok = db.prepare(`SELECT expires_at, atualizado_em FROM oauth_tokens_pub WHERE canal_id=? AND plataforma=?`).get(canalId, plataforma) as any;
  const ses = db.prepare(`SELECT sessao_valida, motivo_ultima_falha, ultima_validacao FROM playwright_sessoes WHERE canal_id=? AND plataforma=?`).get(canalId, plataforma) as any;
  const conx = db.prepare(`SELECT status, detalhe, ultimo_sucesso FROM conexoes_sociais WHERE canal_id=? AND plataforma=?`).get(canalId, plataforma) as any;
  let status = 'nao_configurado'; let detalhe = '';
  if (tok && tok.access_token_enc !== '') { status = 'conectado'; detalhe = 'API'; }
  if (ses) { if (ses.sessao_valida) { status = 'conectado'; detalhe = (detalhe ? detalhe + '+' : '') + 'sessão'; } else if (ses.motivo_ultima_falha) { status = 'desconectado'; detalhe = String(ses.motivo_ultima_falha); } }
  if (conx && conx.status && conx.status !== 'desconhecido') { status = conx.status; if (conx.detalhe) detalhe = String(conx.detalhe); }
  return { canal_id: canalId, plataforma, status, detalhe, ultimo_sucesso: conx?.ultimo_sucesso || null };
}

// ======================= ENDPOINTS: CANAIS / HORÁRIOS =======================
app.get('/pub/canais', (_req, res) => {
  try { res.json({ ok: true, canais: db.prepare(`SELECT * FROM canais_publicacao ORDER BY projeto, nome`).all() }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/canais', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || randomUUID());
    const ex = db.prepare(`SELECT id FROM canais_publicacao WHERE id=?`).get(id);
    if (ex) {
      db.prepare(`UPDATE canais_publicacao SET nome=?, projeto=?, instrumento=?, cor=?, ativo=?, plataformas_json=?, ig_business_id=?, fb_page_id=?, yt_channel_id=?, tiktok_open_id=?, threads_user_id=?, pinterest_board_id=?, descricao_template=?, descricao_social_template=?, hashtags_padrao_json=?, usar_horario_otimo=?, mover_publicados=?, atualizado_em=? WHERE id=?`)
        .run(b.nome || '', b.projeto || '', b.instrumento || '', b.cor || '', b.ativo != null ? (b.ativo ? 1 : 0) : 1, JSON.stringify(b.plataformas || []), b.ig_business_id || null, b.fb_page_id || null, b.yt_channel_id || null, b.tiktok_open_id || null, b.threads_user_id || null, b.pinterest_board_id || null, b.descricao_template || null, b.descricao_social_template || null, JSON.stringify(b.hashtags_padrao || []), b.usar_horario_otimo ? 1 : 0, b.mover_publicados != null ? (b.mover_publicados ? 1 : 0) : 1, agora(), id);
    } else {
      db.prepare(`INSERT INTO canais_publicacao(id,nome,projeto,instrumento,cor,ativo,plataformas_json,ig_business_id,fb_page_id,yt_channel_id,tiktok_open_id,threads_user_id,pinterest_board_id,descricao_template,descricao_social_template,hashtags_padrao_json,usar_horario_otimo,mover_publicados,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
        .run(id, b.nome || '', b.projeto || '', b.instrumento || '', b.cor || '', b.ativo != null ? (b.ativo ? 1 : 0) : 1, JSON.stringify(b.plataformas || []), b.ig_business_id || null, b.fb_page_id || null, b.yt_channel_id || null, b.tiktok_open_id || null, b.threads_user_id || null, b.pinterest_board_id || null, b.descricao_template || null, b.descricao_social_template || null, JSON.stringify(b.hashtags_padrao || []), b.usar_horario_otimo ? 1 : 0, b.mover_publicados != null ? (b.mover_publicados ? 1 : 0) : 1, agora(), agora());
    }
    res.json({ ok: true, id });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/horarios', (req, res) => {
  try { res.json({ ok: true, horarios: db.prepare(`SELECT * FROM horarios_preferidos WHERE canal_id=? ORDER BY dia_semana, horario`).all(String(req.query.canal || '')) }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/horarios', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || randomUUID());
    if (b.remover) { db.prepare(`DELETE FROM horarios_preferidos WHERE id=?`).run(id); return res.json({ ok: true, removido: id }); }
    db.prepare(`INSERT INTO horarios_preferidos(id,canal_id,dia_semana,horario,ativo,formato,trilha) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET dia_semana=excluded.dia_semana, horario=excluded.horario, ativo=excluded.ativo, formato=excluded.formato, trilha=excluded.trilha`)
      .run(id, String(b.canal_id || ''), Number(b.dia_semana || 0), String(b.horario || '10:00'), b.ativo != null ? (b.ativo ? 1 : 0) : 1, b.formato || null, b.trilha || null);
    res.json({ ok: true, id });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= ENDPOINTS: FILA / CALENDÁRIO =======================
app.get('/pub/fila', (req, res) => {
  try {
    const q = req.query as any; const w: string[] = []; const a: any[] = [];
    if (q.canal) { w.push('canal_id=?'); a.push(String(q.canal)); }
    if (q.status) { w.push('status=?'); a.push(String(q.status)); }
    if (q.de) { w.push('data_agendada>=?'); a.push(String(q.de)); }
    if (q.ate) { w.push('data_agendada<=?'); a.push(String(q.ate)); }
    const sql = `SELECT * FROM fila_publicacao ${w.length ? 'WHERE ' + w.join(' AND ') : ''} ORDER BY data_agendada ASC LIMIT ${Math.min(2000, Number(q.limite || 1000))}`;
    res.json({ ok: true, itens: db.prepare(sql).all(...a) });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/calendario', (req, res) => {
  try {
    const q = req.query as any;
    const de = String(q.de || new Date(Date.now()).toISOString().slice(0, 10));
    const ate = String(q.ate || new Date(Date.now() + 30 * 864e5).toISOString().slice(0, 10));
    const w: string[] = ['date(data_agendada)>=date(?)', 'date(data_agendada)<=date(?)']; const a: any[] = [de, ate];
    if (q.canal) { w.push('canal_id=?'); a.push(String(q.canal)); }
    const itens = db.prepare(`SELECT id,canal_id,titulo,formato,plataformas_json,data_agendada,status FROM fila_publicacao WHERE ${w.join(' AND ')} ORDER BY data_agendada ASC`).all(...a) as any[];
    const porDia: any = {};
    for (const it of itens) { const d = String(it.data_agendada || '').slice(0, 10); (porDia[d] = porDia[d] || []).push(it); }
    res.json({ ok: true, de, ate, itens, porDia });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/resumo', (_req, res) => {
  try {
    const hoje = new Date().toISOString().slice(0, 10);
    const porStatus = db.prepare(`SELECT status, COUNT(*) n FROM fila_publicacao GROUP BY status`).all();
    const agendadosHoje = (db.prepare(`SELECT COUNT(*) n FROM fila_publicacao WHERE date(data_agendada)=date(?) AND status='agendado'`).get(hoje) as any)?.n || 0;
    const prox7 = (db.prepare(`SELECT COUNT(*) n FROM fila_publicacao WHERE date(data_agendada)>=date(?) AND date(data_agendada)<=date(?,'+7 day') AND status='agendado'`).get(hoje, hoje) as any)?.n || 0;
    const publicadosHoje = (db.prepare(`SELECT COUNT(*) n FROM fila_publicacao WHERE date(atualizado_em)=date(?) AND status='publicado'`).get(hoje) as any)?.n || 0;
    const porCanal = db.prepare(`SELECT c.nome, c.id canal_id, COUNT(f.id) n FROM canais_publicacao c LEFT JOIN fila_publicacao f ON f.canal_id=c.id AND f.status='agendado' GROUP BY c.id ORDER BY n DESC`).all();
    res.json({ ok: true, porStatus, agendadosHoje, prox7, publicadosHoje, porCanal });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/historico', (req, res) => {
  try {
    const dias = Math.min(365, Number((req.query as any).dias || 30));
    const w: string[] = [`status='publicado'`, `date(atualizado_em)>=date('now','-${dias} day')`]; const a: any[] = [];
    if ((req.query as any).canal) { w.push('canal_id=?'); a.push(String((req.query as any).canal)); }
    res.json({ ok: true, itens: db.prepare(`SELECT * FROM fila_publicacao WHERE ${w.join(' AND ')} ORDER BY atualizado_em DESC LIMIT 500`).all(...a) });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/agendar', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || randomUUID());
    // anti-duplicata: mesmo canal + mesmo onedrive_ref não finalizado
    if (b.onedrive_ref) {
      const dup = db.prepare(`SELECT id FROM fila_publicacao WHERE canal_id=? AND onedrive_ref=? AND status NOT IN ('cancelado','publicado')`).get(String(b.canal_id || ''), String(b.onedrive_ref)) as any;
      if (dup) return res.json({ ok: true, id: dup.id, jaExistia: true });
    }
    // [#2 nuvem] trava anti-conflito de slot: mesmo canal + mesmo horario ja ocupado (a menos que forcar:true)
    if (!b.forcar && b.data_agendada) {
      const slot = db.prepare(`SELECT id, titulo FROM fila_publicacao WHERE canal_id=? AND data_agendada=? AND status NOT IN ('cancelado','publicado')`).get(String(b.canal_id || ''), String(b.data_agendada)) as any;
      if (slot) return res.json({ ok: false, conflito: true, existente: { id: slot.id, titulo: String(slot.titulo || '') }, erro: 'Ja existe uma publicacao agendada nesse canal e horario. Envie forcar:true para agendar mesmo assim.' });
    }
    db.prepare(`INSERT INTO fila_publicacao(id,canal_id,projeto,onedrive_ref,onedrive_item_id,arquivo_nome,thumb_ref,formato,plataformas_json,titulo,descricao,descricao_social,data_agendada,status,prioridade,trial_reels,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(id, String(b.canal_id || ''), String(b.projeto || ''), b.onedrive_ref || null, b.onedrive_item_id || null, b.arquivo_nome || null, b.thumb_ref || null, String(b.formato || 'vertical'), JSON.stringify(b.plataformas || []), b.titulo || null, b.descricao || null, b.descricao_social || null, String(b.data_agendada || agora()), 'agendado', Number(b.prioridade || 0), b.trial_reels ? 1 : 0, agora(), agora());
    res.json({ ok: true, id });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/cancelar', (req, res) => {
  try { db.prepare(`UPDATE fila_publicacao SET status='cancelado', atualizado_em=? WHERE id=?`).run(agora(), String((req.body || {}).id || '')); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/reagendar', (req, res) => {
  try { const b = req.body || {}; db.prepare(`UPDATE fila_publicacao SET data_agendada=?, status='agendado', atualizado_em=? WHERE id=?`).run(String(b.data_agendada || agora()), agora(), String(b.id || '')); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= ENDPOINTS: CONEXÕES (saúde das redes) =======================
app.get('/pub/conexoes', (_req, res) => {
  try {
    const canais = db.prepare(`SELECT id, nome, projeto, plataformas_json FROM canais_publicacao WHERE ativo=1`).all() as any[];
    const out: any[] = [];
    for (const c of canais) {
      let plats: string[] = []; try { plats = JSON.parse(c.plataformas_json || '[]'); } catch {}
      for (const p of plats) out.push({ canal_nome: c.nome, projeto: c.projeto, ..._statusConexao(c.id, p) });
      // [lives-saude] inclui a conexão das LIVES (youtube_lives) se o canal já foi conectado pras lives,
      // pra ela aparecer no mesmo mapa de saúde da tela inicial.
      try {
        const temLives = db.prepare(`SELECT 1 FROM oauth_tokens_pub WHERE canal_id=? AND plataforma='youtube_lives'`).get(c.id)
          || db.prepare(`SELECT 1 FROM conexoes_sociais WHERE canal_id=? AND plataforma='youtube_lives'`).get(c.id);
        if (temLives && !plats.includes('youtube_lives')) out.push({ canal_nome: c.nome, projeto: c.projeto, ..._statusConexao(c.id, 'youtube_lives') });
      } catch { /* */ }
    }
    const resumo = { conectado: out.filter((x) => x.status === 'conectado').length, desconectado: out.filter((x) => x.status === 'desconectado').length, nao_configurado: out.filter((x) => x.status === 'nao_configurado').length, total: out.length };
    res.json({ ok: true, conexoes: out, resumo });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/conexao/status', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.canal_id || '') + ':' + String(b.plataforma || '');
    db.prepare(`INSERT INTO conexoes_sociais(id,canal_id,plataforma,metodo,status,detalhe,ultimo_sucesso,ultimo_check,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, detalhe=excluded.detalhe, ultimo_check=excluded.ultimo_check, atualizado_em=excluded.atualizado_em`)
      .run(id, String(b.canal_id || ''), String(b.plataforma || ''), b.metodo || null, String(b.status || 'desconhecido'), b.detalhe || null, b.ultimo_sucesso || null, agora(), agora());
    res.json({ ok: true });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= [telegram] AVISO QUANDO UMA CONEXÃO CAI =======================
// Config do bot (token cifrado + chat id) via endpoint. INERTE até ter token+chat+ativo.
// Ezequias cria o bot no @BotFather (pega o token) e descobre o chat id (@userinfobot).
async function tgEnviar(texto: string): Promise<boolean> {
  try {
    const token = decifra(pubFlag('telegram_bot_token_enc', ''));
    const chat = pubFlag('telegram_chat_id', '');
    if (!token || !chat) return false;
    const r: any = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text: texto, parse_mode: 'HTML', disable_web_page_preview: true }),
    });
    return r.ok;
  } catch { return false; }
}
app.post('/pub/telegram-config', (req, res) => {
  try {
    const b = req.body || {};
    if (b.botToken != null && String(b.botToken).trim()) setPubFlag('telegram_bot_token_enc', cifra(String(b.botToken).trim()));
    if (b.chatId != null) setPubFlag('telegram_chat_id', String(b.chatId).trim());
    if (b.ativo != null) setPubFlag('telegram_ativo', b.ativo ? '1' : '0');
    res.json({ ok: true, ativo: pubFlag('telegram_ativo', '0') === '1', tem_token: !!pubFlag('telegram_bot_token_enc', ''), chat_id: pubFlag('telegram_chat_id', '') });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/telegram-config', (_req, res) => {
  res.json({ ok: true, ativo: pubFlag('telegram_ativo', '0') === '1', tem_token: !!pubFlag('telegram_bot_token_enc', ''), chat_id: pubFlag('telegram_chat_id', '') });
});
app.post('/pub/telegram-test', async (_req, res) => {
  const ok = await tgEnviar('✅ Teste do Motor Melodias e Riffs — avisos de conexão ligados.');
  res.json({ ok, erro: ok ? undefined : 'falhou (confira token/chat id e se está ativo)' });
});
// lista as conexões FORA DO AR (publicação + lives), pra comparar entre checagens
function _conexoesForaDoAr(): string[] {
  const fora: string[] = [];
  try {
    const canais = db.prepare(`SELECT id, nome, plataformas_json FROM canais_publicacao WHERE ativo=1`).all() as any[];
    for (const c of canais) {
      let plats: string[] = []; try { plats = JSON.parse(c.plataformas_json || '[]'); } catch { /* */ }
      const checar = [...plats];
      try { const temLives = db.prepare(`SELECT 1 FROM conexoes_sociais WHERE canal_id=? AND plataforma='youtube_lives'`).get(c.id); if (temLives && !checar.includes('youtube_lives')) checar.push('youtube_lives'); } catch { /* */ }
      for (const p of checar) { const s = _statusConexao(c.id, p); if (s.status === 'desconectado') fora.push(`${c.nome}|${p}`); }
    }
  } catch { /* */ }
  return fora;
}
async function tickAlertasConexao() {
  if (pubFlag('telegram_ativo', '0') !== '1') return;
  const fora = _conexoesForaDoAr();
  let avisados: string[] = []; try { avisados = JSON.parse(pubFlag('telegram_avisados', '[]')); } catch { avisados = []; }
  const novos = fora.filter((x) => !avisados.includes(x));
  for (const n of novos) {
    const [canal, plat] = n.split('|');
    const nomePlat = plat === 'youtube_lives' ? 'YouTube (Lives)' : (plat === 'youtube' ? 'YouTube' : (plat === 'instagram' ? 'Instagram' : (plat === 'facebook' ? 'Facebook' : (plat === 'tiktok' ? 'TikTok' : plat))));
    await tgEnviar(`🔴 <b>Conexão caiu</b>\nCanal: <b>${canal}</b>\nRede: <b>${nomePlat}</b>\n\nReconecte quando puder no app (na tela inicial, no card de atenção, ou em Publicação → editar o canal → Conectar).`);
  }
  setPubFlag('telegram_avisados', JSON.stringify(fora)); // guarda o estado atual (quem voltou sai da lista e volta a avisar se cair de novo)
}
setInterval(() => { tickAlertasConexao().catch(() => { }); }, 15 * 60 * 1000); // checa a cada 15 min

// ======================= ENDPOINTS: SDR / BOAS-VINDAS (espelho) =======================
app.get('/sdr/resumo', (_req, res) => {
  try {
    const hoje = new Date().toISOString().slice(0, 10);
    const enviadosHoje = (db.prepare(`SELECT COUNT(*) n FROM bv_enviados WHERE date(sent_at)=date(?)`).get(hoje) as any)?.n || 0;
    const naFila = (db.prepare(`SELECT COUNT(*) n FROM bv_fila WHERE status='pendente'`).get() as any)?.n || 0;
    const seguidoresConhecidos = (db.prepare(`SELECT COUNT(*) n FROM bv_seguidores`).get() as any)?.n || 0;
    const porTipo = db.prepare(`SELECT tipo, COUNT(*) n FROM bv_fila WHERE status='pendente' GROUP BY tipo`).all();
    res.json({ ok: true, enviadosHoje, naFila, seguidoresConhecidos, porTipo, ativo: pubFlag('sdr_ativo') === '1' });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/sdr/fila', (req, res) => {
  try { res.json({ ok: true, itens: db.prepare(`SELECT * FROM bv_fila WHERE status=? ORDER BY not_before ASC LIMIT 500`).all(String((req.query as any).status || 'pendente')) }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= ENDPOINTS: AGENDA / REUNIÃO (ponte NOVA) =======================
// A SellFlux (ou o agente) chama isto quando qualifica e marca a reunião de diagnóstico.
// Grava a reunião e, quando o Google estiver conectado, cria o evento na agenda do dono.
app.post('/agenda/reuniao', async (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || randomUUID());
    const inicio = String(b.inicio || ''); if (!inicio) return res.json({ ok: false, erro: 'inicio_obrigatorio' });
    const fim = String(b.fim || new Date(new Date(inicio).getTime() + 45 * 60000).toISOString());
    const titulo = String(b.titulo || ('Diagnóstico — ' + (b.lead_nome || 'Lead')));
    db.prepare(`INSERT INTO reunioes_sdr(id,projeto,lead_nome,lead_telefone,lead_email,origem,titulo,inicio,fim,observacao,status,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET inicio=excluded.inicio, fim=excluded.fim, titulo=excluded.titulo, atualizado_em=excluded.atualizado_em`)
      .run(id, String(b.projeto || ''), b.lead_nome || null, b.lead_telefone || null, b.lead_email || null, b.origem || null, titulo, inicio, fim, b.observacao || null, 'agendada', agora(), agora());
    let googleEventId: string | null = null; let avisoGoogle = '';
    if (pubFlag('gcal_ativo') === '1' && _gcalFonteLigada('reuniao')) { // [gcal-fonte] toggle da reuniao manual
      try { googleEventId = await _gcalCriarEvento({ tipo: 'reuniao', refId: id, titulo, inicio, fim, descricao: (b.observacao || '') + '\nLead: ' + (b.lead_nome || '') + ' ' + (b.lead_telefone || ''), convidadoEmail: b.lead_email || '' }); }
      catch (e: any) { avisoGoogle = 'google_falhou: ' + String(e.message || e); }
      if (googleEventId) db.prepare(`UPDATE reunioes_sdr SET google_event_id=? WHERE id=?`).run(googleEventId, id);
    } else { avisoGoogle = 'google_nao_conectado'; }
    res.json({ ok: true, id, googleEventId, avisoGoogle });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/agenda/reunioes', (req, res) => {
  try {
    const dias = Math.min(180, Number((req.query as any).dias || 30));
    res.json({ ok: true, reunioes: db.prepare(`SELECT * FROM reunioes_sdr WHERE date(inicio)>=date('now','-2 day') AND date(inicio)<=date('now','+${dias} day') ORDER BY inicio ASC`).all() });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [gcal-config] status + liga/desliga por fonte o que a NUVEM manda pra Agenda do Google do Ezequias.
// Fontes que hoje criam evento no Google: 'ia' (quando o agente marca) e 'reuniao' (reuniao SDR manual).
const _GCAL_FONTES = ['ia', 'reuniao'];
app.get('/gcal/status', (_req, res) => {
  try {
    const fontes: any = {}; for (const f of _GCAL_FONTES) fontes[f] = _gcalFonteLigada(f);
    res.json({ ok: true, conectado: pubFlag('gcal_ativo') === '1', conta: pubFlag('gcal_conta', ''), calendario: pubFlag('gcal_calendar_id', 'primary'), fontes });
  } catch (e: any) { res.json({ ok: false, erro: String(e && e.message || e) }); }
});
app.get('/gcal/sync-config', (_req, res) => {
  try { const fontes: any = {}; for (const f of _GCAL_FONTES) fontes[f] = _gcalFonteLigada(f); res.json({ ok: true, conectado: pubFlag('gcal_ativo') === '1', fontes }); }
  catch (e: any) { res.json({ ok: false, erro: String(e && e.message || e) }); }
});
app.post('/gcal/sync-config', (req, res) => {
  try {
    const b: any = (req.body || {}).fontes || req.body || {};
    for (const f of _GCAL_FONTES) { if (b[f] !== undefined) setPubFlag('gcal_sync_' + f, (b[f] === true || b[f] === '1' || b[f] === 1 || b[f] === 'on') ? '1' : '0'); }
    const fontes: any = {}; for (const f of _GCAL_FONTES) fontes[f] = _gcalFonteLigada(f);
    res.json({ ok: true, fontes });
  } catch (e: any) { res.json({ ok: false, erro: String(e && e.message || e) }); }
});

// [gcal-connect] conectar o Google Agenda da NUVEM reaproveitando o app Google do YouTube (sem novo cadastro no Google Cloud).
const GCAL_SCOPE_CONNECT = 'https://www.googleapis.com/auth/calendar openid email';
// ======================= [num-escolha] Escolha do número de envio =======================
// Lista os números conectados por projeto + o padrão escolhido (pro app montar o seletor).
app.get('/config/numeros', (_req, res) => {
  const numeros = db.prepare(`SELECT projeto, instancia, telefone, status FROM numeros ORDER BY projeto, criado_em`).all();
  res.json({ ok: true, numeros, numeroPadrao: (CONFIG as any).numeroPadrao || {} });
});
// Define o número PADRÃO de envio de um projeto (nome da instância). instancia vazia = volta ao 1º conectado.
app.post('/config/numero-padrao', (req, res) => {
  const b = req.body || {};
  const projeto = String(b.projeto || '');
  if (!PROJETOS.includes(projeto as any)) return res.status(400).json({ ok: false, erro: 'projeto_invalido' });
  if (!(CONFIG as any).numeroPadrao) (CONFIG as any).numeroPadrao = {};
  const inst = String(b.instancia || '').trim();
  if (inst) (CONFIG as any).numeroPadrao[projeto] = inst; else delete (CONFIG as any).numeroPadrao[projeto];
  try { salvarConfig(); } catch (e: any) { return res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  res.json({ ok: true, numeroPadrao: (CONFIG as any).numeroPadrao });
});

// ======================= [motor2] Endpoints de gestão (trilha/tags/flags) =======================
// Deixam o app ligar/desligar e editar as tags por produto SEM redeploy. Exigem x-api-key (middleware global).
app.get('/motor2/config', (_req, res) => {
  res.json({
    ok: true,
    motor2_ativo: pubFlag('motor2_ativo', '0') === '1',
    agente_tag_gate: pubFlag('agente_tag_gate', '1') === '1',
    produtoTags: (CONFIG as any).produtoTags || [],
    agenteTagsGatilho: (CONFIG as any).agenteTagsGatilho || [],
    agenteTagsGatilhoEfetivo: _agenteTagsGatilho(),
  });
});
// Substitui a lista de tags por produto (array completo). Grava no config.json.
app.post('/motor2/produto-tags', (req, res) => {
  const b = req.body || {};
  if (!Array.isArray(b.produtoTags)) return res.status(400).json({ ok: false, erro: 'produtoTags_deve_ser_array' });
  (CONFIG as any).produtoTags = b.produtoTags.map((pt: any) => ({
    chave: String(pt?.chave || ''), porId: !!pt?.porId,
    tags: Array.isArray(pt?.tags) ? pt.tags.map((t: any) => String(t || '')).filter(Boolean) : [],
  })).filter((pt: any) => pt.chave);
  if (Array.isArray(b.agenteTagsGatilho)) (CONFIG as any).agenteTagsGatilho = b.agenteTagsGatilho.map((t: any) => String(t || '')).filter(Boolean);
  try { salvarConfig(); } catch (e: any) { return res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  res.json({ ok: true, produtoTags: (CONFIG as any).produtoTags, agenteTagsGatilho: (CONFIG as any).agenteTagsGatilho || [] });
});
// Liga/desliga as flags do motor2. flag = 'motor2_ativo' | 'agente_tag_gate'. on = true/false.
app.post('/motor2/flag', (req, res) => {
  const b = req.body || {};
  const flag = String(b.flag || '');
  if (flag !== 'motor2_ativo' && flag !== 'agente_tag_gate') return res.status(400).json({ ok: false, erro: 'flag_invalida' });
  const on = (b.on === true || b.on === '1' || b.on === 1 || b.on === 'on');
  setPubFlag(flag, on ? '1' : '0');
  res.json({ ok: true, flag, valor: pubFlag(flag, flag === 'agente_tag_gate' ? '1' : '0') === '1' });
});
// [#2 provedor trocável] Config do provedor de IA (base_url + chave + modelo). A CHAVE é guardada CIFRADA e nunca é devolvida.
app.get('/motor2/llm', (_req, res) => {
  const p = (CONFIG as any).llmProvider || {};
  const prov = _llmEfetivo();
  res.json({ ok: true, baseUrl: p.baseUrl || '', modelo: p.modelo || '', temChaveCustom: !!p.apiKeyEnc, usandoEnv: !p.apiKeyEnc && !!LLM_API_KEY, baseUrlEfetivo: prov.baseUrl, modeloPadraoEfetivo: prov.modeloPadrao });
});
app.post('/motor2/llm', (req, res) => {
  const b = req.body || {};
  if (!(CONFIG as any).llmProvider) (CONFIG as any).llmProvider = {};
  const p = (CONFIG as any).llmProvider;
  if (b.baseUrl !== undefined) p.baseUrl = String(b.baseUrl || '').trim();
  if (b.modelo !== undefined) p.modelo = String(b.modelo || '').trim();
  if (b.limparChave === true) { delete p.apiKeyEnc; }
  else if (b.apiKey && String(b.apiKey).trim()) { try { p.apiKeyEnc = cifra(String(b.apiKey).trim()); } catch (e: any) { return res.status(500).json({ ok: false, erro: 'falha ao cifrar: ' + String(e?.message || e) }); } }
  try { salvarConfig(); } catch (e: any) { return res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  res.json({ ok: true, baseUrl: p.baseUrl || '', modelo: p.modelo || '', temChaveCustom: !!p.apiKeyEnc });
});
// [#2 seletor de canal] Canal de Instagram por instrumento: 'off' | 'navegador' | 'api'. (WhatsApp é sempre via Evolution.)
app.get('/motor2/canais', (_req, res) => {
  res.json({ ok: true, canalIg: (CONFIG as any).canalIg || {} });
});
app.post('/motor2/canal-ig', (req, res) => {
  const b = req.body || {};
  const projeto = String(b.projeto || '');
  const modo = String(b.modo || 'off');
  if (!PROJETOS.includes(projeto as any)) return res.status(400).json({ ok: false, erro: 'projeto_invalido' });
  if (!['off', 'navegador', 'api'].includes(modo)) return res.status(400).json({ ok: false, erro: 'modo_invalido' });
  if (!(CONFIG as any).canalIg) (CONFIG as any).canalIg = {};
  (CONFIG as any).canalIg[projeto] = modo;
  try { salvarConfig(); } catch (e: any) { return res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  res.json({ ok: true, canalIg: (CONFIG as any).canalIg });
});

app.get('/gcal/connect', (_req, res) => {
  try {
    const { clientId } = pbCred('youtube');
    if (!clientId) return res.status(400).json({ ok: false, erro: 'App Google do YouTube nao configurado no motor.' });
    const params = new URLSearchParams({
      client_id: clientId, redirect_uri: pbYtRedirect(), response_type: 'code', scope: GCAL_SCOPE_CONNECT,
      access_type: 'offline', prompt: 'consent', include_granted_scopes: 'false', state: 'gcal:connect',
    });
    res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}` });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
app.get('/gcal/agendas', async (_req, res) => {
  try {
    const at = await _gcalAccessToken();
    const r: any = await fetch('https://www.googleapis.com/calendar/v3/users/me/calendarList?minAccessRole=writer', { headers: { Authorization: 'Bearer ' + at } });
    const j: any = await r.json();
    const agendas = (j.items || []).map((c: any) => ({ id: c.id, nome: c.summary, primary: !!c.primary }));
    res.json({ ok: true, agendas, atual: pubFlag('gcal_calendar_id', 'primary') });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});
app.post('/gcal/calendario', (req, res) => {
  try { const id = String((req.body || {}).calendarId || (req.body || {}).id || '').trim(); if (id) setPubFlag('gcal_calendar_id', id); res.json({ ok: true, calendario: pubFlag('gcal_calendar_id', 'primary') }); }
  catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});
app.post('/gcal/desconectar', (_req, res) => {
  try { setPubFlag('gcal_ativo', '0'); setPubFlag('gcal_refresh_enc', ''); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// ---- Google Calendar: refresh token + criar evento (gated por gcal_ativo) ----
async function _gcalAccessToken(): Promise<string> {
  const refreshEnc = pubFlag('gcal_refresh_enc', ''); const cid = pubFlag('gcal_client_id', ''); const csec = decifra(pubFlag('gcal_client_secret_enc', ''));
  const refresh = decifra(refreshEnc);
  if (!refresh || !cid) throw new Error('gcal_sem_credencial');
  const body = new URLSearchParams({ client_id: cid, client_secret: csec, refresh_token: refresh, grant_type: 'refresh_token' });
  const r: any = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
  const j: any = await r.json(); if (!j.access_token) throw new Error('gcal_token_falhou');
  return j.access_token;
}
async function _gcalCriarEvento(ev: { tipo: string; refId: string; titulo: string; inicio: string; fim: string; descricao?: string; convidadoEmail?: string }): Promise<string> {
  const at = await _gcalAccessToken();
  const corpo: any = { summary: ev.titulo, description: ev.descricao || '', start: { dateTime: ev.inicio, timeZone: 'America/Sao_Paulo' }, end: { dateTime: ev.fim, timeZone: 'America/Sao_Paulo' }, extendedProperties: { private: { motor: '1', tipo: ev.tipo, refId: ev.refId } } };
  if (ev.convidadoEmail) corpo.attendees = [{ email: ev.convidadoEmail }];
  const existe = db.prepare(`SELECT google_event_id FROM gcal_event_map WHERE tipo=? AND ref_id=?`).get(ev.tipo, ev.refId) as any;
  const cal = pubFlag('gcal_calendar_id', 'primary');
  const url = existe?.google_event_id ? `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(cal)}/events/${existe.google_event_id}` : `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(cal)}/events`;
  const r: any = await fetch(url, { method: existe?.google_event_id ? 'PUT' : 'POST', headers: { Authorization: 'Bearer ' + at, 'Content-Type': 'application/json' }, body: JSON.stringify(corpo) });
  const j: any = await r.json(); if (!j.id) throw new Error('gcal_evento_falhou: ' + JSON.stringify(j).slice(0, 200));
  db.prepare(`INSERT INTO gcal_event_map(tipo,ref_id,google_event_id,atualizado_em) VALUES(?,?,?,?) ON CONFLICT(tipo,ref_id) DO UPDATE SET google_event_id=excluded.google_event_id, atualizado_em=excluded.atualizado_em`).run(ev.tipo, ev.refId, j.id, agora());
  return j.id;
}

// ======================= OneDrive: token + limpeza de postados =======================
// Cliente público do rclone (etapa20c): permite conectar o OneDrive SEM registrar app no
// portal da Microsoft. O refresh_token vem do `rclone authorize onedrive` (login único).
const ONEDRIVE_RCLONE_CLIENT_ID = 'b15665d9-eda6-4092-8539-0eec376afd59';
async function _onedriveToken(): Promise<string> {
  const refresh = decifra(pubFlag('onedrive_refresh_enc', ''));
  if (!refresh) throw new Error('onedrive_sem_credencial');
  // MODO rclone (sem portal): cliente público, SEM secret, endpoint /common/.
  if (pubFlag('onedrive_modo', '') === 'rclone') {
    const body = new URLSearchParams({ client_id: ONEDRIVE_RCLONE_CLIENT_ID, refresh_token: refresh, grant_type: 'refresh_token', scope: 'Files.ReadWrite offline_access' });
    const r: any = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
    const j: any = await r.json(); if (!j.access_token) throw new Error('onedrive_token_falhou: ' + String(j.error_description || j.error || '?'));
    if (j.refresh_token) setPubFlag('onedrive_refresh_enc', cifra(String(j.refresh_token))); // rotação do refresh
    return j.access_token;
  }
  // MODO app próprio (portal): client_id + secret + /consumers/ (mantido).
  const cid = pubFlag('onedrive_client_id', ''); const csec = decifra(pubFlag('onedrive_client_secret_enc', ''));
  if (!cid) throw new Error('onedrive_sem_credencial');
  const body = new URLSearchParams({ client_id: cid, client_secret: csec, refresh_token: refresh, grant_type: 'refresh_token', scope: 'offline_access Files.ReadWrite' });
  const r: any = await fetch('https://login.microsoftonline.com/consumers/oauth2/v2.0/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
  const j: any = await r.json(); if (!j.access_token) throw new Error('onedrive_token_falhou'); return j.access_token;
}
async function _onedriveApagar(itemId: string): Promise<boolean> {
  const at = await _onedriveToken();
  const r: any = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(itemId)}`, { method: 'DELETE', headers: { Authorization: 'Bearer ' + at } });
  return r.status === 204 || r.status === 404;
}
// Worker de limpeza: apaga do OneDrive os vídeos JÁ POSTADOS (regra do PC, adaptada):
// só apaga quando status='publicado' E tem onedrive_item_id E passou o período de carência.
// Mantém a linha no banco (histórico). Inerte até onedrive_ativo=1 e limpeza_ativa=1.
let _limpezaRodando = false;
async function tickLimpezaOneDrive() {
  if (_limpezaRodando) return; if (pubFlag('onedrive_ativo') !== '1' || pubFlag('limpeza_ativa') !== '1') return;
  _limpezaRodando = true;
  try {
    const graceDias = Number(pubFlag('limpeza_grace_dias', '3'));
    const alvos = db.prepare(`SELECT id, onedrive_item_id FROM fila_publicacao WHERE status='publicado' AND arquivo_apagado=0 AND onedrive_item_id IS NOT NULL AND date(atualizado_em)<=date('now','-${graceDias} day') LIMIT 20`).all() as any[];
    for (const it of alvos) {
      try { const ok = await _onedriveApagar(String(it.onedrive_item_id)); if (ok) db.prepare(`UPDATE fila_publicacao SET arquivo_apagado=1, data_arquivo_apagado=? WHERE id=?`).run(agora(), it.id); }
      catch (e) { /* tenta na próxima rodada */ }
    }
  } finally { _limpezaRodando = false; }
}
setInterval(() => { tickLimpezaOneDrive().catch(() => {}); }, 60 * 60 * 1000); // 1x/hora

app.get('/pub/onedrive/status', (_req, res) => {
  try {
    res.json({ ok: true, conectado: pubFlag('onedrive_ativo') === '1', limpeza_ativa: pubFlag('limpeza_ativa') === '1', grace_dias: Number(pubFlag('limpeza_grace_dias', '3')),
      aAapagar: (db.prepare(`SELECT COUNT(*) n FROM fila_publicacao WHERE status='publicado' AND arquivo_apagado=0 AND onedrive_item_id IS NOT NULL`).get() as any)?.n || 0,
      jaApagados: (db.prepare(`SELECT COUNT(*) n FROM fila_publicacao WHERE arquivo_apagado=1`).get() as any)?.n || 0 });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/pub/flags', (req, res) => {
  try { const b = req.body || {}; for (const k of Object.keys(b)) if (/^[a-z0-9_]+$/.test(k)) setPubFlag(k, String(b[k])); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/pub/flags', (_req, res) => {
  try { res.json({ ok: true, flags: db.prepare(`SELECT chave, valor FROM pub_kv`).all() }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ======================= OneDrive: CONECTAR (login único) + LISTAR + BAIXAR (etapa20b) =======================
// client_id/secret do app da Microsoft e a pasta "pra postar" ficam no pub_kv (secret cifrado).
// redirect_uri FIXO = PUBLIC_BASE_URL + /pub/onedrive/callback (cadastrar IGUAL no app da Microsoft).
const _odvRedirect = () => (PUBLIC_BASE_URL || '') + '/pub/onedrive/callback';
const _ODV_SCOPE = 'offline_access Files.ReadWrite';

// Salva client_id + client_secret (cifrado) + a pasta de vídeos (ex: Melodias/PraPostar).
app.post('/pub/onedrive/config', (req, res) => {
  try {
    const b = req.body || {};
    if (b.client_id != null) setPubFlag('onedrive_client_id', String(b.client_id).trim());
    if (b.client_secret != null && String(b.client_secret).trim()) setPubFlag('onedrive_client_secret_enc', cifra(String(b.client_secret).trim()));
    if (b.pasta != null) setPubFlag('onedrive_pasta', String(b.pasta).replace(/^\/+|\/+$/g, ''));
    res.json({ ok: true, client_id: pubFlag('onedrive_client_id', ''), pasta: pubFlag('onedrive_pasta', ''), redirect_uri: _odvRedirect(), tem_secret: !!pubFlag('onedrive_client_secret_enc', '') });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// Monta a URL de login da Microsoft (o usuário abre, loga e autoriza 1 vez).
app.get('/pub/onedrive/connect', (_req, res) => {
  try {
    const cid = pubFlag('onedrive_client_id', '');
    if (!cid) return res.json({ ok: false, erro: 'configure_client_id_primeiro' });
    const p = new URLSearchParams({ client_id: cid, response_type: 'code', redirect_uri: _odvRedirect(), response_mode: 'query', scope: _ODV_SCOPE });
    res.json({ ok: true, url: 'https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?' + p.toString(), redirect_uri: _odvRedirect() });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// A Microsoft redireciona pra cá com ?code=... — trocamos por refresh_token e salvamos.
app.get('/pub/onedrive/callback', async (req, res) => {
  const code = String((req.query as any).code || '');
  const erro = String((req.query as any).error_description || (req.query as any).error || '');
  if (erro) return res.status(400).send('<h3>Falhou: ' + erro.replace(/[<>]/g, '') + '</h3>');
  if (!code) return res.status(400).send('<h3>Sem código. Tente conectar de novo.</h3>');
  try {
    const cid = pubFlag('onedrive_client_id', ''); const csec = decifra(pubFlag('onedrive_client_secret_enc', ''));
    const body = new URLSearchParams({ client_id: cid, client_secret: csec, code, grant_type: 'authorization_code', redirect_uri: _odvRedirect(), scope: _ODV_SCOPE });
    const r: any = await fetch('https://login.microsoftonline.com/consumers/oauth2/v2.0/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
    const j: any = await r.json();
    if (!j.refresh_token) return res.status(400).send('<h3>Não veio o refresh_token. Confira o app na Microsoft (precisa do offline_access). Resposta: ' + String(j.error_description || j.error || '?').replace(/[<>]/g, '') + '</h3>');
    setPubFlag('onedrive_refresh_enc', cifra(String(j.refresh_token)));
    setPubFlag('onedrive_ativo', '1');
    res.send('<html><body style="font-family:sans-serif;background:#0f1115;color:#e6e8ec;padding:40px"><h2>✅ OneDrive conectado!</h2><p>Pode fechar esta aba e voltar pro app.</p></body></html>');
  } catch (e: any) { res.status(500).send('<h3>Erro ao conectar: ' + String(e?.message || e).replace(/[<>]/g, '') + '</h3>'); }
});

// Lista os arquivos (vídeos) da pasta "pra postar" no OneDrive.
async function _onedriveListar(): Promise<any[]> {
  const at = await _onedriveToken();
  const pasta = pubFlag('onedrive_pasta', '');
  const url = pasta
    ? `https://graph.microsoft.com/v1.0/me/drive/root:/${pasta.split('/').map(encodeURIComponent).join('/')}:/children?$top=200`
    : 'https://graph.microsoft.com/v1.0/me/drive/root/children?$top=200';
  const r: any = await fetch(url, { headers: { Authorization: 'Bearer ' + at } });
  const j: any = await r.json();
  if (!j.value) throw new Error('listar_falhou: ' + String(j.error?.message || r.status));
  return j.value.filter((x: any) => x.file).map((x: any) => ({ id: x.id, nome: x.name, tamanho: x.size, criado: x.createdDateTime }));
}

// Baixa um item do OneDrive pro disco do servidor (usado pelos uploaders — próxima etapa).
async function _onedriveBaixar(itemId: string, destPath: string): Promise<string> {
  const at = await _onedriveToken();
  const r: any = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(itemId)}/content`, { headers: { Authorization: 'Bearer ' + at } });
  if (!r.ok || !r.body) throw new Error('baixar_falhou_' + r.status);
  const { Readable } = await import('node:stream');
  try { fs.mkdirSync(destPath.replace(/\/[^/]*$/, ''), { recursive: true }); } catch {}
  await new Promise<void>((resolve, reject) => {
    const ws = fs.createWriteStream(destPath);
    (Readable as any).fromWeb(r.body).pipe(ws); ws.on('finish', () => resolve()); ws.on('error', reject);
  });
  return destPath;
}
void _onedriveBaixar; // ainda não chamado aqui (usado na etapa do publicador)

// Testa a conexão: lista a pasta configurada (não posta nada).
app.get('/pub/onedrive/testar', async (_req, res) => {
  try { res.json({ ok: true, pasta: pubFlag('onedrive_pasta', '') || '(raiz)', arquivos: await _onedriveListar() }); }
  catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// [lives-extras] lista arquivos de uma SUBPASTA do OneDrive (pro seletor de arquivo do app:
// escolher vídeo/capa por instrumento sem digitar). Ex.: GET /pub/onedrive/listar?pasta=Lives/Teclado&tipo=video
// tipo = video | imagem | todos. Devolve caminho RELATIVO à base (serve direto como fonte_ref/capa_ref).
app.get('/pub/onedrive/listar', (req, res) => {
  try {
    const pasta = String(req.query.pasta || '').replace(/^\/+|\/+$/g, '');
    const tipo = String(req.query.tipo || 'todos').toLowerCase();
    const EXTV = ['mp4', 'mov', 'mkv', 'webm', 'avi', 'm4v', 'flv'];
    const EXTI = ['png', 'jpg', 'jpeg', 'webp', 'bmp', 'gif'];
    const rows = (_odRcloneListar(pasta || undefined) as any[]) || [];
    const out = rows.filter((r) => !r.IsDir).map((r) => {
      const rel = String(r.Path || r.Name || '');
      const nome = String(r.Name || rel.split('/').pop() || rel);
      const ext = String(nome.split('.').pop() || '').toLowerCase();
      return { nome, caminho: (pasta ? pasta + '/' : '') + rel, tamanho: Number(r.Size || 0), ext };
    }).filter((r) => tipo === 'video' ? EXTV.includes(r.ext) : (tipo === 'imagem' ? EXTI.includes(r.ext) : true))
      .sort((a, b) => String(a.nome).localeCompare(String(b.nome)));
    res.json({ ok: true, pasta, arquivos: out });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// O APP pede um token CURTO pra subir vídeo direto PC->OneDrive (mesma conexão). Atrás da x-api-key.
app.get('/pub/onedrive/token-app', async (_req, res) => {
  try { res.json({ ok: true, access_token: await _onedriveToken(), pasta: pubFlag('onedrive_pasta', '') }); }
  catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// MODO SEM PORTAL (etapa20c): cola o token do `rclone authorize onedrive`. Aceita o JSON
// inteiro do rclone OU só o refresh_token. Liga o modo rclone (cliente público, sem secret).
app.post('/pub/onedrive/config-rclone', (req, res) => {
  try {
    const b = req.body || {};
    let refresh = String(b.refresh_token || '').trim();
    if (!refresh && b.token != null) {
      const raw = b.token;
      try { const t = typeof raw === 'string' ? JSON.parse(raw) : raw; if (t && t.refresh_token) refresh = String(t.refresh_token); } catch { /* não era JSON */ }
      if (!refresh) { const m = String(raw).match(/"refresh_token"\s*:\s*"([^"]+)"/); if (m) refresh = m[1]; }
    }
    if (!refresh) return res.json({ ok: false, erro: 'sem_refresh_token (cole o token do rclone ou só o refresh_token)' });
    setPubFlag('onedrive_refresh_enc', cifra(refresh));
    setPubFlag('onedrive_modo', 'rclone');
    setPubFlag('onedrive_client_id', ONEDRIVE_RCLONE_CLIENT_ID);
    setPubFlag('onedrive_ativo', '1');
    if (b.pasta != null) setPubFlag('onedrive_pasta', String(b.pasta).replace(/^\/+|\/+$/g, ''));
    res.json({ ok: true, modo: 'rclone', pasta: pubFlag('onedrive_pasta', '') });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

console.log('[etapa16] módulo de publicação na nuvem carregado (inerte até configurar tokens/sessões)');
console.log('[etapa20b] OneDrive: conectar(login)/config/callback/listar/baixar/testar prontos');
console.log('[etapa20c] OneDrive SEM portal: config-rclone (cliente público rclone) pronto');



// ============================================================================
// ETAPA 17 — Construtor de fluxo visual (campanhas + pastas) — ADITIVO
// Guarda os fluxos montados no canvas (estilo SellFlux) do app: campanhas de
// "venda em grupos" e individuais. Só armazenamento + listagem aqui; a EXECUÇÃO
// real (interpretar nós -> enfileirar disparos) entra na próxima etapa.
// Namespace: /fluxo/*
// ============================================================================
try {
  db.exec(`
  CREATE TABLE IF NOT EXISTS fluxo_pastas (
    id TEXT PRIMARY KEY, nome TEXT, tipo TEXT, criado_em TEXT
  );
  CREATE TABLE IF NOT EXISTS fluxo_campanhas (
    id TEXT PRIMARY KEY, nome TEXT, pasta_id TEXT, tipo TEXT DEFAULT 'grupos',
    ativo INTEGER DEFAULT 0, fluxo_json TEXT, qtd_nos INTEGER DEFAULT 0,
    criado_em TEXT, atualizado_em TEXT
  );
  CREATE INDEX IF NOT EXISTS ix_fluxo_camp_tipo ON fluxo_campanhas(tipo, atualizado_em);
  `);
} catch (e) { console.error('[etapa17] schema erro (ignorado):', e); }

function _contaNos(fluxoJson: string): number {
  try { const o = JSON.parse(fluxoJson || '{}'); return Array.isArray(o.nodes) ? o.nodes.length : 0; } catch { return 0; }
}

app.get('/fluxo/campanhas', (req, res) => {
  try {
    const tipo = String((req.query as any).tipo || '');
    const wc = tipo ? ` WHERE tipo=?` : ``;
    const campanhas = db.prepare(`SELECT id, nome, pasta_id, ativo, qtd_nos, atualizado_em FROM fluxo_campanhas${wc} ORDER BY atualizado_em DESC`).all(...(tipo ? [tipo] : [])) as any[];
    const pastas = db.prepare(`SELECT id, nome FROM fluxo_pastas${tipo ? ' WHERE tipo=?' : ''} ORDER BY nome`).all(...(tipo ? [tipo] : [])) as any[];
    res.json({ ok: true, campanhas, pastas });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.get('/fluxo/campanha/:id', (req, res) => {
  try {
    const row = db.prepare(`SELECT id, nome, pasta_id, tipo, ativo, fluxo_json FROM fluxo_campanhas WHERE id=?`).get(String(req.params.id)) as any;
    if (!row) return res.json({ ok: false, erro: 'nao_encontrada' });
    res.json({ ok: true, campanha: row });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.post('/fluxo/campanha', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || randomUUID());
    const fluxoJson = typeof b.fluxo_json === 'string' ? b.fluxo_json : JSON.stringify(b.fluxo_json || { nodes: [], edges: [] });
    const qtd = _contaNos(fluxoJson);
    const ex = db.prepare(`SELECT id FROM fluxo_campanhas WHERE id=?`).get(id);
    if (ex) {
      db.prepare(`UPDATE fluxo_campanhas SET nome=?, pasta_id=?, tipo=?, ativo=?, fluxo_json=?, qtd_nos=?, atualizado_em=? WHERE id=?`)
        .run(String(b.nome || 'Sem nome'), b.pasta_id || null, String(b.tipo || 'grupos'), b.ativo ? 1 : 0, fluxoJson, qtd, agora(), id);
    } else {
      db.prepare(`INSERT INTO fluxo_campanhas(id,nome,pasta_id,tipo,ativo,fluxo_json,qtd_nos,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`)
        .run(id, String(b.nome || 'Sem nome'), b.pasta_id || null, String(b.tipo || 'grupos'), b.ativo ? 1 : 0, fluxoJson, qtd, agora(), agora());
    }
    // ao salvar ATIVADA, já compila/agenda os disparos (Etapa 18); desativada, cancela os pendentes
    try {
      db.prepare(`DELETE FROM fila_envio WHERE campanha_id=? AND status='camp_pend'`).run(id);
      if ((b.ativo ? 1 : 0) === 1) { const _c = db.prepare(`SELECT id, tipo, fluxo_json FROM fluxo_campanhas WHERE id=?`).get(id) as any; if (_c) _compilarFluxo(_c, false); }
    } catch (_e) { /* compilacao nunca derruba o salvar */ }
    res.json({ ok: true, id });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.post('/fluxo/campanha/excluir', (req, res) => {
  try { db.prepare(`DELETE FROM fluxo_campanhas WHERE id=?`).run(String((req.body || {}).id || '')); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

app.post('/fluxo/pasta', (req, res) => {
  try {
    const b = req.body || {}; const nome = String(b.nome || '').trim(); if (!nome) return res.json({ ok: false, erro: 'nome' });
    const id = String(b.id || randomUUID());
    db.prepare(`INSERT INTO fluxo_pastas(id,nome,tipo,criado_em) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET nome=excluded.nome`).run(id, nome, String(b.tipo || 'grupos'), agora());
    res.json({ ok: true, id });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [+merge] acrescenta nós/arestas a uma campanha existente (monta fluxos grandes em partes, sem reenviar tudo).
app.post('/fluxo/campanha/merge', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || '');
    const row = db.prepare(`SELECT fluxo_json FROM fluxo_campanhas WHERE id=?`).get(id) as any;
    if (!row) return res.json({ ok: false, erro: 'nao_encontrada' });
    let fx: any = {}; try { fx = JSON.parse(row.fluxo_json || '{}'); } catch { fx = {}; }
    const nodes: any[] = Array.isArray(fx.nodes) ? fx.nodes : [];
    const edges: any[] = Array.isArray(fx.edges) ? fx.edges : [];
    const temN = new Set(nodes.map((n: any) => String(n.id)));
    for (const n of (Array.isArray(b.nodes) ? b.nodes : [])) { if (n && !temN.has(String(n.id))) { nodes.push(n); temN.add(String(n.id)); } }
    const temE = new Set(edges.map((e: any) => String(e.id)));
    for (const e of (Array.isArray(b.edges) ? b.edges : [])) { if (e && !temE.has(String(e.id))) { edges.push(e); temE.add(String(e.id)); } }
    if (Array.isArray(b.instancias)) (fx as any).instancias = b.instancias;
    fx.nodes = nodes; fx.edges = edges;
    const fluxoJson = JSON.stringify(fx);
    db.prepare(`UPDATE fluxo_campanhas SET fluxo_json=?, qtd_nos=?, atualizado_em=? WHERE id=?`).run(fluxoJson, _contaNos(fluxoJson), agora(), id);
    res.json({ ok: true, id, nos: nodes.length, arestas: edges.length });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[etapa17] módulo de fluxo visual (campanhas/pastas) carregado');



// ============================================================================
// ETAPA 18 — EXECUÇÃO do fluxo visual de grupos — ADITIVO
// Interpreta o desenho salvo (nodes/edges) da campanha e AGENDA os disparos de
// verdade na fila_envio existente (status 'camp_pend' -> tickCampanha envia,
// respeitando agendado_para/anti-ban). Compila ao ATIVAR; cancela ao desativar.
// Tipos de nó: tag (entrada por tags do grupo), timer (espera), whatsapp (blocos),
// editar_grupos (add/remove tag no grupo), nota (ignorado).
// ============================================================================
try {
  db.exec(`CREATE TABLE IF NOT EXISTS grupo_tags (grupo_id TEXT, tag TEXT, criado_em TEXT, PRIMARY KEY(grupo_id, tag));`);
} catch (e) { console.error('[etapa18] grupo_tags erro:', e); }
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN campanha_id TEXT`); } catch {}
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN no_id TEXT`); } catch {} // [etapa27] bloco de origem no canvas (contador por bloco)

// ---- tags de grupo (pro bloco Tag escolher a audiência) ----
app.post('/fluxo/grupo/tag', (req, res) => {
  try {
    const b = req.body || {}; const gid = String(b.grupo_id || ''); const tag = String(b.tag || '').trim();
    if (!gid || !tag) return res.json({ ok: false, erro: 'grupo_id/tag' });
    if (b.remover) db.prepare(`DELETE FROM grupo_tags WHERE grupo_id=? AND tag=?`).run(gid, tag);
    else db.prepare(`INSERT OR IGNORE INTO grupo_tags(grupo_id,tag,criado_em) VALUES(?,?,?)`).run(gid, tag, agora());
    res.json({ ok: true });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/fluxo/grupo/tags', (req, res) => {
  try {
    const gid = String((req.query as any).grupo_id || '');
    if (gid) return res.json({ ok: true, tags: db.prepare(`SELECT tag FROM grupo_tags WHERE grupo_id=?`).all(gid) });
    res.json({ ok: true, tags: db.prepare(`SELECT tag, COUNT(*) n FROM grupo_tags GROUP BY tag ORDER BY n DESC`).all() });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

function _msUnidade(u: string): number {
  const s = String(u || '').toLowerCase();
  if (s.startsWith('min')) return 60 * 1000;
  if (s.startsWith('hor')) return 3600 * 1000;
  return 86400 * 1000; // dias (padrão)
}
// [timer-agenda] Timer com AGENDA ABSOLUTA (dia da semana + horário, ou data específica), em horário de Brasília (UTC-3).
// Retorna o instante (ms UTC) da PRÓXIMA ocorrência a partir de baseMs, ou null se o timer for do tipo "esperar X" (relativo).
function _proximoTimerAbsoluto(d: any, baseMs: number): number | null {
  try {
    const dataEsp = String((d && d.data_especifica) || '').trim();
    const temDias = !!(d && d.dias_on) && d.dias && Object.values(d.dias).some(Boolean);
    if (!dataEsp && !temDias) return null; // sem agenda absoluta => comportamento relativo
    const hhmm = String((d && d.iniciar) || '').trim() || '00:00';
    const hh = Number(hhmm.split(':')[0]) || 0;
    const mm = Number(hhmm.split(':')[1]) || 0;
    const BRT = -3; // Brasil (sem horário de verão). hh:mm BRT -> UTC = hh - BRT (= hh+3).
    if (dataEsp) {
      // [fix-timer] data_especifica pode vir "YYYY-MM-DD" ou "YYYY-MM-DDTHH:MM" (datetime-local).
      const mEsp = dataEsp.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/);
      if (!mEsp) return null;
      const yEsp = Number(mEsp[1]); const moEsp = Number(mEsp[2]) - 1; const daEsp = Number(mEsp[3]);
      const hEsp = mEsp[4] != null ? Number(mEsp[4]) : hh; // hora do proprio datetime; senao o iniciar
      const miEsp = mEsp[5] != null ? Number(mEsp[5]) : mm;
      return Date.UTC(yEsp, moEsp, daEsp, hEsp - BRT, miEsp);
    }
    const mapDia: any = { dom: 0, seg: 1, ter: 2, qua: 3, qui: 4, sex: 5, sab: 6 };
    const alvo = new Set<number>();
    for (const k of Object.keys(d.dias || {})) if (d.dias[k] && mapDia[k] != null) alvo.add(mapDia[k]);
    if (!alvo.size) return null;
    const nowBRT = new Date(baseMs + BRT * 3600000); // ler com getUTC* dá o relógio de Brasília
    for (let i = 0; i < 14; i++) {
      const cand = new Date(Date.UTC(nowBRT.getUTCFullYear(), nowBRT.getUTCMonth(), nowBRT.getUTCDate() + i));
      if (!alvo.has(cand.getUTCDay())) continue;
      const ms = Date.UTC(cand.getUTCFullYear(), cand.getUTCMonth(), cand.getUTCDate(), hh - BRT, mm);
      if (ms > baseMs + 1000) return ms;
    }
    return null;
  } catch { return null; }
}
// [timer-agenda] O fluxo é recorrente (agenda semanal por dia da semana, sem data fixa)? Usado pra re-armar toda semana.
function _fluxoRecorrente(fluxoJson: string): boolean {
  try {
    const f = JSON.parse(fluxoJson || '{}'); const ns = Array.isArray(f.nodes) ? f.nodes : [];
    return ns.some((n: any) => { const d = (n && (n.data || n.d)) || {}; return _tipoNo(n) === 'timer' && !!d.dias_on && d.dias && Object.values(d.dias).some(Boolean) && !String(d.data_especifica || '').trim() && String(d.aba || '') !== 'espera'; });
  } catch { return false; }
}
// resolve os grupos-alvo: precisam ter TODAS as tags listadas
function _gruposPorTags(tags: string[]): any[] {
  const ts = (tags || []).map((t) => String(t).trim()).filter(Boolean);
  if (!ts.length) return [];
  const ph = ts.map(() => '?').join(',');
  const ids = db.prepare(`SELECT DISTINCT grupo_id FROM grupo_tags WHERE tag IN (${ph})`).all(...ts) as any[]; // [tag-ou] grupo com QUALQUER uma das tags (OU)
  if (!ids.length) return [];
  const ph2 = ids.map(() => '?').join(',');
  return db.prepare(`SELECT id, projeto, instancia, nome FROM grupos_wpp WHERE id IN (${ph2})`).all(...ids.map((r) => r.grupo_id)) as any[];
}
function _dataDoNo(n: any): any { return (n && (n.data || n.d)) || {}; }
function _tipoNo(n: any): string { return String((n && (n.type || n.tipo)) || ''); }

// [etapa27] resolve os CONTATOS-alvo (individual): precisam ter TODAS as tags listadas
function _contatosPorTags(tags: string[]): any[] {
  const ts = (tags || []).map((t) => String(t).trim()).filter(Boolean);
  if (!ts.length) return [];
  const ph = ts.map(() => '?').join(',');
  const ids = db.prepare(`SELECT ct.contato_id FROM contato_tags ct JOIN tags t ON t.id=ct.tag_id WHERE t.nome IN (${ph}) GROUP BY ct.contato_id HAVING COUNT(DISTINCT t.nome)=?`).all(...ts, ts.length) as any[];
  if (!ids.length) return [];
  const ph2 = ids.map(() => '?').join(',');
  return db.prepare(`SELECT id, projeto, telefone FROM contatos WHERE id IN (${ph2}) AND telefone IS NOT NULL AND telefone<>''`).all(...ids.map((r) => r.contato_id)) as any[];
}

// [etapa27] compila um fluxo de canvas do tipo INDIVIDUAL -> enfileira pra CONTATOS (is_grupo=0). Espelha _compilarFluxo.
function _compilarFluxoIndividual(camp: any, simular = false): { grupos: number; mensagens: number; ate: string; erros: string[] } {
  const erros: string[] = [];
  let fluxo: any = {}; try { fluxo = JSON.parse(camp.fluxo_json || '{}'); } catch { return { grupos: 0, mensagens: 0, ate: '', erros: ['fluxo_json invalido'] }; }
  const nodes: any[] = Array.isArray(fluxo.nodes) ? fluxo.nodes : [];
  const edges: any[] = Array.isArray(fluxo.edges) ? fluxo.edges : [];
  if (!nodes.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['sem blocos'] };
  const byId: any = {}; for (const n of nodes) byId[n.id] = n;
  const prox: any = {}; for (const e of edges) { (prox[e.source] = prox[e.source] || []).push(e.target); }
  const tagNodes = nodes.filter((n) => _tipoNo(n) === 'tag');
  if (!tagNodes.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['sem bloco Tag de entrada'] };
  const mapa: any = {};
  for (const tn of tagNodes) for (const c of _contatosPorTags(_dataDoNo(tn).tags || [])) mapa[c.id] = c;
  const selInst: string[] = Array.isArray((fluxo as any).instancias) ? (fluxo as any).instancias.map((x: any) => String(x)).filter(Boolean) : []; // [+seletor-numero]
  const contatos = Object.values(mapa) as any[];
  if (!contatos.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['nenhum contato com essas tags'] };
  let inicio: string | null = null;
  for (const tn of tagNodes) { const alvo = (prox[tn.id] || [])[0]; if (alvo) { inicio = alvo; break; } }
  if (!inicio) return { grupos: contatos.length, mensagens: 0, ate: '', erros: ['o bloco Tag não está ligado a nada'] };
  const base = Date.now();
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,agendado_para,criado_em,campanha_id,no_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  let offset = 0; let msgs = 0; let maxWhen = base;
  const padrao = (CONFIG as any).projetoPadrao || 'teclado';
  const mapaMidia: any = { imagem: 'image', video: 'video', audio: 'audio', documento: 'documento' }; // [+documento]
  const andar = db.transaction(() => {
    let atual: string | null = inicio; let guard = 0; const visto: any = {};
    while (atual && guard < 200) {
      guard++; if (visto[atual]) break; visto[atual] = 1;
      const n = byId[atual]; if (!n) break;
      const t = _tipoNo(n); const d = _dataDoNo(n);
      if (t === 'timer') {
        const abs = _proximoTimerAbsoluto(d, base);
        if (abs != null) offset = Math.max(0, abs - base); // agenda absoluta (dia+hora BRT)
        else { const val = Math.max(0, Number(d.valor ?? 1)); offset += val * _msUnidade(d.unidade || 'Dias'); } // esperar X
      }
      else if (t === 'whatsapp') {
        const blocos = _resolverBlocosOd(d.blocos || []).filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
        contatos.forEach((c: any, ci: number) => {
          blocos.forEach((b: any, bi: number) => {
            const when = new Date(base + offset + ci * 12000 + bi * 2000);
            if (when.getTime() > maxWhen) maxWhen = when.getTime();
            const tipo = b.tipo === 'texto' ? 'texto' : (mapaMidia[b.midia_tipo] || 'documento');
            const _inst = selInst.length ? selInst[ci % selInst.length] : null; // [+seletor-numero]
            const _st = _inst ? 'cmp_multi' : 'camp_pend';
            if (!simular) ins.run(randomUUID(), c.projeto || padrao, String(c.telefone), 0, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), _st, _inst, when.toISOString(), agora(), String(camp.id), String(n.id));
            msgs++;
          });
        });
      }
      // editar_grupos / nota: ignorados no individual
      atual = (prox[atual] || [])[0] || null;
    }
  });
  andar();
  return { grupos: contatos.length, mensagens: msgs, ate: new Date(maxWhen).toISOString(), erros };
}

// compila o fluxo -> enfileira em fila_envio (status camp_pend). Retorna resumo.
function _compilarFluxo(camp: any, simular = false): { grupos: number; mensagens: number; ate: string; erros: string[] } {
  if (String(camp?.tipo || '') === 'ig_individual') return { grupos: 0, mensagens: 0, ate: '', erros: ['Instagram ainda não conectado (falta a permissão instagram_manage_messages da Meta). O fluxo está salvo e dispara automaticamente quando o Instagram for conectado.'] }; // [+instagram] casca: nunca envia por WhatsApp
  if (String(camp?.tipo || '') === 'individual') return _compilarFluxoIndividual(camp, simular); // [etapa27] canvas individual (contatos) — caminho dos grupos fica intacto
  const erros: string[] = [];
  let fluxo: any = {}; try { fluxo = JSON.parse(camp.fluxo_json || '{}'); } catch { return { grupos: 0, mensagens: 0, ate: '', erros: ['fluxo_json invalido'] }; }
  const nodes: any[] = Array.isArray(fluxo.nodes) ? fluxo.nodes : [];
  const edges: any[] = Array.isArray(fluxo.edges) ? fluxo.edges : [];
  if (!nodes.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['sem blocos'] };
  const byId: any = {}; for (const n of nodes) byId[n.id] = n;
  const prox: any = {}; for (const e of edges) { (prox[e.source] = prox[e.source] || []).push(e.target); }
  // entradas = todos os nós 'tag'; audiência = união dos grupos
  const tagNodes = nodes.filter((n) => _tipoNo(n) === 'tag');
  if (!tagNodes.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['sem bloco Tag de entrada'] };
  const mapaGrupos: any = {};
  for (const tn of tagNodes) for (const g of _gruposPorTags(_dataDoNo(tn).tags || [])) mapaGrupos[g.id] = g;
  const selInst: string[] = Array.isArray((fluxo as any).instancias) ? (fluxo as any).instancias.map((x: any) => String(x)).filter(Boolean) : []; // [+seletor-numero]
  const grupos = Object.values(mapaGrupos) as any[];
  const gruposUso = (grupos as any[]).filter((g: any) => { const i = String(g.instancia || ''); return !!i && (!selInst.length || selInst.includes(i)); }); // [+seletor-numero] só grupos cujo número está dentro (e, se houver seleção, só os escolhidos)
  if (!grupos.length) return { grupos: 0, mensagens: 0, ate: '', erros: ['nenhum grupo com essas tags (marque as tags nos grupos em Gerenciar Grupos)'] };
  // começa a andar a partir do que o(s) Tag apontam
  let inicio: string | null = null;
  for (const tn of tagNodes) { const alvo = (prox[tn.id] || [])[0]; if (alvo) { inicio = alvo; break; } }
  if (!inicio) return { grupos: grupos.length, mensagens: 0, ate: '', erros: ['o bloco Tag não está ligado a nada'] };

  const base = Date.now();
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,agendado_para,criado_em,campanha_id,no_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); // [etapa27] +no_id (bloco de origem) [+seletor-numero]
  let offset = 0; let msgs = 0; let maxWhen = base;
  const padrao = (CONFIG as any).projetoPadrao || 'teclado';
  const mapaMidia: any = { imagem: 'image', video: 'video', audio: 'audio', documento: 'documento' }; // [+documento]
  const andar = db.transaction(() => {
    let atual: string | null = inicio; let guard = 0;
    const visto: any = {};
    while (atual && guard < 200) {
      guard++; if (visto[atual]) break; visto[atual] = 1;
      const n = byId[atual]; if (!n) break;
      const t = _tipoNo(n); const d = _dataDoNo(n);
      if (t === 'timer') {
        const abs = _proximoTimerAbsoluto(d, base);
        if (abs != null) offset = Math.max(0, abs - base); // [timer-agenda] agenda absoluta (dia da semana + horário BRT, ou data específica)
        else { const val = Math.max(0, Number(d.valor ?? 1)); offset += val * _msUnidade(d.unidade || 'Dias'); } // esperar X min/horas/dias
      } else if (t === 'whatsapp') {
        const blocos = _resolverBlocosOd(d.blocos || []).filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
        gruposUso.forEach((g: any, gi: number) => {
          blocos.forEach((b: any, bi: number) => {
            const when = new Date(base + offset + gi * 12000 + bi * 2000);
            if (when.getTime() > maxWhen) maxWhen = when.getTime();
            const tipo = b.tipo === 'texto' ? 'texto' : (mapaMidia[b.midia_tipo] || 'documento');
            if (!simular) ins.run(randomUUID(), g.projeto || padrao, String(g.id), 1, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), 'cmp_multi', String(g.instancia || ''), when.toISOString(), agora(), String(camp.id), String(n.id)); // [etapa27] +no_id [+seletor-numero] envia pela instância do grupo
            msgs++;
          });
        });
      } else if (t === 'editar_grupos') {
        const acao = String(d.acao || ''); const tag = String(d.tag || '').trim();
        if (!simular && tag) for (const g of grupos) {
          if (acao === 'remove_tag') db.prepare(`DELETE FROM grupo_tags WHERE grupo_id=? AND tag=?`).run(String(g.id), tag);
          else db.prepare(`INSERT OR IGNORE INTO grupo_tags(grupo_id,tag,criado_em) VALUES(?,?,?)`).run(String(g.id), tag, agora());
        }
      } // nota = ignora
      atual = (prox[atual] || [])[0] || null;
    }
  });
  andar();
  return { grupos: grupos.length, mensagens: msgs, ate: new Date(maxWhen).toISOString(), erros };
}

// prévia (não grava nada) — pra conferir antes de ativar
app.get('/fluxo/campanha/:id/previa', (req, res) => {
  try {
    const camp = db.prepare(`SELECT id, tipo, fluxo_json FROM fluxo_campanhas WHERE id=?`).get(String(req.params.id)) as any;
    if (!camp) return res.json({ ok: false, erro: 'nao_encontrada' });
    res.json({ ok: true, previa: _compilarFluxo(camp, true) });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ativar/desativar = compila (agenda) ou cancela os disparos pendentes da campanha
app.post('/fluxo/campanha/ativar', (req, res) => {
  try {
    const b = req.body || {}; const id = String(b.id || ''); const ativo = b.ativo ? 1 : 0;
    const camp = db.prepare(`SELECT id, tipo, fluxo_json FROM fluxo_campanhas WHERE id=?`).get(id) as any;
    if (!camp) return res.json({ ok: false, erro: 'nao_encontrada' });
    // sempre limpa os pendentes antigos dessa campanha
    db.prepare(`DELETE FROM fila_envio WHERE campanha_id=? AND status='camp_pend'`).run(id);
    db.prepare(`UPDATE fluxo_campanhas SET ativo=?, atualizado_em=? WHERE id=?`).run(ativo, agora(), id);
    if (!ativo) return res.json({ ok: true, ativo: 0, cancelados: true });
    const r = _compilarFluxo(camp, false);
    _salvarEnvio({ canal: String(camp.tipo || '') === 'ig_individual' ? 'instagram' : 'whatsapp', tipo: 'fluxo', payload: { campanhaId: id, tipoFluxo: camp.tipo, agendados: r.mensagens, grupos: r.grupos } });
    res.json({ ok: true, ativo: 1, agendados: r.mensagens, grupos: r.grupos, ate: r.ate, erros: r.erros });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [timer-agenda] RE-ARMA fluxos recorrentes (agenda semanal): quando não resta envio pendente da campanha
// ativa, recompila pra agendar a PRÓXIMA ocorrência (ex.: toda segunda 11:00). Roda a cada 5 min.
setInterval(() => {
  try {
    const camps = db.prepare(`SELECT id, tipo, fluxo_json FROM fluxo_campanhas WHERE ativo=1`).all() as any[];
    for (const c of camps) {
      try {
        if (!_fluxoRecorrente(c.fluxo_json)) continue;
        const pend = db.prepare(`SELECT COUNT(*) n FROM fila_envio WHERE campanha_id=? AND status IN ('camp_pend','cmp_multi')`).get(c.id) as any;
        if ((pend && pend.n) > 0) continue; // ainda tem envio agendado -> não mexe
        _compilarFluxo(c, false); // agenda a próxima ocorrência
      } catch { /* best-effort por campanha */ }
    }
  } catch { /* best-effort */ }
}, 300000);

// status de execução de uma campanha (quantos pendentes/enviados/falhou)
app.get('/fluxo/campanha/:id/status', (req, res) => {
  try {
    const id = String(req.params.id);
    const porStatus = db.prepare(`SELECT status, COUNT(*) n FROM fila_envio WHERE campanha_id=? GROUP BY status`).all(id);
    const prox = db.prepare(`SELECT MIN(agendado_para) p FROM fila_envio WHERE campanha_id=? AND status IN ('camp_pend','cmp_multi')`).get(id) as any;
    res.json({ ok: true, porStatus, proximoEnvio: prox?.p || null });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// [etapa27] contagem POR BLOCO (no_id) da campanha — pro contador embaixo de cada bloco no canvas
app.get('/fluxo/campanha/:id/status-nos', (req, res) => {
  try {
    const id = String(req.params.id);
    const rows = db.prepare(`SELECT no_id, status, COUNT(*) n FROM fila_envio WHERE campanha_id=? AND no_id IS NOT NULL GROUP BY no_id, status`).all(id) as any[];
    const porNo: Record<string, { aguardando: number; contatados: number; falha: number }> = {};
    for (const r of rows) {
      const k = String(r.no_id);
      if (!porNo[k]) porNo[k] = { aguardando: 0, contatados: 0, falha: 0 };
      const st = String(r.status || '').toLowerCase();
      if (st === 'falhou') porNo[k].falha += r.n;
      else if (st === 'enviado') porNo[k].contatados += r.n;
      else porNo[k].aguardando += r.n;
    }
    res.json({ ok: true, porNo });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[etapa27] contador por bloco (no_id) + disparo INDIVIDUAL (canvas tipo=individual, lista de contatos, resumo-individual) carregado');

console.log('[etapa18] execução de fluxo de grupos carregada (compila -> fila_envio; inerte até ativar)');


// ============================================================================
// ETAPA 19 — CONEXÕES / DISPOSITIVOS (gerenciar números no app) — ADITIVO
// Dá ao Ezequias autonomia pra, DENTRO do app (aba Sellflux), ver o status de cada
// número, reconectar sozinho (QR ou código de pareamento), reiniciar, desconectar,
// adicionar e excluir — igual a tela "Dispositivos" da SellFlux. Reaproveita os
// helpers que já existem (evo/statusNumero/normalizaEstado/registrarInstanciaExistente/
// projetoDaInstancia) e a tabela numeros. Namespace: /conexoes/*
// ============================================================================
function rotuloEstado(e: any): string {
  const s = String(e || '').toLowerCase();
  if (s === 'open') return 'Conectado';
  if (s === 'connecting') return 'Conectando';
  if (s === 'oficial') return 'API Oficial';
  return 'Desconectado';
}

// Lista TODAS as conexões: junta o que o Evolution conhece (não-oficial/QR) com o
// cadastro local (tabela numeros), pra pegar o projeto certo e também os números
// de API Oficial (que não aparecem no Evolution). Atualiza o status de passagem.
app.get('/conexoes/listar', async (_req, res) => {
  try {
    let evoArr: any[] = [];
    try {
      const todas = await evo('/instance/fetchInstances', 'GET');
      evoArr = Array.isArray(todas) ? todas : (todas?.instances || []);
    } catch { /* Evolution fora do ar -> mostra só o cadastro */ }
    const porNome: Record<string, any> = {};
    for (const i of evoArr) {
      const nome = i?.name || i?.instance?.instanceName || i?.instanceName;
      if (!nome) continue;
      const estado = String(i?.connectionStatus || i?.instance?.state || i?.state || 'close').toLowerCase();
      const owner = i?.ownerJid || i?.instance?.owner || i?.owner || '';
      const tel = String(owner).replace(/@.*/, '') || String(i?.number || '');
      const profile = i?.profileName || i?.instance?.profileName || '';
      porNome[nome] = { instancia: nome, estado, telefone: tel, profile };
      const row = db.prepare(`SELECT id FROM numeros WHERE instancia=?`).get(nome) as any;
      if (row) db.prepare(`UPDATE numeros SET status=?, telefone=COALESCE(NULLIF(?,''),telefone) WHERE instancia=?`).run(normalizaEstado(estado), tel, nome);
    }
    const cadastro = db.prepare(`SELECT instancia, projeto, telefone, tipo, status FROM numeros`).all() as any[];
    const saida: any[] = []; const vistos = new Set<string>();
    for (const c of cadastro) {
      vistos.add(c.instancia);
      const ev = porNome[c.instancia];
      const estado = ev ? ev.estado : (c.tipo === 'oficial' ? 'oficial' : 'close');
      saida.push({
        instancia: c.instancia, projeto: c.projeto,
        telefone: (ev && ev.telefone) || c.telefone || '',
        profile: ev ? ev.profile : '', tipo: c.tipo || 'nao-oficial',
        estado, estadoLabel: rotuloEstado(estado),
      });
    }
    for (const nome in porNome) {
      if (vistos.has(nome)) continue;
      const ev = porNome[nome];
      saida.push({ instancia: nome, projeto: projetoDaInstancia(nome), telefone: ev.telefone, profile: ev.profile, tipo: 'nao-oficial', estado: ev.estado, estadoLabel: rotuloEstado(ev.estado), naoCadastrado: true });
    }
    res.json({ ok: true, conexoes: saida });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Estado de uma conexão (pro app ficar fazendo polling enquanto o QR não é lido).
app.get('/conexoes/estado/:instancia', async (req, res) => {
  try {
    const inst = String(req.params.instancia);
    const st = await statusNumero(inst);
    const e = st?.instance?.state ?? st?.state;
    try { db.prepare(`UPDATE numeros SET status=? WHERE instancia=?`).run(normalizaEstado(e), inst); } catch {}
    res.json({ ok: true, estado: String(e || '').toLowerCase(), estadoLabel: rotuloEstado(e) });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Reconectar: devolve o QR (base64 = imagem pronta pro app) e o code.
app.post('/conexoes/conectar', async (req, res) => {
  try {
    const instancia = String((req.body || {}).instancia || '');
    if (!instancia) return res.json({ ok: false, erro: 'instancia' });
    let qr: any;
    try { qr = await evo(`/instance/connect/${encodeURIComponent(instancia)}`, 'GET'); }
    catch {
      try { await evo('/instance/create', 'POST', { instanceName: instancia, integration: 'WHATSAPP-BAILEYS', qrcode: true }); } catch {}
      qr = await evo(`/instance/connect/${encodeURIComponent(instancia)}`, 'GET');
    }
    res.json({ ok: true, base64: qr?.base64 || '', code: qr?.code || '', pairingCode: qr?.pairingCode || null });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Reconectar por CÓDIGO DE PAREAMENTO (mais fácil num celular só): desloga pra zerar
// a sessão de QR e pede o connect com o número -> volta um código de 8 dígitos.
app.post('/conexoes/pareamento', async (req, res) => {
  try {
    const b = req.body || {}; const instancia = String(b.instancia || ''); const numero = String(b.numero || '').replace(/\D/g, '');
    if (!instancia || !numero) return res.json({ ok: false, erro: 'instancia/numero' });
    try { await evo(`/instance/logout/${encodeURIComponent(instancia)}`, 'DELETE'); } catch {}
    await new Promise((r) => setTimeout(r, 1500));
    let qr: any;
    try { qr = await evo(`/instance/connect/${encodeURIComponent(instancia)}?number=${numero}`, 'GET'); }
    catch {
      try { await evo('/instance/create', 'POST', { instanceName: instancia, integration: 'WHATSAPP-BAILEYS', number: numero, qrcode: true }); } catch {}
      qr = await evo(`/instance/connect/${encodeURIComponent(instancia)}?number=${numero}`, 'GET');
    }
    res.json({ ok: true, pairingCode: qr?.pairingCode || null, base64: qr?.base64 || '', code: qr?.code || '' });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Reiniciar a instância (quando trava em "connecting").
app.post('/conexoes/reiniciar', async (req, res) => {
  try {
    const instancia = String((req.body || {}).instancia || '');
    if (!instancia) return res.json({ ok: false, erro: 'instancia' });
    const r = await evo(`/instance/restart/${encodeURIComponent(instancia)}`, 'POST');
    res.json({ ok: true, resultado: r });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Desconectar (logout) sem apagar a instância.
app.post('/conexoes/desconectar', async (req, res) => {
  try {
    const instancia = String((req.body || {}).instancia || '');
    if (!instancia) return res.json({ ok: false, erro: 'instancia' });
    const r = await evo(`/instance/logout/${encodeURIComponent(instancia)}`, 'DELETE');
    try { db.prepare(`UPDATE numeros SET status='desconectado' WHERE instancia=?`).run(instancia); } catch {}
    res.json({ ok: true, resultado: r });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Adicionar NOVO número (cria instância Baileys + registra no projeto) e já devolve o QR.
app.post('/conexoes/criar', async (req, res) => {
  try {
    const b = req.body || {}; const projeto = String(b.projeto || '');
    if (!ehProjeto(projeto)) return res.json({ ok: false, erro: 'projeto_invalido' });
    let nome = String(b.nome || '').trim().replace(/[^a-zA-Z0-9-]/g, '');
    if (!nome) nome = `${projeto}-${Date.now().toString(36)}`;
    const numero = String(b.numero || '').replace(/\D/g, '');
    try { await evo('/instance/create', 'POST', { instanceName: nome, integration: 'WHATSAPP-BAILEYS', qrcode: true, ...(numero ? { number: numero } : {}) }); } catch { /* talvez já exista */ }
    const existe = db.prepare(`SELECT id FROM numeros WHERE instancia=?`).get(nome) as any;
    if (!existe) db.prepare(`INSERT INTO numeros(id,projeto,telefone,instancia,status,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), projeto, numero, nome, 'aguardando_qr', agora());
    else db.prepare(`UPDATE numeros SET projeto=? WHERE instancia=?`).run(projeto, nome);
    let qr: any; try { qr = await evo(`/instance/connect/${encodeURIComponent(nome)}${numero ? `?number=${numero}` : ''}`, 'GET'); } catch {}
    res.json({ ok: true, instancia: nome, base64: qr?.base64 || '', code: qr?.code || '', pairingCode: qr?.pairingCode || null });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Registrar no projeto uma instância que apareceu no Evolution mas não estava no cadastro.
app.post('/conexoes/registrar', async (req, res) => {
  try {
    const b = req.body || {}; const projeto = String(b.projeto || ''); const instancia = String(b.instancia || '');
    if (!ehProjeto(projeto) || !instancia) return res.json({ ok: false, erro: 'projeto/instancia' });
    res.json(await registrarInstanciaExistente(projeto, instancia, String(b.telefone || '') || undefined));
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// Excluir a conexão (logout + delete no Evolution + tira do cadastro). Ação do dono na UI.
app.post('/conexoes/excluir', async (req, res) => {
  try {
    const instancia = String((req.body || {}).instancia || '');
    if (!instancia) return res.json({ ok: false, erro: 'instancia' });
    try { await evo(`/instance/logout/${encodeURIComponent(instancia)}`, 'DELETE'); } catch {}
    try { await evo(`/instance/delete/${encodeURIComponent(instancia)}`, 'DELETE'); } catch (e: any) { return res.json({ ok: false, erro: String(e.message || e) }); }
    try { db.prepare(`DELETE FROM numeros WHERE instancia=?`).run(instancia); } catch {}
    res.json({ ok: true });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// ============================================================================
// ETAPA 21 — rclone OneDrive: o MOTOR pega a mídia do OneDrive e usa. ADITIVO.
// O binário rclone (instalado no container) usa o remote "onedrive" já logado
// em /root/.config/rclone/rclone.conf (montado via docker-compose). O motor BAIXA
// o arquivo do OneDrive pra /data/media e serve em /media/<arq> — rota PÚBLICA,
// pois a Evolution busca a mídia por uma URL INTERNA (http://engine:8080) na mesma
// rede docker. Assim vídeo/imagem/áudio ficam SÓ no OneDrive e o motor posta 24/7
// (PC desligado). Namespaces: /pub/onedrive/rclone-*  e  /disparo/enviar-onedrive
// ============================================================================
const RCLONE_REMOTE = process.env.ONEDRIVE_REMOTE || 'onedrive';
const ONEDRIVE_PASTA_BASE = process.env.ONEDRIVE_PASTA_BASE || 'Postagem Automatica';
const MEDIA_DIR = '/data/media';
// URL que a EVOLUTION usa pra baixar a mídia DO engine (mesma rede docker; a própria
// Evolution já fala com http://engine:8080 no WEBHOOK_GLOBAL_URL).
const MEDIA_INTERNAL_BASE = process.env.MEDIA_INTERNAL_BASE || 'http://engine:8080';
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch { /* ok */ }

function _rclone(args: string[], opts: { timeoutMs?: number } = {}): string {
  return execFileSync('rclone', args, {
    encoding: 'utf8',
    timeout: opts.timeoutMs || 120000,
    maxBuffer: 64 * 1024 * 1024,
  });
}
function _rcloneOk(): boolean {
  try { _rclone(['version'], { timeoutMs: 10000 }); return true; } catch { return false; }
}
/** Monta remote:"Base/rel" sem duplicar a pasta base. */
function _odRemotePath(rel: string): string {
  let r = String(rel || '').replace(/^\/+/, '');
  const base = ONEDRIVE_PASTA_BASE.replace(/^\/+|\/+$/g, '');
  if (base && r.toLowerCase() !== base.toLowerCase() && !r.toLowerCase().startsWith(base.toLowerCase() + '/')) {
    r = base + '/' + r;
  }
  return `${RCLONE_REMOTE}:${r}`;
}
/** Lista (recursivo, só arquivos) a pasta base do OneDrive. */
function _odRcloneListar(subpasta?: string): any[] {
  const alvo = subpasta ? _odRemotePath(subpasta) : `${RCLONE_REMOTE}:${ONEDRIVE_PASTA_BASE}`;
  try { return JSON.parse(_rclone(['lsjson', '-R', '--files-only', alvo], { timeoutMs: 90000 })); }
  catch { return []; }
}
/** Baixa 1 arquivo do OneDrive pra /data/media (cache por hash). Devolve {file,url,nome}. */
function _odRcloneBaixar(rel: string): { file: string; url: string; nome: string } {
  const remote = _odRemotePath(rel);
  const nome = (String(rel).split('/').pop() || 'arquivo').split('?')[0];
  const ext = (nome.includes('.') ? nome.split('.').pop() : '') || '';
  const safe = createHash('sha1').update(remote).digest('hex').slice(0, 16) + (ext ? '.' + ext.toLowerCase() : '');
  const dest = MEDIA_DIR + '/' + safe;
  if (!fs.existsSync(dest)) _rclone(['copyto', remote, dest], { timeoutMs: 180000 });
  return { file: dest, url: `${MEDIA_INTERNAL_BASE}/media/${safe}`, nome };
}

// [lives-fonte-grande 13/10] Videos de aula sao grandes (varios GB). Em vez de baixar o
// arquivo inteiro com limite de 3min (estourava ETIMEDOUT), este bloco: (1) PRE-BAIXA em
// segundo plano ao agendar (sem limite, atomico via .part); (2) na live usa o LOCAL se ja
// estiver pronto; (3) senao TRANSMITE DIRETO do OneDrive via `rclone serve http` (comeca na
// hora) e segue baixando pro futuro; (4) se o serve nao subir, espera o download e usa local.
// NAO altera o fluxo de publicacao (pbBaixarMidia/_odRcloneBaixar seguem iguais).
function _odDestPath(rel: string): string {
  const remote = _odRemotePath(rel);
  const nome = (String(rel).split('/').pop() || 'arquivo').split('?')[0];
  const ext = (nome.includes('.') ? nome.split('.').pop() : '') || '';
  const safe = createHash('sha1').update(remote).digest('hex').slice(0, 16) + (ext ? '.' + ext.toLowerCase() : '');
  return MEDIA_DIR + '/' + safe;
}
function _odRelToBase(rel: string): string {
  let r = String(rel || '').replace(/^\/+/, '');
  const base = ONEDRIVE_PASTA_BASE.replace(/^\/+|\/+$/g, '');
  if (base && r.toLowerCase().startsWith(base.toLowerCase() + '/')) r = r.slice(base.length + 1);
  else if (base && r.toLowerCase() === base.toLowerCase()) r = '';
  return r;
}
const _odBaixandoGrande = new Map<string, Promise<string>>();
// [lives-barra] progresso do pre-download (0..100) por arquivo, pra barrinha na aba Transmissoes
const _odProgresso = new Map<string, { rel: string; baixados: number; total: number; pct: number; estado: string; ts: number }>();
function _odProgSet(dest: string, rel: string, patch: { baixados?: number; total?: number; pct?: number; estado?: string }): void {
  const cur = _odProgresso.get(dest) || { rel, baixados: 0, total: 0, pct: 0, estado: 'baixando', ts: Date.now() };
  if (patch.baixados != null) cur.baixados = patch.baixados;
  if (patch.total != null) cur.total = patch.total;
  if (patch.estado != null) cur.estado = patch.estado;
  if (patch.pct != null) cur.pct = patch.pct;
  else if (cur.total > 0) cur.pct = Math.min(100, Math.max(0, Math.round((cur.baixados / cur.total) * 100)));
  cur.rel = rel; cur.ts = Date.now();
  _odProgresso.set(dest, cur);
}
function _odTamanhoRemotoAsync(rel: string, cb: (n: number) => void): void {
  try {
    const c = spawn('rclone', ['lsjson', _odRemotePath(rel)], { stdio: ['ignore', 'pipe', 'ignore'] });
    let o = ''; try { c.stdout.on('data', (d: any) => { o += String(d); }); } catch { /* */ }
    c.on('close', () => { try { const a = JSON.parse(o); cb(Array.isArray(a) && a[0] && a[0].Size != null ? Number(a[0].Size) : 0); } catch { cb(0); } });
    c.on('error', () => cb(0));
  } catch { cb(0); }
}
function _odBaixarGrande(rel: string): Promise<string> {
  const dest = _odDestPath(rel);
  try { if (fs.existsSync(dest) && fs.statSync(dest).size > 0) return Promise.resolve(dest); } catch { /* */ }
  const existente = _odBaixandoGrande.get(dest);
  if (existente) return existente;
  const remote = _odRemotePath(rel);
  const part = dest + '.part';
  // [lives-barra] acompanha o tamanho do .part vs total pra alimentar a barrinha
  let _progTimer: any = null;
  try {
    _odProgSet(dest, rel, { baixados: 0, estado: 'baixando' });
    _odTamanhoRemotoAsync(rel, (t) => { if (t > 0) _odProgSet(dest, rel, { total: t }); });
    _progTimer = setInterval(() => {
      try {
        if (!_odBaixandoGrande.has(dest)) {
          clearInterval(_progTimer);
          try { const ok = fs.existsSync(dest) && fs.statSync(dest).size > 0; _odProgSet(dest, rel, ok ? { pct: 100, estado: 'pronto' } : { estado: 'erro' }); } catch { /* */ }
          return;
        }
        let sz = 0; try { sz = fs.statSync(part).size; } catch { /* */ }
        _odProgSet(dest, rel, { baixados: sz });
      } catch { /* */ }
    }, 1500);
  } catch { /* */ }
  const p = new Promise<string>((resolve, reject) => {
    try { if (fs.existsSync(part)) fs.unlinkSync(part); } catch { /* */ }
    const ch = spawn('rclone', ['copyto', remote, part, '--transfers', '1', '--low-level-retries', '10'], { stdio: ['ignore', 'ignore', 'pipe'] });
    let err = '';
    try { ch.stderr.on('data', (d: any) => { err = (err + String(d)).slice(-800); }); } catch { /* */ }
    ch.on('error', (e: any) => reject(e));
    ch.on('close', (code: number) => {
      if (code === 0 && fs.existsSync(part)) { try { fs.renameSync(part, dest); } catch (e2) { /* */ } resolve(dest); }
      else { try { if (fs.existsSync(part)) fs.unlinkSync(part); } catch { /* */ } reject(new Error('rclone copyto code ' + code + ' ' + err)); }
    });
  }).then((f) => { _odBaixandoGrande.delete(dest); return f; }, (e) => { _odBaixandoGrande.delete(dest); throw e; });
  _odBaixandoGrande.set(dest, p);
  return p;
}
let _odServeProc: any = null;
const OD_SERVE_PORT = Number(process.env.OD_SERVE_PORT || 8699);
function _odServeBaseUrl(): string {
  const url = 'http://127.0.0.1:' + OD_SERVE_PORT;
  if (_odServeProc && !_odServeProc.killed) return url;
  const base = `${RCLONE_REMOTE}:${ONEDRIVE_PASTA_BASE}`;
  try {
    _odServeProc = spawn('rclone', ['serve', 'http', base, '--addr', '127.0.0.1:' + OD_SERVE_PORT, '--read-only', '--vfs-cache-mode', 'off', '--dir-cache-time', '15s'], { stdio: ['ignore', 'ignore', 'ignore'] });
    _odServeProc.on('exit', () => { _odServeProc = null; });
    _odServeProc.on('error', () => { _odServeProc = null; });
  } catch { _odServeProc = null; }
  return url;
}
async function _odServeUrl(rel: string): Promise<string> {
  const baseUrl = _odServeBaseUrl();
  let pronto = false;
  for (let i = 0; i < 24; i++) {
    try { const r = await fetch(baseUrl + '/', { method: 'GET' }); if (r && (r.ok || r.status === 200)) { pronto = true; break; } } catch { /* subindo */ }
    await new Promise((rs) => setTimeout(rs, 500));
  }
  if (!pronto) throw new Error('rclone serve http nao subiu');
  const relBase = _odRelToBase(rel);
  return baseUrl + '/' + relBase.split('/').map(encodeURIComponent).join('/');
}
async function livResolverInput(fonteTipo: string, fonteRef: string): Promise<{ input: string; modo: string; preInput: string[] }> {
  if (String(fonteTipo) === 'url') return { input: String(fonteRef || ''), modo: 'url', preInput: [] };
  const rel = String(fonteRef || '');
  if (!rel) return { input: '', modo: 'vazio', preInput: [] };
  const dest = _odDestPath(rel);
  try { if (fs.existsSync(dest) && fs.statSync(dest).size > 0) return { input: dest, modo: 'local', preInput: [] }; } catch { /* */ }
  _odBaixarGrande(rel).then(() => { /* */ }).catch(() => { /* */ });
  try {
    const url = await _odServeUrl(rel);
    if (url) return { input: url, modo: 'direto', preInput: ['-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5'] };
  } catch { /* cai pro download */ }
  const f = await _odBaixarGrande(rel);
  return { input: f, modo: 'local-apos-espera', preInput: [] };
}
function livPreBaixarFonte(fonteTipo: string, fonteRef: string, capaRef?: string): void {
  try {
    if (String(fonteTipo || 'onedrive') !== 'url' && fonteRef) _odBaixarGrande(String(fonteRef)).then(() => { /* */ }).catch(() => { /* */ });
    if (capaRef) _odBaixarGrande(String(capaRef)).then(() => { /* */ }).catch(() => { /* */ });
  } catch { /* */ }
}

// serve a mídia baixada (rota liberada no middleware acima)
app.use('/media', express.static(MEDIA_DIR));

// [lives-barra] progresso do carregamento (pre-download) das lives -> barrinha na aba Transmissoes
app.get('/lives/progresso', (_req, res) => {
  try {
    const linhas = db.prepare(`SELECT id, titulo, status, fonte_tipo, fonte_ref FROM fila_lives WHERE status IN ('agendado','no_ar','preparando','baixando') ORDER BY id DESC`).all() as any[];
    const itens = (linhas || []).map((r: any) => {
      const rel = String(r.fonte_ref || '');
      const base = { id: r.id, titulo: r.titulo, status: r.status };
      if (!rel || String(r.fonte_tipo || 'onedrive') === 'url') return { ...base, pct: null, estado: 'na', baixados: 0, total: 0 };
      const dest = _odDestPath(rel);
      let pronto = false; try { pronto = fs.existsSync(dest) && fs.statSync(dest).size > 0; } catch { /* */ }
      if (pronto) return { ...base, pct: 100, estado: 'pronto', baixados: 0, total: 0 };
      const pr = _odProgresso.get(dest);
      if (pr) return { ...base, pct: pr.pct, estado: pr.estado, baixados: pr.baixados, total: pr.total };
      return { ...base, pct: 0, estado: 'aguardando', baixados: 0, total: 0 };
    });
    res.json({ ok: true, itens });
  } catch (e: any) { res.json({ ok: false, erro: String((e && e.message) || e) }); }
});

// status do rclone/OneDrive no motor
app.get('/pub/onedrive/rclone-status', (_req, res) => {
  try {
    const ok = _rcloneOk();
    let remotes = ''; try { remotes = _rclone(['listremotes'], { timeoutMs: 10000 }).trim(); } catch { /* */ }
    res.json({ ok, rclone: ok, remotes, remote: RCLONE_REMOTE, base: ONEDRIVE_PASTA_BASE });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// lista a mídia do OneDrive (pro app conferir/escolher)
app.get('/pub/onedrive/rclone-listar', (req, res) => {
  try {
    if (!_rcloneOk()) return res.json({ ok: false, erro: 'rclone_indisponivel' });
    const sub = String((req.query || {}).subpasta || '') || undefined;
    res.json({ ok: true, base: ONEDRIVE_PASTA_BASE, arquivos: _odRcloneListar(sub) });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// baixa 1 arquivo e devolve a URL interna /media (teste/uso avulso)
app.post('/pub/onedrive/rclone-baixar', (req, res) => {
  try {
    if (!_rcloneOk()) return res.json({ ok: false, erro: 'rclone_indisponivel' });
    const rel = String((req.body || {}).caminho || (req.body || {}).rel || '');
    if (!rel) return res.json({ ok: false, erro: 'informe caminho (ex: "Melodias e Riffs Teclado/Reels-Short/x.mp4")' });
    const r = _odRcloneBaixar(rel);
    res.json({ ok: true, url: r.url, nome: r.nome });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// DISPARO com mídia vinda do OneDrive. Igual /disparo/enviar, mas os blocos de mídia
// trazem { od: "caminho/no/onedrive" }: o motor baixa de lá (1x, cache /data/media) e
// troca por uma URL interna /media/<arq> que a Evolution busca. Reusa _enfileiraDisparo.
app.post('/disparo/enviar-onedrive', (req, res) => {
  try {
    if (!_rcloneOk()) return res.json({ ok: false, erro: 'rclone_indisponivel' });
    const p = req.body || {};
    const blocos = (p.blocos || []).map((b: any) => {
      if (b && b.tipo && b.tipo !== 'texto' && (b.od || b.onedrive)) {
        const r = _odRcloneBaixar(String(b.od || b.onedrive));
        return { ...b, url: r.url };
      }
      return b;
    });
    const n = _enfileiraDisparo({ ...p, blocos });
    res.json({ ok: true, enfileirados: n });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[etapa21] rclone OneDrive no motor: /media (público interno) + /pub/onedrive/rclone-status|listar|baixar + /disparo/enviar-onedrive');

// ============================================================================
// [+envios-salvos +biblioteca-onedrive] (14/10) — add-only
//  (1) ENVIOS SALVOS: todo envio cai automatico num historico; da pra nomear,
//      duplicar (reenviar com edicoes) e excluir. Cobre disparo rapido, multi,
//      lote, sequencia, fluxo e Instagram — nos 5 lugares do app.
//  (2) BIBLIOTECA ONEDRIVE p/ fluxos: _resolverBlocosOd resolve blocos de midia
//      que apontam pra arquivo no OneDrive (campo od) -> URL publica; /midia/upload
//      sobe arquivo (img/pdf/audio/doc) pro OneDrive; /fluxo/campanha/duplicar.
// ============================================================================
try {
  db.exec(`
  CREATE TABLE IF NOT EXISTS envios_salvos (
    id TEXT PRIMARY KEY, nome TEXT, canal TEXT, tipo TEXT, origem TEXT, projeto TEXT,
    resumo TEXT, payload_json TEXT, criado_em TEXT, atualizado_em TEXT
  );
  CREATE INDEX IF NOT EXISTS ix_envios_canal ON envios_salvos(canal, atualizado_em);
  `);
} catch (e) { console.error('[envios] schema erro (ignorado):', e); }

function _resumoEnvioBlocos(blocos: any[]): string {
  try { return (blocos || []).map((b: any) => b.tipo === 'texto' ? '📝' : b.tipo === 'video' ? '🎬' : b.tipo === 'audio' ? '🎤' : (b.tipo === 'documento' || b.tipo === 'document') ? '📎' : '🖼️').join(' '); } catch { return ''; }
}
// salva um envio no historico — NUNCA quebra o disparo (try/catch total).
function _salvarEnvio(info: { canal?: string; tipo?: string; origem?: string; payload?: any; nome?: string }) {
  try {
    const p = info.payload || {};
    const blocos = Array.isArray(p.blocos) ? p.blocos : (Array.isArray(p.sequencia) && p.sequencia[0] && Array.isArray(p.sequencia[0].blocos) ? p.sequencia[0].blocos : []);
    let resumo = _resumoEnvioBlocos(blocos);
    if (Array.isArray(p.sequencia) && p.sequencia.length) resumo += ` · ${p.sequencia.length} passo(s)`;
    if (info.tipo === 'fluxo') resumo = `🧩 fluxo` + (p.grupos ? ` · ${p.grupos} alvo(s)` : '');
    const nome = String(info.nome || p.nomeEnvio || '').trim() || `Envio ${new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })}`; // [+hora-brasilia]
    db.prepare(`INSERT INTO envios_salvos(id,nome,canal,tipo,origem,projeto,resumo,payload_json,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?,?)`)
      .run(randomUUID(), nome, String(info.canal || 'whatsapp'), String(info.tipo || 'disparo'), String(info.origem || p.origem || ''), String(p.projeto || ''), resumo, JSON.stringify(p), agora(), agora());
  } catch (e) { /* historico e best-effort */ }
}

// resolve blocos de midia do OneDrive (campo od) -> URL publica p/ a Evolution. Guard: so com od e sem url.
function _resolverBlocosOd(blocos: any[]): any[] {
  try {
    return (blocos || []).map((b: any) => {
      if (b && b.tipo && b.tipo !== 'texto' && (b.od || b.onedrive) && !String(b.url || '').trim()) {
        try { return { ...b, url: _odBaixarPub(String(b.od || b.onedrive)) }; } catch { return b; }
      }
      return b;
    });
  } catch { return blocos || []; }
}

// --- Historico de envios (endpoints) ---
app.get('/envios', (req, res) => {
  try {
    const conds: string[] = []; const args: any[] = [];
    for (const k of ['canal', 'tipo', 'origem', 'projeto']) { const v = String((req.query as any)[k] || ''); if (v) { conds.push(`${k}=?`); args.push(v); } }
    const where = conds.length ? ('WHERE ' + conds.join(' AND ')) : '';
    const rows = db.prepare(`SELECT id,nome,canal,tipo,origem,projeto,resumo,criado_em,atualizado_em FROM envios_salvos ${where} ORDER BY datetime(atualizado_em) DESC, datetime(criado_em) DESC LIMIT 300`).all(...args);
    res.json({ ok: true, envios: rows });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.get('/envios/:id', (req, res) => {
  try { const r = db.prepare(`SELECT * FROM envios_salvos WHERE id=?`).get(String(req.params.id)) as any; if (!r) return res.json({ ok: false, erro: 'nao_encontrado' }); let payload: any = {}; try { payload = JSON.parse(r.payload_json || '{}'); } catch { /* */ } res.json({ ok: true, envio: { ...r, payload } }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/envios/salvar', (req, res) => {
  try { const b = req.body || {}; _salvarEnvio({ canal: b.canal, tipo: b.tipo, origem: b.origem, nome: b.nome, payload: b.payload || b }); res.json({ ok: true }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/envios/:id/nome', (req, res) => {
  try { const nome = String((req.body || {}).nome || '').trim(); if (!nome) return res.json({ ok: false, erro: 'nome_vazio' }); const r = db.prepare(`UPDATE envios_salvos SET nome=?, atualizado_em=? WHERE id=?`).run(nome, agora(), String(req.params.id)); res.json({ ok: true, alterados: r.changes }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/envios/:id/duplicar', (req, res) => {
  try { const r = db.prepare(`SELECT * FROM envios_salvos WHERE id=?`).get(String(req.params.id)) as any; if (!r) return res.json({ ok: false, erro: 'nao_encontrado' }); let payload: any = {}; try { payload = JSON.parse(r.payload_json || '{}'); } catch { /* */ } res.json({ ok: true, envio: { ...r, payload }, sugestaoNome: (r.nome || 'Envio') + ' (copia)' }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
app.post('/envios/excluir', (req, res) => {
  try { const id = String((req.body || {}).id || ''); const r = db.prepare(`DELETE FROM envios_salvos WHERE id=?`).run(id); res.json({ ok: true, excluidos: r.changes }); }
  catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});

// --- Upload geral de arquivo pro OneDrive (img/pdf/audio/doc) — base64 ---
app.post('/midia/upload', (req: any, res: any) => {
  try {
    if (!_rcloneOk()) return res.json({ ok: false, erro: 'rclone_indisponivel' });
    const b = req.body || {}; const pasta = String(b.pasta || 'Fluxos-Midia').replace(/^\/+|\/+$/g, ''); let nome = String(b.nome || '').replace(/[^a-zA-Z0-9._ -]/g, '_'); const b64 = String(b.b64 || '');
    if (!nome || !b64) return res.status(400).json({ ok: false, erro: 'faltou_dados' });
    const data = b64.includes(',') ? b64.split(',')[1] : b64; const buf = Buffer.from(data, 'base64');
    if (buf.length > 60 * 1024 * 1024) return res.status(413).json({ ok: false, erro: 'arquivo_grande_max_60mb' });
    const tmp = `${MEDIA_DIR}/up-${randomUUID().slice(0, 8)}-${nome}`; fs.writeFileSync(tmp, buf);
    try { _rclone(['copyto', tmp, _odRemotePath(pasta + '/' + nome)], { timeoutMs: 180000 }); } finally { try { fs.unlinkSync(tmp); } catch { /* */ } }
    res.json({ ok: true, caminho: pasta + '/' + nome, nome });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// --- Duplicar uma campanha de fluxo (copia o fluxo_json num novo, desativado) ---
app.post('/fluxo/campanha/duplicar', (req, res) => {
  try {
    const b = req.body || {}; const src = db.prepare(`SELECT * FROM fluxo_campanhas WHERE id=?`).get(String(b.id || '')) as any;
    if (!src) return res.json({ ok: false, erro: 'nao_encontrada' });
    const novoId = randomUUID(); const nome = String(b.nome || ((src.nome || 'Fluxo') + ' (copia)'));
    db.prepare(`INSERT INTO fluxo_campanhas(id,nome,pasta_id,tipo,ativo,fluxo_json,qtd_nos,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?,?)`)
      .run(novoId, nome, src.pasta_id || null, String(src.tipo || 'grupos'), 0, src.fluxo_json || '{}', Number(src.qtd_nos || 0), agora(), agora());
    res.json({ ok: true, id: novoId, nome });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[+envios-salvos] /envios (list/get/salvar/nome/duplicar/excluir) + /midia/upload + /fluxo/campanha/duplicar + _resolverBlocosOd(OneDrive nos fluxos)');


// ============================================================================
// ETAPA 22 (08/10) — DISPARO MULTI-NÚMERO EM PARALELO  [add-only, não mexe no antigo]
// Permite: (a) escolher POR QUAL número cada grupo é enviado; (b) usar 1,2,3+
// números ao mesmo tempo, dividindo os grupos entre eles (modo auto) OU atribuindo
// manualmente número->grupos (modo manual); (c) envio EM PARALELO (um por número
// por ciclo); (d) mídia via URL PÚBLICA (a Evolution recusa host sem domínio).
// Status próprio 'cmp_multi' + ticker próprio tickCampanhaMulti (NÃO mexe no
// tickCampanha/tickFila/enviarTexto/enviarMidia existentes).
// ============================================================================
const MEDIA_PUBLIC_BASE = String(process.env.MEDIA_PUBLIC_BASE || PUBLIC_BASE_URL || '').replace(/\/+$/, '');
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN instancia TEXT`); } catch { /* ja existe */ }

// baixa do OneDrive e devolve URL PÚBLICA (com domínio) p/ a Evolution conseguir buscar.
function _odBaixarPub(rel: string): string {
  const r = _odRcloneBaixar(rel);
  if (MEDIA_PUBLIC_BASE && r.url) return r.url.replace(MEDIA_INTERNAL_BASE, MEDIA_PUBLIC_BASE);
  return r.url;
}
// envia por uma INSTÂNCIA específica (não pelo projeto). Reusa evo().
async function enviarTextoVia(instancia: string, para: string, texto: string) {
  return evo(`/message/sendText/${encodeURIComponent(instancia)}`, 'POST', { number: para, text: texto });
}
async function enviarMidiaVia(instancia: string, para: string, tipo: string, url: string, legenda?: string) {
  if (!url) throw new Error('midia_sem_url');
  if (tipo === 'audio') return evo(`/message/sendWhatsAppAudio/${encodeURIComponent(instancia)}`, 'POST', { number: para, audio: url });
  const mediatype = tipo === 'video' ? 'video' : (tipo === 'documento' || tipo === 'document') ? 'document' : 'image';
  const body: any = { number: para, mediatype, media: url };
  if (legenda) body.caption = legenda;
  if (mediatype === 'document') body.fileName = (url.split('/').pop() || 'arquivo').split('?')[0];
  return evo(`/message/sendMedia/${encodeURIComponent(instancia)}`, 'POST', body);
}
// descobre quais das NOSSAS instâncias são admin de um grupo (cache 10min). Casa pelo phoneNumber (@lid esconde o número).
const _admCache: Record<string, { ts: number; adm: Record<string, string> }> = {};
async function _gruposAdmins(jid: string, instancias: string[]): Promise<Record<string, string>> {
  const c = _admCache[jid];
  if (c && Date.now() - c.ts < 10 * 60 * 1000) return c.adm;
  const nums = db.prepare(`SELECT instancia, telefone FROM numeros`).all() as any[];
  const telToInst: Record<string, string> = {};
  for (const n of nums) if (n.telefone && n.instancia) telToInst[String(n.telefone).replace(/\D/g, '')] = n.instancia;
  const adm: Record<string, string> = {};
  for (const inst of instancias) {
    let d: any = null;
    try { d = await evo(`/group/findGroupInfos/${encodeURIComponent(inst)}?groupJid=${encodeURIComponent(jid)}`, 'GET'); } catch { d = null; }
    const ps = d && Array.isArray(d.participants) ? d.participants : null;
    if (!ps) continue;
    for (const p of ps) {
      if (!p.admin) continue;
      const tel = String(p.phoneNumber || p.id || '').split('@')[0].replace(/\D/g, '');
      const mine = telToInst[tel];
      if (mine) adm[mine] = p.admin;
    }
    break; // uma instância que veja o grupo já traz a lista completa de admins
  }
  _admCache[jid] = { ts: Date.now(), adm };
  return adm;
}
// enfileira: cada {jid,instancia} recebe os blocos; agendamento POR instância => paralelo entre números.
function _enfileiraMulti(assigns: { jid: string; instancia: string }[], blocos: any[], intervalo: number, projeto: string, baseTs?: number, loteId?: string, loteNome?: string | null) { // [+envios-programados]
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,agendado_para,criado_em,lote_id,lote_nome) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const base = (baseTs && !isNaN(baseTs)) ? baseTs : Date.now(); // [etapa26] agendar: hora-base opcional (futuro => fila segura até a hora)
  const idxPorInst: Record<string, number> = {};
  let n = 0;
  const tx = db.transaction(() => {
    for (const a of assigns) {
      const i = (idxPorInst[a.instancia] = (idxPorInst[a.instancia] ?? -1) + 1);
      blocos.forEach((b: any, j: number) => {
        const tipo = b.tipo === 'texto' ? 'texto' : b.tipo;
        const quando = new Date(base + i * intervalo * 1000 + j * 2000).toISOString();
        ins.run(randomUUID(), projeto, a.jid, 1, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), 'cmp_multi', a.instancia, quando, agora(), loteId || null, loteNome || null); // [+envios-programados]
        n++;
      });
    }
  });
  tx();
  return n;
}
// ticker paralelo: a cada ciclo pega 1 item DEVIDO por instância distinta e envia todos juntos.
let _multiBusy = false;
async function tickCampanhaMulti() {
  if (_multiBusy) return; _multiBusy = true;
  try {
    const rows = db.prepare(`SELECT * FROM fila_envio WHERE status='cmp_multi' AND instancia IS NOT NULL AND (agendado_para IS NULL OR agendado_para <= ?) ORDER BY agendado_para, criado_em`).all(agora()) as any[];
    const porInst: Record<string, any> = {};
    for (const r of rows) { const k = r.instancia || '_'; if (!porInst[k]) porInst[k] = r; }
    const lote = Object.values(porInst) as any[];
    await Promise.all(lote.map(async (item: any) => {
      try {
        if ((item.tipo || 'texto') === 'texto') await enviarTextoVia(item.instancia, item.para, item.texto || '');
        else await enviarMidiaVia(item.instancia, item.para, item.tipo, item.url || '', item.legenda || '');
        db.prepare(`UPDATE fila_envio SET status='enviado' WHERE id=?`).run(item.id);
      } catch {
        db.prepare(`UPDATE fila_envio SET tentativas=tentativas+1, status=CASE WHEN tentativas>=3 THEN 'falhou' ELSE 'cmp_multi' END WHERE id=?`).run(item.id);
      }
    }));
  } finally { _multiBusy = false; }
}
setInterval(() => { tickCampanhaMulti().catch(() => {}); }, 1000);

// lista, p/ cada grupo, quais dos NOSSOS números são admin (p/ a tela montar as opções).
app.post('/disparo/grupos-admins', async (req, res) => {
  try {
    const p = req.body || {};
    const numeros = (p.numeros || []).map((x: any) => String(x));
    const base = numeros.length ? numeros : (db.prepare(`SELECT instancia FROM numeros WHERE status='conectado'`).all() as any[]).map((n: any) => n.instancia);
    const out: any[] = [];
    for (const jid of (p.grupos || [])) { const adm = await _gruposAdmins(String(jid), base); out.push({ jid, admins: Object.keys(adm) }); }
    res.json({ ok: true, grupos: out });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// DISPARO MULTI-NÚMERO. body: { modo:'auto'|'manual', numeros:[...], grupos:[jid...] (auto),
//   atribuicoes:[{instancia,grupos:[jid...]}] (manual), blocos:[{tipo:'imagem',od:'...'}|{tipo:'texto',texto}],
//   intervaloSegundos, validarAdmin?:bool }. Mídia com od => baixa do OneDrive e usa URL PÚBLICA.
app.post('/disparo/enviar-multi', async (req, res) => {
  try {
    const p = req.body || {};
    let blocos: any[];
    try {
      blocos = (p.blocos || []).map((b: any) => {
        if (b && b.tipo && b.tipo !== 'texto' && (b.od || b.onedrive)) {
          if (!_rcloneOk()) throw new Error('rclone_indisponivel');
          return { ...b, url: _odBaixarPub(String(b.od || b.onedrive)) };
        }
        return b;
      });
    } catch (e: any) { return res.json({ ok: false, erro: String(e.message || e) }); }
    blocos = blocos.filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
    if (!blocos.length) return res.json({ ok: false, erro: 'sem blocos válidos' });
    const intervalo = Math.max(1, Number(p.intervaloSegundos || 8));
    const projeto = String(p.projeto || 'teclado');
    const assigns: { jid: string; instancia: string }[] = [];
    const pulados: any[] = [];
    if (p.modo === 'manual') {
      for (const a of (p.atribuicoes || [])) {
        const inst = String(a.instancia || '');
        for (const jid of (a.grupos || [])) {
          if (p.validarAdmin === false) { assigns.push({ jid: String(jid), instancia: inst }); continue; }
          const adm = await _gruposAdmins(String(jid), [inst]);
          if (adm[inst]) assigns.push({ jid: String(jid), instancia: inst });
          else pulados.push({ jid, instancia: inst, motivo: 'nao_admin' });
        }
      }
    } else {
      const numeros = (p.numeros || []).map((x: any) => String(x));
      if (!numeros.length) return res.json({ ok: false, erro: 'informe numeros (auto)' });
      const carga: Record<string, number> = {}; numeros.forEach((n: string) => carga[n] = 0);
      for (const jid of (p.grupos || [])) {
        const adm = await _gruposAdmins(String(jid), numeros);
        const validos = numeros.filter((n: string) => adm[n]);
        if (!validos.length) { pulados.push({ jid, motivo: 'nenhum_numero_admin' }); continue; }
        validos.sort((a: string, b: string) => (carga[a] - carga[b]));
        const esc = validos[0]; carga[esc]++;
        assigns.push({ jid: String(jid), instancia: esc });
      }
    }
    if (!assigns.length) return res.json({ ok: false, erro: 'nenhum grupo atribuído', pulados });
    const baseTs = p.agendadoPara ? new Date(p.agendadoPara).getTime() : undefined; // [etapa26] agendar vários disparos (hora opcional vinda do app)
    const _agFut = !!(baseTs && !isNaN(baseTs) && baseTs > Date.now() + 1000); // [+envios-programados] só é "programado" se for pro futuro
    const _loteId = _agFut ? randomUUID() : undefined; // [+envios-programados]
    const _loteNome = (String(p.loteNome || p.nomeEnvio || '').trim() || null); // [+envios-programados]
    const n = (p.marcarTodos ? _enfileiraMultiMk : _enfileiraMulti)(assigns, blocos, intervalo, projeto, (baseTs && !isNaN(baseTs) ? baseTs : undefined), _loteId, _loteNome);
    if (_loteId && n > 0) { // [+envios-programados] registra o lote de grupos agendado
      try {
        db.prepare(`INSERT OR REPLACE INTO envios_programados(id,projeto,is_grupo,canal,nome,quando,total,payload_json,status,criado_em) VALUES(?,?,1,'whatsapp',?,?,?,?,'agendado',?)`)
          .run(_loteId, String(projeto || ''), _loteNome, new Date(baseTs as number).toISOString(), n, JSON.stringify(p || {}), agora());
      } catch { /* meta best-effort */ }
    }
    const porNumero = assigns.reduce((o: any, a) => ((o[a.instancia] = (o[a.instancia] || 0) + 1), o), {});
    _salvarEnvio({ canal: 'whatsapp', tipo: 'disparo-multi', payload: p });
    res.json({ ok: true, enfileirados: n, grupos: assigns.length, porNumero, marcarTodos: !!p.marcarTodos, pulados });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// acompanha o disparo multi (quanto já saiu por número).
app.get('/disparo/multi-status', (_req, res) => {
  res.json({ ok: true, porInstancia: db.prepare(`SELECT instancia, status, COUNT(*) n FROM fila_envio WHERE instancia IS NOT NULL AND status IN ('cmp_multi','cmp_mmk','enviado','falhou') GROUP BY instancia, status`).all() });
});
console.log('[etapa22] disparo multi-número paralelo: POST /disparo/enviar-multi (auto/manual) + /disparo/grupos-admins + /disparo/multi-status (mídia por URL pública)');

// ============================================================================
// ETAPA 23 (08/10) — disparo em LOTE por filtro de nome + RELATÓRIO por grupo [add-only]
// enviar-lote: seleciona os grupos pelo NOME (ex. "BLACK TECLADO GOSPEL 2026"),
//   exclui por nome/jid, e manda TODOS por UMA instância (sem depender da validação
//   de admin ao vivo, que oscila). Reusa _enfileiraMulti + tickCampanhaMulti.
// multi-relatorio: lista GRUPO A GRUPO o que saiu/falhou/pendente (+ nome), pro
//   Ezequias saber exatamente quais não foram e reenviar.
// ============================================================================
app.post('/disparo/enviar-lote', (req, res) => {
  try {
    const p = req.body || {};
    const filtro = String(p.filtroNome || '').trim();
    const instancia = String(p.instancia || '').trim();
    if (!filtro) return res.json({ ok: false, erro: 'informe filtroNome' });
    if (!instancia) return res.json({ ok: false, erro: 'informe instancia' });
    const excluirNome = (p.excluirNome ? ([] as any[]).concat(p.excluirNome) : []).map((s: any) => String(s).toLowerCase());
    const excluirJids = new Set((p.excluirJids || []).map((s: any) => String(s)));
    const todos = db.prepare(`SELECT id, nome FROM grupos_wpp WHERE nome LIKE ?`).all('%' + filtro + '%') as any[];
    const jids: string[] = [];
    const vistos = new Set<string>();
    for (const g of todos) {
      const nomeL = String(g.nome || '').toLowerCase();
      if (excluirNome.some((e: string) => e && nomeL.includes(e))) continue;
      if (excluirJids.has(String(g.id))) continue;
      if (vistos.has(String(g.id))) continue;
      vistos.add(String(g.id)); jids.push(String(g.id));
    }
    if (!jids.length) return res.json({ ok: false, erro: 'nenhum grupo no filtro' });
    let blocos: any[];
    try {
      blocos = (p.blocos || []).map((b: any) => {
        if (b && b.tipo && b.tipo !== 'texto' && (b.od || b.onedrive)) {
          if (!_rcloneOk()) throw new Error('rclone_indisponivel');
          return { ...b, url: _odBaixarPub(String(b.od || b.onedrive)) };
        }
        return b;
      });
    } catch (e: any) { return res.json({ ok: false, erro: String(e.message || e) }); }
    blocos = blocos.filter((b: any) => (b.tipo === 'texto' ? String(b.texto || '').trim() : String(b.url || '').trim()));
    if (!blocos.length) return res.json({ ok: false, erro: 'sem blocos válidos' });
    const intervalo = Math.max(1, Number(p.intervaloSegundos || 8));
    const assigns = jids.map((j) => ({ jid: j, instancia }));
    const n = (p.marcarTodos ? _enfileiraMultiMk : _enfileiraMulti)(assigns, blocos, intervalo, String(p.projeto || 'teclado'));
    _salvarEnvio({ canal: 'whatsapp', tipo: 'disparo-lote', payload: p });
    res.json({ ok: true, enfileirados: n, grupos: jids.length, instancia, marcarTodos: !!p.marcarTodos, jids });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
// relatório por grupo (o que saiu / falhou / pendente), com nome do grupo.
app.get('/disparo/multi-relatorio', (_req, res) => {
  try {
    const rows = db.prepare(`SELECT para, instancia, status, COUNT(*) n FROM fila_envio WHERE instancia IS NOT NULL AND status IN ('cmp_multi','cmp_mmk','enviado','falhou') GROUP BY para, instancia, status`).all() as any[];
    const porGrupo: Record<string, any> = {};
    for (const r of rows) {
      const k = String(r.para);
      if (!porGrupo[k]) porGrupo[k] = { jid: r.para, instancia: r.instancia, enviado: 0, falhou: 0, pendente: 0 };
      if (r.status === 'enviado') porGrupo[k].enviado += r.n;
      else if (r.status === 'falhou') porGrupo[k].falhou += r.n;
      else porGrupo[k].pendente += r.n;
    }
    const nomes = db.prepare(`SELECT id, nome FROM grupos_wpp`).all() as any[];
    const nomeDe: Record<string, string> = {};
    for (const g of nomes) nomeDe[String(g.id)] = String(g.nome || '');
    const grupos = (Object.values(porGrupo) as any[]).map((g: any) => ({
      ...g, nome: nomeDe[g.jid] || g.jid,
      situacao: g.pendente > 0 ? 'pendente' : (g.falhou > 0 && g.enviado === 0 ? 'falhou' : (g.falhou > 0 ? 'parcial' : 'enviado')),
    }));
    const resumo = {
      enviados: grupos.filter((g: any) => g.situacao === 'enviado').length,
      falhou: grupos.filter((g: any) => g.situacao === 'falhou').length,
      parcial: grupos.filter((g: any) => g.situacao === 'parcial').length,
      pendente: grupos.filter((g: any) => g.situacao === 'pendente').length,
      total: grupos.length,
    };
    res.json({ ok: true, resumo, grupos });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[etapa23] disparo em lote por filtro (/disparo/enviar-lote) + relatório por grupo (/disparo/multi-relatorio)');

// ============================================================================
// ETAPA 24 (08/10) — MARCAR MEMBROS (menção @) no disparo [add-only]
// Caminho PRÓPRIO (status 'cmp_mmk' + ticker próprio) pra NÃO tocar no disparo
// normal que já roda (tickCampanhaMulti/_enfileiraMulti/enviarTextoVia seguem iguais).
// Evolution v2: mentionsEveryOne:true no corpo marca todos os membros do grupo =>
// chega como notificação pra cada pessoa (mesmo no silêncio) e aumenta a abertura.
// As rotas /disparo/enviar-lote e /disparo/enviar-multi passam a aceitar { marcarTodos:true }.
// ============================================================================
try { db.exec(`ALTER TABLE fila_envio ADD COLUMN marcar INTEGER`); } catch { /* ja existe */ }
// envia MARCANDO todos os membros (mentionsEveryOne). Reusa evo(). Não mexe nas enviar*Via normais.
async function enviarTextoViaMk(instancia: string, para: string, texto: string) {
  return evo(`/message/sendText/${encodeURIComponent(instancia)}`, 'POST', { number: para, text: texto, mentionsEveryOne: true });
}
async function enviarMidiaViaMk(instancia: string, para: string, tipo: string, url: string, legenda?: string) {
  if (!url) throw new Error('midia_sem_url');
  if (tipo === 'audio') return evo(`/message/sendWhatsAppAudio/${encodeURIComponent(instancia)}`, 'POST', { number: para, audio: url, mentionsEveryOne: true });
  const mediatype = tipo === 'video' ? 'video' : (tipo === 'documento' || tipo === 'document') ? 'document' : 'image';
  const body: any = { number: para, mediatype, media: url, mentionsEveryOne: true };
  if (legenda) body.caption = legenda;
  if (mediatype === 'document') body.fileName = (url.split('/').pop() || 'arquivo').split('?')[0];
  return evo(`/message/sendMedia/${encodeURIComponent(instancia)}`, 'POST', body);
}
// enfileira igual ao _enfileiraMulti, mas com status 'cmp_mmk' + marcar=1 (ticker marcado cuida).
function _enfileiraMultiMk(assigns: { jid: string; instancia: string }[], blocos: any[], intervalo: number, projeto: string, baseTs?: number, loteId?: string, loteNome?: string | null) { // [+envios-programados]
  const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,tipo,url,legenda,status,instancia,marcar,agendado_para,criado_em,lote_id,lote_nome) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const base = (baseTs && !isNaN(baseTs)) ? baseTs : Date.now(); // [etapa26] agendar: hora-base opcional
  const idxPorInst: Record<string, number> = {};
  let n = 0;
  const tx = db.transaction(() => {
    for (const a of assigns) {
      const i = (idxPorInst[a.instancia] = (idxPorInst[a.instancia] ?? -1) + 1);
      blocos.forEach((b: any, j: number) => {
        const tipo = b.tipo === 'texto' ? 'texto' : b.tipo;
        const quando = new Date(base + i * intervalo * 1000 + j * 2000).toISOString();
        ins.run(randomUUID(), projeto, a.jid, 1, tipo === 'texto' ? String(b.texto || '') : '', tipo, String(b.url || ''), String(b.texto || ''), 'cmp_mmk', a.instancia, 1, quando, agora(), loteId || null, loteNome || null); // [+envios-programados]
        n++;
      });
    }
  });
  tx();
  return n;
}
// ticker MARCADO: pega 1 item devido por instância distinta (status 'cmp_mmk') e envia marcando todos.
let _mkBusy = false;
async function tickCampanhaMultiMk() {
  if (_mkBusy) return; _mkBusy = true;
  try {
    const rows = db.prepare(`SELECT * FROM fila_envio WHERE status='cmp_mmk' AND instancia IS NOT NULL AND (agendado_para IS NULL OR agendado_para <= ?) ORDER BY agendado_para, criado_em`).all(agora()) as any[];
    const porInst: Record<string, any> = {};
    for (const r of rows) { const k = r.instancia || '_'; if (!porInst[k]) porInst[k] = r; }
    const lote = Object.values(porInst) as any[];
    await Promise.all(lote.map(async (item: any) => {
      try {
        if ((item.tipo || 'texto') === 'texto') await enviarTextoViaMk(item.instancia, item.para, item.texto || '');
        else await enviarMidiaViaMk(item.instancia, item.para, item.tipo, item.url || '', item.legenda || '');
        db.prepare(`UPDATE fila_envio SET status='enviado' WHERE id=?`).run(item.id);
      } catch {
        db.prepare(`UPDATE fila_envio SET tentativas=tentativas+1, status=CASE WHEN tentativas>=3 THEN 'falhou' ELSE 'cmp_mmk' END WHERE id=?`).run(item.id);
      }
    }));
  } finally { _mkBusy = false; }
}
setInterval(() => { tickCampanhaMultiMk().catch(() => {}); }, 1000);
console.log('[etapa24] marcar membros (menção @ todos) no disparo: { marcarTodos:true } em /disparo/enviar-lote e /disparo/enviar-multi');

// ============== [instagram] CASCA DM 1-a-1 — só envia depois da permissão instagram_manage_messages ==============
// Guarda de segurança: por enquanto NÃO envia; responde claro pro app mostrar o aviso.
// Quando a Meta aprovar, a chamada à Graph API (POST /{ig-user-id}/messages) entra aqui, e a flag vira '1'.
function _igConectado(): boolean { return pubFlag('ig_manage_messages_ok', '0') === '1'; }
app.get('/ig/status', (_req, res) => {
  res.json({ ok: true, conectado: _igConectado(), motivo: _igConectado() ? '' : 'falta permissão instagram_manage_messages (App Review da Meta)' });
});
app.post('/ig/enviar', (_req, res) => {
  _salvarEnvio({ canal: 'instagram', tipo: 'ig-rapido', payload: _req.body || {} });
  if (!_igConectado()) return res.json({ ok: false, erro: 'Instagram ainda não conectado (falta a permissão instagram_manage_messages da Meta). Monte e salve os fluxos; eles disparam quando o Instagram for conectado.' });
  res.json({ ok: false, erro: 'Envio de Instagram ainda não implementado (aguardando conexão).' });
});
console.log('[instagram] casca DM 1-a-1 carregada (/ig/status, /ig/enviar) — envia só após instagram_manage_messages');

// ============================================================================
// ETAPA 25 (08/10) — EDITAR GRUPOS EM MASSA: nome / descrição / foto [add-only]
// POST /disparo/grupos-editar { instancia, jids:[...], nome?, descricao?, fotoOd?|fotoUrl?, intervaloSegundos? }
//   por grupo chama os endpoints do Evolution só pros campos enviados; foto via OneDrive (od) vira URL pública.
//   Retorna resultado grupo a grupo (nome/descricao/foto = ok|erro).
// ============================================================================
async function _grpSubject(instancia: string, jid: string, subject: string) {
  return evo(`/group/updateGroupSubject/${encodeURIComponent(instancia)}?groupJid=${encodeURIComponent(jid)}`, 'POST', { subject });
}
async function _grpDescription(instancia: string, jid: string, description: string) {
  return evo(`/group/updateGroupDescription/${encodeURIComponent(instancia)}?groupJid=${encodeURIComponent(jid)}`, 'POST', { description });
}
async function _grpPicture(instancia: string, jid: string, image: string) {
  return evo(`/group/updateGroupPicture/${encodeURIComponent(instancia)}?groupJid=${encodeURIComponent(jid)}`, 'POST', { image });
}
app.post('/disparo/grupos-editar', async (req, res) => {
  try {
    const p = req.body || {};
    const instancia = String(p.instancia || '').trim();
    const jids = (p.jids || []).map((x: any) => String(x)).filter(Boolean);
    if (!instancia) return res.json({ ok: false, erro: 'informe instancia' });
    if (!jids.length) return res.json({ ok: false, erro: 'informe jids' });
    const nome = p.nome != null && String(p.nome) !== '' ? String(p.nome) : null;
    const descricao = p.descricao != null ? String(p.descricao) : null; // '' = limpar descrição
    let fotoUrl = p.fotoUrl ? String(p.fotoUrl) : null;
    if (nome == null && descricao == null && !p.fotoOd && !fotoUrl) return res.json({ ok: false, erro: 'nada pra alterar (nome/descricao/foto)' });
    try {
      if (p.fotoOd && !fotoUrl) { if (!_rcloneOk()) throw new Error('rclone_indisponivel'); fotoUrl = _odBaixarPub(String(p.fotoOd)); }
    } catch (e: any) { return res.json({ ok: false, erro: String(e.message || e) }); }
    const intervalo = Math.max(0, Number(p.intervaloSegundos != null ? p.intervaloSegundos : 1));
    const out: any[] = [];
    for (const jid of jids) {
      const r: any = { jid };
      if (nome != null) { try { await _grpSubject(instancia, jid, nome); r.nome = 'ok'; } catch (e: any) { r.nome = 'erro:' + String(e.message || e).slice(0, 80); } }
      if (descricao != null) { try { await _grpDescription(instancia, jid, descricao); r.descricao = 'ok'; } catch (e: any) { r.descricao = 'erro:' + String(e.message || e).slice(0, 80); } }
      if (fotoUrl) { try { await _grpPicture(instancia, jid, fotoUrl); r.foto = 'ok'; } catch (e: any) { r.foto = 'erro:' + String(e.message || e).slice(0, 80); } }
      out.push(r);
      if (intervalo) await new Promise((rs) => setTimeout(rs, intervalo * 1000));
    }
    const resumo = {
      total: out.length,
      nomeOk: out.filter((r: any) => r.nome === 'ok').length,
      descricaoOk: out.filter((r: any) => r.descricao === 'ok').length,
      fotoOk: out.filter((r: any) => r.foto === 'ok').length,
      comErro: out.filter((r: any) => [r.nome, r.descricao, r.foto].some((v: any) => v && String(v).startsWith('erro'))).length,
    };
    res.json({ ok: true, resumo, grupos: out });
  } catch (e: any) { res.json({ ok: false, erro: String(e.message || e) }); }
});
console.log('[etapa25] editar grupos em massa: POST /disparo/grupos-editar (nome/descrição/foto)');

console.log('[etapa19] conexões/dispositivos carregado (listar/estado/conectar/pareamento/reiniciar/desconectar/criar/registrar/excluir)');
console.log('[etapa20] entrega: chamar pela pessoa pelo nome com liga/desliga + fallback por produto');



// ============================================================================
// [etapa28] PUBLICADOR NA NUVEM — posta nas redes 24/7 a partir do OneDrive.
//   Aditivo. INERTE por padrão: só posta quando pub_kv 'publicador_ativo'='1'.
//   Reusa do motor: db, app, express, cifra/decifra, pubFlag/setPubFlag,
//   _odRcloneBaixar (baixa do OneDrive), MEDIA_DIR/MEDIA_PUBLIC_BASE,
//   PUBLIC_BASE_URL, agora(), fs, createHash/randomBytes/randomUUID.
//   Tabelas já existentes: fila_publicacao, canais_publicacao, oauth_tokens_pub,
//   conexoes_sociais, pub_kv, horarios_preferidos.
//   Prefixos próprios (pb*/tickPublicador) pra não colidir com nada existente.
// ============================================================================

// ---- helpers locais (nome/extensão sem depender de import novo) ----
function pbBasename(p: string): string { return String(p || '').split(/[\\/]/).pop() || ''; }
function pbExt(p: string): string { const n = pbBasename(p); return n.includes('.') ? (n.split('.').pop() || '').toLowerCase() : ''; }
function pbIsImagem(p: string): boolean { return ['jpg', 'jpeg', 'png', 'webp'].includes(pbExt(p)); }
function pbMime(p: string): string {
  const e = pbExt(p);
  if (e === 'png') return 'image/png'; if (e === 'webp') return 'image/webp';
  if (e === 'jpg' || e === 'jpeg') return 'image/jpeg';
  if (e === 'mov') return 'video/quicktime'; if (e === 'webm') return 'video/webm';
  if (e === 'mkv') return 'video/x-matroska'; if (e === 'm4v') return 'video/x-m4v';
  return 'video/mp4';
}
function pbSleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }
function pbLog(evt: string, ...args: any[]) { try { console.log(`[etapa28] ${evt}`, ...args); } catch { /* */ } }

// ---- store de tokens OAuth (oauth_tokens_pub, cifrado) ----
interface PbTok { accessToken: string; refreshToken: string | null; expiresAt: string | null; scope: string | null; conta: string | null; }
function pbTokSalvar(canalId: string, plataforma: string, t: { accessToken?: string; refreshToken?: string | null; expiresAt?: string | null; scope?: string | null; conta?: string | null }) {
  const atual = pbTokLer(canalId, plataforma);
  const acc = t.accessToken != null ? t.accessToken : (atual?.accessToken || '');
  const ref = t.refreshToken !== undefined ? t.refreshToken : (atual?.refreshToken ?? null);
  const exp = t.expiresAt !== undefined ? t.expiresAt : (atual?.expiresAt ?? null);
  const scp = t.scope !== undefined ? t.scope : (atual?.scope ?? null);
  const cnt = t.conta !== undefined ? t.conta : (atual?.conta ?? null);
  db.prepare(`INSERT INTO oauth_tokens_pub(canal_id,plataforma,access_token_enc,refresh_token_enc,expires_at,scope,conta,atualizado_em)
    VALUES(?,?,?,?,?,?,?,?)
    ON CONFLICT(canal_id,plataforma) DO UPDATE SET access_token_enc=excluded.access_token_enc,
      refresh_token_enc=excluded.refresh_token_enc, expires_at=excluded.expires_at, scope=excluded.scope,
      conta=excluded.conta, atualizado_em=excluded.atualizado_em`)
    .run(String(canalId), plataforma, cifra(acc), ref ? cifra(ref) : null, exp, scp, cnt, agora());
}
function pbTokLer(canalId: string, plataforma: string): PbTok | null {
  const r = db.prepare(`SELECT * FROM oauth_tokens_pub WHERE canal_id=? AND plataforma=?`).get(String(canalId), plataforma) as any;
  if (!r) return null;
  return {
    accessToken: r.access_token_enc ? decifra(r.access_token_enc) : '',
    refreshToken: r.refresh_token_enc ? decifra(r.refresh_token_enc) : null,
    expiresAt: r.expires_at || null, scope: r.scope || null, conta: r.conta || null,
  };
}
function pbTokApagar(canalId: string, plataforma: string) {
  try { db.prepare(`DELETE FROM oauth_tokens_pub WHERE canal_id=? AND plataforma=?`).run(String(canalId), plataforma); } catch { /* */ }
}
function pbTokValido(t: PbTok | null): boolean {
  if (!t || !t.accessToken) return false;
  if (!t.expiresAt) return true; // tokens de página do Meta não expiram
  return new Date(t.expiresAt).getTime() - 60_000 > Date.now();
}

// ---- credenciais de app OAuth (client_id/secret) em pub_kv cifrado, com fallback env ----
function pbCred(plataforma: string): { clientId: string; clientSecret: string } {
  const ci = decifra(pubFlag(`cred_${plataforma}_client_id`, '')) || process.env[`${plataforma.toUpperCase()}_CLIENT_ID`] || '';
  const cs = decifra(pubFlag(`cred_${plataforma}_client_secret`, '')) || process.env[`${plataforma.toUpperCase()}_CLIENT_SECRET`] || '';
  return { clientId: ci, clientSecret: cs };
}
function pbCredSalvar(plataforma: string, clientId: string, clientSecret: string) {
  if (clientId) setPubFlag(`cred_${plataforma}_client_id`, cifra(clientId));
  if (clientSecret) setPubFlag(`cred_${plataforma}_client_secret`, cifra(clientSecret));
}

// ---- helper de canal (lê colunas de canais_publicacao) ----
function pbCanal(canalId: string): any { return db.prepare(`SELECT * FROM canais_publicacao WHERE id=?`).get(String(canalId)); }
function pbCanalSet(canalId: string, campo: string, valor: string) {
  const permitido = ['ig_business_id', 'fb_page_id', 'yt_channel_id', 'threads_user_id', 'pinterest_board_id', 'tiktok_open_id'];
  if (!permitido.includes(campo)) return;
  db.prepare(`UPDATE canais_publicacao SET ${campo}=?, atualizado_em=? WHERE id=?`).run(valor, agora(), String(canalId));
}

// ---- baixa a mídia do OneDrive e devolve caminho local + URL PÚBLICA (pras redes que exigem URL) ----
function pbBaixarMidia(onedriveRef: string): { file: string; publicUrl: string; nome: string } {
  const r = _odRcloneBaixar(onedriveRef); // {file, url(interna), nome}
  const safe = pbBasename(r.file);
  const base = String(MEDIA_PUBLIC_BASE || PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  return { file: r.file, publicUrl: `${base}/media/${safe}`, nome: r.nome };
}

// ---- marca status de conexão (conexoes_sociais) ----
function pbConexao(canalId: string, plataforma: string, status: string, detalhe = '') {
  try {
    db.prepare(`INSERT INTO conexoes_sociais(id,canal_id,plataforma,metodo,status,detalhe,ultimo_check,atualizado_em)
      VALUES(?,?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET status=excluded.status, detalhe=excluded.detalhe, ultimo_check=excluded.ultimo_check, atualizado_em=excluded.atualizado_em`)
      .run(`${canalId}:${plataforma}`, String(canalId), plataforma, 'oauth', status, detalhe, agora(), agora());
  } catch { /* conexoes_sociais usa PK id; se schema diferente, ignora */ }
}

// ============================ OAUTH — YOUTUBE (Google) ============================
const PB_YT_SCOPES = [
  'https://www.googleapis.com/auth/youtube.upload',
  'https://www.googleapis.com/auth/youtube',
  'https://www.googleapis.com/auth/youtube.readonly',
  'openid', 'email', 'profile',
].join(' ');
function pbYtRedirect(): string { return (PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/pub/youtube/callback'; }

app.get('/pub/youtube/connect', (req, res) => {
  try {
    const canalId = String((req.query.canalId || req.query.canal_id || '')).trim();
    if (!canalId) return res.status(400).json({ ok: false, erro: 'canalId obrigatório' });
    const { clientId } = pbCred('youtube');
    if (!clientId) return res.status(400).json({ ok: false, erro: 'YouTube client_id não configurado (POST /pub/pub-cred)' });
    const params = new URLSearchParams({
      client_id: clientId, redirect_uri: pbYtRedirect(), response_type: 'code', scope: PB_YT_SCOPES,
      access_type: 'offline', prompt: 'select_account consent', include_granted_scopes: 'false', state: `yt:${canalId}`,
    });
    res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}` });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// ROTA PÚBLICA (Google chama sem x-api-key) — exceção adicionada no middleware.
app.get('/pub/youtube/callback', async (req, res) => {
  const enviaHtml = (ok: boolean, msg: string) => res.status(200).type('html').send(pbHtmlResposta(ok, msg));
  try {
    const code = String(req.query.code || ''); const state = String(req.query.state || '');
    if (req.query.error) return enviaHtml(false, `Google: ${req.query.error}`);
    // [gcal-connect] reaproveita o MESMO redirect do YouTube pra conectar o Google Agenda (state 'gcal:')
    if (state.startsWith('gcal:')) {
      try {
        const { clientId: gci, clientSecret: gcs } = pbCred('youtube');
        const tr2: any = await fetch('https://oauth2.googleapis.com/token', {
          method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ code, client_id: gci, client_secret: gcs, redirect_uri: pbYtRedirect(), grant_type: 'authorization_code' }),
        });
        if (!tr2.ok) return enviaHtml(false, `Google Agenda: troca de token falhou (${tr2.status})`);
        const td2: any = await tr2.json();
        if (!td2.refresh_token) return enviaHtml(false, 'Google nao devolveu refresh_token. Tente conectar de novo.');
        setPubFlag('gcal_client_id', gci);
        setPubFlag('gcal_client_secret_enc', cifra(gcs));
        setPubFlag('gcal_refresh_enc', cifra(String(td2.refresh_token)));
        setPubFlag('gcal_ativo', '1');
        let contaG = '';
        try { const ui: any = await (await fetch('https://openidconnect.googleapis.com/v1/userinfo', { headers: { Authorization: `Bearer ${td2.access_token}` } })).json(); contaG = ui.email || ui.name || ''; } catch { /* */ }
        if (contaG) setPubFlag('gcal_conta', contaG);
        return enviaHtml(true, `Google Agenda conectado: ${contaG || 'conta'}. Pode fechar esta aba.`);
      } catch (e: any) { return enviaHtml(false, 'Google Agenda: ' + String(e?.message || e)); }
    }
    if (!code || !state.startsWith('yt:')) return enviaHtml(false, 'callback inválido');
    const canalId = state.slice(3);
    const { clientId, clientSecret } = pbCred('youtube');
    const tr = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: clientId, client_secret: clientSecret, redirect_uri: pbYtRedirect(), grant_type: 'authorization_code' }),
    });
    if (!tr.ok) return enviaHtml(false, `troca de token falhou (${tr.status})`);
    const td = await tr.json() as any;
    const expiresAt = new Date(Date.now() + (td.expires_in || 3600) * 1000).toISOString();
    // conta + channelId
    let conta = ''; let channelId = '';
    try { const ui = await (await fetch('https://openidconnect.googleapis.com/v1/userinfo', { headers: { Authorization: `Bearer ${td.access_token}` } })).json() as any; conta = ui.email || ui.name || ''; } catch { /* */ }
    try {
      const ch = await (await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers: { Authorization: `Bearer ${td.access_token}` } })).json() as any;
      if (ch.items && ch.items[0]) { channelId = ch.items[0].id; conta = ch.items[0].snippet?.title || conta; }
    } catch { /* */ }
    pbTokSalvar(canalId, 'youtube', { accessToken: td.access_token, refreshToken: td.refresh_token || null, expiresAt, scope: td.scope || PB_YT_SCOPES, conta });
    if (channelId) pbCanalSet(canalId, 'yt_channel_id', channelId);
    pbConexao(canalId, 'youtube', 'conectado', conta);
    enviaHtml(true, `YouTube: ${conta || channelId || 'conta'}`);
  } catch (e: any) { enviaHtml(false, String(e?.message || e)); }
});

async function pbYtToken(canalId: string): Promise<string> {
  const rec = pbTokLer(canalId, 'youtube');
  if (!rec) throw new Error('Canal não conectado ao YouTube.');
  if (pbTokValido(rec)) return rec.accessToken;
  if (!rec.refreshToken) throw new Error('Token YouTube expirado e sem refresh — reconecte.');
  const { clientId, clientSecret } = pbCred('youtube');
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, grant_type: 'refresh_token', refresh_token: rec.refreshToken }),
  });
  if (!r.ok) throw new Error(`Refresh YouTube falhou (${r.status}): ${(await r.text()).slice(0, 200)}`);
  const d = await r.json() as any;
  const expiresAt = new Date(Date.now() + (d.expires_in || 3600) * 1000).toISOString();
  pbTokSalvar(canalId, 'youtube', { accessToken: d.access_token, expiresAt, ...(d.scope ? { scope: d.scope } : {}) });
  return d.access_token;
}

// ===================== OAUTH — YOUTUBE (LIVES) — PROJETO/APP SEPARADO ([lives-sep]) =====================
// As LIVES usam uma 2ª credencial de YouTube ('youtube_lives') vinda de um PROJETO Google
// Cloud SEPARADO, pra ter COTA PRÓPRIA e NÃO disputar com os uploads do publicador.
// Mesmos escopos; token guardado sob plataforma 'youtube_lives'; callback próprio.
// Configurar a credencial: POST /pub/pub-cred { plataforma:'youtube_lives', clientId, clientSecret }.
// Conectar um canal: GET /pub/youtube-lives/connect?canalId=XXX  (login + autorizar).
// COMPATÍVEL: se o canal AINDA não tiver 'youtube_lives' configurado/conectado, as lives
// caem de volta no 'youtube' normal (comportamento atual) — nada quebra.
function pbYtLivesRedirect(): string { return (PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/pub/youtube-lives/callback'; }

app.get('/pub/youtube-lives/connect', (req, res) => {
  try {
    const canalId = String((req.query.canalId || req.query.canal_id || '')).trim();
    if (!canalId) return res.status(400).json({ ok: false, erro: 'canalId obrigatório' });
    const { clientId } = pbCred('youtube_lives');
    if (!clientId) return res.status(400).json({ ok: false, erro: 'YouTube (lives) client_id não configurado (POST /pub/pub-cred plataforma=youtube_lives)' });
    const params = new URLSearchParams({
      client_id: clientId, redirect_uri: pbYtLivesRedirect(), response_type: 'code', scope: PB_YT_SCOPES,
      access_type: 'offline', prompt: 'select_account consent', include_granted_scopes: 'false', state: `ytl:${canalId}`,
    });
    res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}` });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// ROTA PÚBLICA (Google chama sem x-api-key) — exceção adicionada no middleware.
app.get('/pub/youtube-lives/callback', async (req, res) => {
  const enviaHtml = (ok: boolean, msg: string) => res.status(200).type('html').send(pbHtmlResposta(ok, msg));
  try {
    const code = String(req.query.code || ''); const state = String(req.query.state || '');
    if (req.query.error) return enviaHtml(false, `Google: ${req.query.error}`);
    if (!code || !state.startsWith('ytl:')) return enviaHtml(false, 'callback inválido');
    const canalId = state.slice(4);
    const { clientId, clientSecret } = pbCred('youtube_lives');
    const tr = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: clientId, client_secret: clientSecret, redirect_uri: pbYtLivesRedirect(), grant_type: 'authorization_code' }),
    });
    if (!tr.ok) return enviaHtml(false, `troca de token falhou (${tr.status})`);
    const td = await tr.json() as any;
    const expiresAt = new Date(Date.now() + (td.expires_in || 3600) * 1000).toISOString();
    let conta = ''; let channelId = '';
    try { const ui = await (await fetch('https://openidconnect.googleapis.com/v1/userinfo', { headers: { Authorization: `Bearer ${td.access_token}` } })).json() as any; conta = ui.email || ui.name || ''; } catch { /* */ }
    try {
      const ch = await (await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers: { Authorization: `Bearer ${td.access_token}` } })).json() as any;
      if (ch.items && ch.items[0]) { channelId = ch.items[0].id; conta = ch.items[0].snippet?.title || conta; }
    } catch { /* */ }
    pbTokSalvar(canalId, 'youtube_lives', { accessToken: td.access_token, refreshToken: td.refresh_token || null, expiresAt, scope: td.scope || PB_YT_SCOPES, conta });
    pbConexao(canalId, 'youtube_lives', 'conectado', conta);
    enviaHtml(true, `YouTube (lives): ${conta || channelId || 'conta'}`);
  } catch (e: any) { enviaHtml(false, String(e?.message || e)); }
});

// Token p/ as LIVES. Se o canal não tiver 'youtube_lives' configurado/conectado, cai no 'youtube' normal.
async function pbYtTokenLives(canalId: string): Promise<string> {
  const cred = pbCred('youtube_lives');
  const rec = pbTokLer(canalId, 'youtube_lives');
  if (!cred.clientId || !rec) return pbYtToken(canalId); // compatível com o comportamento atual
  if (pbTokValido(rec)) return rec.accessToken;
  // [lives-saude] marca desconectado se não dá pra renovar — assim a tela inicial e o aviso do Telegram pegam.
  if (!rec.refreshToken) { try { pbConexao(canalId, 'youtube_lives', 'desconectado', 'token expirado, sem refresh'); } catch { /* */ } throw new Error('Token YouTube (lives) expirado e sem refresh — reconecte em /pub/youtube-lives/connect.'); }
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: cred.clientId, client_secret: cred.clientSecret, grant_type: 'refresh_token', refresh_token: rec.refreshToken }),
  });
  if (!r.ok) { const et = (await r.text()).slice(0, 200); try { pbConexao(canalId, 'youtube_lives', 'desconectado', `refresh ${r.status}`); } catch { /* */ } throw new Error(`Refresh YouTube (lives) falhou (${r.status}): ${et}`); }
  const d = await r.json() as any;
  const expiresAt = new Date(Date.now() + (d.expires_in || 3600) * 1000).toISOString();
  pbTokSalvar(canalId, 'youtube_lives', { accessToken: d.access_token, expiresAt, ...(d.scope ? { scope: d.scope } : {}) });
  try { pbConexao(canalId, 'youtube_lives', 'conectado', 'API'); } catch { /* */ }
  return d.access_token;
}

// ============================ OAUTH — META (Instagram + Facebook) ============================
const PB_META_VER = 'v21.0';
const PB_META_SCOPES = ['pages_show_list', 'pages_read_engagement', 'pages_manage_posts', 'business_management', 'instagram_basic', 'instagram_content_publish'].join(',');
function pbMetaRedirect(): string { return (PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/pub/meta/callback'; }

app.get('/pub/meta/connect', (req, res) => {
  try {
    const canalId = String((req.query.canalId || req.query.canal_id || '')).trim();
    if (!canalId) return res.status(400).json({ ok: false, erro: 'canalId obrigatório' });
    const { clientId } = pbCred('meta');
    if (!clientId) return res.status(400).json({ ok: false, erro: 'Meta app_id não configurado (POST /pub/pub-cred)' });
    const params = new URLSearchParams({ client_id: clientId, redirect_uri: pbMetaRedirect(), state: `meta:${canalId}`, scope: PB_META_SCOPES, response_type: 'code' });
    res.json({ ok: true, url: `https://www.facebook.com/${PB_META_VER}/dialog/oauth?${params.toString()}` });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// ROTA PÚBLICA (Facebook chama sem x-api-key)
app.get('/pub/meta/callback', async (req, res) => {
  const enviaHtml = (ok: boolean, msg: string) => res.status(200).type('html').send(pbHtmlResposta(ok, msg));
  try {
    const code = String(req.query.code || ''); const state = String(req.query.state || '');
    if (req.query.error) return enviaHtml(false, `Meta: ${req.query.error_description || req.query.error}`);
    if (!code || !state.startsWith('meta:')) return enviaHtml(false, 'callback inválido');
    const canalId = state.slice(5);
    const { clientId, clientSecret } = pbCred('meta');
    // code -> short-lived user token
    const t1 = await fetch(`https://graph.facebook.com/${PB_META_VER}/oauth/access_token?` + new URLSearchParams({ client_id: clientId, client_secret: clientSecret, redirect_uri: pbMetaRedirect(), code }).toString());
    if (!t1.ok) return enviaHtml(false, `troca de code falhou (${t1.status})`);
    const d1 = await t1.json() as any;
    // short -> long-lived user token (~60d)
    const t2 = await fetch(`https://graph.facebook.com/${PB_META_VER}/oauth/access_token?` + new URLSearchParams({ grant_type: 'fb_exchange_token', client_id: clientId, client_secret: clientSecret, fb_exchange_token: d1.access_token }).toString());
    const d2 = t2.ok ? await t2.json() as any : d1;
    const userTok = d2.access_token || d1.access_token;
    pbTokSalvar(canalId, 'meta_user', { accessToken: userTok, refreshToken: null, expiresAt: new Date(Date.now() + 55 * 864e5).toISOString(), scope: PB_META_SCOPES, conta: 'user' });
    // lista páginas + IG vinculado
    const pr = await fetch(`https://graph.facebook.com/${PB_META_VER}/me/accounts?fields=id,name,access_token,instagram_business_account&access_token=${encodeURIComponent(userTok)}`);
    const pd = pr.ok ? await pr.json() as any : { data: [] };
    const paginas = (pd.data || []).map((p: any) => ({ id: p.id, name: p.name, igId: p.instagram_business_account?.id || null }));
    setPubFlag(`meta_paginas_${canalId}`, JSON.stringify(paginas));
    pbConexao(canalId, 'meta', 'autorizado', `${paginas.length} página(s)`);
    const lista = paginas.map((p: any) => `• ${p.name} (page ${p.id}${p.igId ? `, IG ${p.igId}` : ', sem IG'})`).join('<br>');
    enviaHtml(true, `Meta autorizado. Páginas encontradas:<br>${lista || '(nenhuma)'}<br><br>Volte ao app pra vincular a página a este canal.`);
  } catch (e: any) { enviaHtml(false, String(e?.message || e)); }
});

// lista páginas achadas no connect (pro app/Kia escolher)
app.get('/pub/meta/paginas', (req, res) => {
  const canalId = String(req.query.canalId || req.query.canal_id || '');
  res.json({ ok: true, paginas: JSON.parse(pubFlag(`meta_paginas_${canalId}`, '[]')) });
});
// vincula uma página (+ IG) ao canal: busca o page token e salva tokens instagram/facebook
app.post('/pub/meta/vincular', async (req, res) => {
  try {
    const { canalId, pageId } = req.body || {};
    if (!canalId || !pageId) return res.status(400).json({ ok: false, erro: 'canalId e pageId obrigatórios' });
    const userRec = pbTokLer(String(canalId), 'meta_user');
    if (!userRec?.accessToken) return res.status(400).json({ ok: false, erro: 'conecte o Meta primeiro (/pub/meta/connect)' });
    const pr = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}?fields=access_token,name,instagram_business_account&access_token=${encodeURIComponent(userRec.accessToken)}`);
    const pd = await pr.json() as any;
    if (!pd.access_token) return res.status(400).json({ ok: false, erro: 'não obtive page token (o usuário tem permissão de conteúdo na página?)' });
    const igId = pd.instagram_business_account?.id || '';
    // page tokens de user de longa duração não expiram
    pbTokSalvar(String(canalId), 'facebook', { accessToken: pd.access_token, refreshToken: null, expiresAt: null, scope: PB_META_SCOPES, conta: pd.name || pageId });
    pbTokSalvar(String(canalId), 'instagram', { accessToken: pd.access_token, refreshToken: null, expiresAt: null, scope: PB_META_SCOPES, conta: igId });
    pbCanalSet(String(canalId), 'fb_page_id', String(pageId));
    if (igId) pbCanalSet(String(canalId), 'ig_business_id', String(igId));
    pbConexao(String(canalId), 'facebook', 'conectado', pd.name || pageId);
    if (igId) pbConexao(String(canalId), 'instagram', 'conectado', igId);
    res.json({ ok: true, pageId, igId });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// [#2 nuvem] vincular Meta por TOKEN manual (Usuario do Sistema / Page token de longa duracao),
// sem depender do login OAuth no navegador (apps tipo Empresa, em negocios diferentes).
// Recebe { canalId, pageId, token }: tenta obter o PAGE access token real + IG vinculado a partir
// do token informado e salva igual ao /pub/meta/vincular (facebook+instagram por canal). ADITIVO.
app.post('/pub/meta/token-manual', async (req, res) => {
  try {
    const b = req.body || {};
    const canalId = String(b.canalId || b.canal_id || '').trim();
    const pageId = String(b.pageId || b.page_id || '').trim();
    const tokenIn = String(b.token || b.pageToken || b.page_token || '').trim();
    if (!canalId || !pageId || !tokenIn) return res.status(400).json({ ok: false, erro: 'canalId, pageId e token obrigatorios' });
    let pageTok = tokenIn; let nome = ''; let igId = '';
    // 1) tenta derivar o page token (quando o token informado e de usuario/usuario-do-sistema com acesso a pagina)
    try {
      const pr = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}?fields=access_token,name,instagram_business_account&access_token=${encodeURIComponent(tokenIn)}`);
      const pd = await pr.json() as any;
      if (pd && pd.access_token) pageTok = String(pd.access_token);
      if (pd && pd.name) nome = String(pd.name);
      if (pd && pd.instagram_business_account && pd.instagram_business_account.id) igId = String(pd.instagram_business_account.id);
    } catch { /* usa o token informado como page token */ }
    // 2) se ainda nao achou o IG, tenta de novo ja com o page token
    if (!igId || !nome) {
      try {
        const ir = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}?fields=name,instagram_business_account&access_token=${encodeURIComponent(pageTok)}`);
        const idd = await ir.json() as any;
        if (!nome && idd && idd.name) nome = String(idd.name);
        if (!igId && idd && idd.instagram_business_account && idd.instagram_business_account.id) igId = String(idd.instagram_business_account.id);
      } catch { /* ok */ }
    }
    // 3) valida que o page token enxerga a pagina (GET /{pageId} devolve id)
    try {
      const vr = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}?fields=id&access_token=${encodeURIComponent(pageTok)}`);
      const vd = await vr.json() as any;
      if (!vd || !vd.id) return res.status(400).json({ ok: false, erro: 'token nao tem acesso a essa pagina (confira o pageId e as permissoes do token)', detalhe: (vd && vd.error && vd.error.message) || '' });
    } catch (e: any) { return res.status(400).json({ ok: false, erro: 'falha ao validar token: ' + String(e?.message || e) }); }
    pbTokSalvar(canalId, 'facebook', { accessToken: pageTok, refreshToken: null, expiresAt: null, scope: PB_META_SCOPES, conta: nome || pageId });
    pbTokSalvar(canalId, 'instagram', { accessToken: pageTok, refreshToken: null, expiresAt: null, scope: PB_META_SCOPES, conta: igId || '' });
    pbCanalSet(canalId, 'fb_page_id', pageId);
    if (igId) pbCanalSet(canalId, 'ig_business_id', igId);
    pbConexao(canalId, 'facebook', 'conectado', nome || pageId);
    if (igId) pbConexao(canalId, 'instagram', 'conectado', igId);
    res.json({ ok: true, canalId, pageId, igId: igId || null, conta: nome || pageId, derivouPageToken: pageTok !== tokenIn });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// vincula o Threads (usa o token do Meta/Facebook; descobre threads_user_id)
app.post('/pub/threads/vincular', async (req, res) => {
  try {
    const { canalId } = req.body || {};
    const tok = pbTokLer(String(canalId), 'facebook') || pbTokLer(String(canalId), 'instagram') || pbTokLer(String(canalId), 'threads');
    if (!tok?.accessToken) return res.status(400).json({ ok: false, erro: 'conecte o Meta primeiro' });
    const r = await fetch(`https://graph.threads.net/v1.0/me?fields=id,username&access_token=${encodeURIComponent(tok.accessToken)}`);
    const d = await r.json() as any;
    if (!d.id) return res.status(400).json({ ok: false, erro: `Threads /me falhou: ${JSON.stringify(d).slice(0, 200)}` });
    pbTokSalvar(String(canalId), 'threads', { accessToken: tok.accessToken, refreshToken: null, expiresAt: null, conta: d.username || d.id });
    pbCanalSet(String(canalId), 'threads_user_id', String(d.id));
    pbConexao(String(canalId), 'threads', 'conectado', d.username || d.id);
    res.json({ ok: true, threadsUserId: d.id, username: d.username });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// ============================ OAUTH — PINTEREST ============================
function pbPinRedirect(): string { return (PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/pub/pinterest/callback'; }
const PB_PIN_SCOPES = 'pins:read,pins:write,boards:read,user_accounts:read';

app.get('/pub/pinterest/connect', (req, res) => {
  try {
    const canalId = String((req.query.canalId || req.query.canal_id || '')).trim();
    if (!canalId) return res.status(400).json({ ok: false, erro: 'canalId obrigatório' });
    const { clientId } = pbCred('pinterest');
    if (!clientId) return res.status(400).json({ ok: false, erro: 'Pinterest client_id não configurado (POST /pub/pub-cred)' });
    const params = new URLSearchParams({ client_id: clientId, redirect_uri: pbPinRedirect(), response_type: 'code', scope: PB_PIN_SCOPES, state: `pin:${canalId}` });
    res.json({ ok: true, url: `https://www.pinterest.com/oauth/?${params.toString()}` });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// ROTA PÚBLICA
app.get('/pub/pinterest/callback', async (req, res) => {
  const enviaHtml = (ok: boolean, msg: string) => res.status(200).type('html').send(pbHtmlResposta(ok, msg));
  try {
    const code = String(req.query.code || ''); const state = String(req.query.state || '');
    if (req.query.error) return enviaHtml(false, `Pinterest: ${req.query.error}`);
    if (!code || !state.startsWith('pin:')) return enviaHtml(false, 'callback inválido');
    const canalId = state.slice(4);
    const { clientId, clientSecret } = pbCred('pinterest');
    const r = await fetch('https://api.pinterest.com/v5/oauth/token', {
      method: 'POST', headers: { Authorization: 'Basic ' + Buffer.from(`${clientId}:${clientSecret}`).toString('base64'), 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: pbPinRedirect() }),
    });
    if (!r.ok) return enviaHtml(false, `troca de token falhou (${r.status}): ${(await r.text()).slice(0, 150)}`);
    const d = await r.json() as any;
    const expiresAt = new Date(Date.now() + (d.expires_in || 30 * 86400) * 1000).toISOString();
    pbTokSalvar(canalId, 'pinterest', { accessToken: d.access_token, refreshToken: d.refresh_token || null, expiresAt, scope: d.scope || PB_PIN_SCOPES, conta: 'pinterest' });
    pbConexao(canalId, 'pinterest', 'conectado', 'pinterest');
    enviaHtml(true, 'Pinterest conectado. Volte ao app pra escolher o board.');
  } catch (e: any) { enviaHtml(false, String(e?.message || e)); }
});
async function pbPinToken(canalId: string): Promise<string> {
  const rec = pbTokLer(canalId, 'pinterest');
  if (!rec) throw new Error('Canal não conectado ao Pinterest.');
  if (pbTokValido(rec)) return rec.accessToken;
  if (!rec.refreshToken) throw new Error('Token Pinterest expirado e sem refresh — reconecte.');
  const { clientId, clientSecret } = pbCred('pinterest');
  const r = await fetch('https://api.pinterest.com/v5/oauth/token', {
    method: 'POST', headers: { Authorization: 'Basic ' + Buffer.from(`${clientId}:${clientSecret}`).toString('base64'), 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: rec.refreshToken }),
  });
  if (!r.ok) throw new Error(`Refresh Pinterest falhou (${r.status})`);
  const d = await r.json() as any;
  const expiresAt = new Date(Date.now() + (d.expires_in || 30 * 86400) * 1000).toISOString();
  pbTokSalvar(canalId, 'pinterest', { accessToken: d.access_token, expiresAt });
  return d.access_token;
}
app.get('/pub/pinterest/boards', async (req, res) => {
  try {
    const canalId = String(req.query.canalId || req.query.canal_id || '');
    const tok = await pbPinToken(canalId);
    const r = await fetch('https://api.pinterest.com/v5/boards?page_size=100', { headers: { Authorization: `Bearer ${tok}` } });
    const d = await r.json() as any;
    res.json({ ok: true, boards: (d.items || []).map((b: any) => ({ id: b.id, name: b.name })) });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
app.post('/pub/pinterest/vincular', (req, res) => {
  try {
    const { canalId, boardId } = req.body || {};
    if (!canalId || !boardId) return res.status(400).json({ ok: false, erro: 'canalId e boardId obrigatórios' });
    pbCanalSet(String(canalId), 'pinterest_board_id', String(boardId));
    res.json({ ok: true });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// HTML simples de retorno dos callbacks OAuth
function pbHtmlResposta(ok: boolean, msg: string): string {
  const cor = ok ? '#10b981' : '#ef4444'; const titulo = ok ? '✓ Conectado!' : '✗ Falhou';
  return `<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>${titulo}</title>
<style>body{font-family:system-ui,sans-serif;background:#0b0c0e;color:#fff;margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
.box{background:#131418;border:1px solid #1f2128;border-radius:14px;padding:32px 28px;max-width:460px;text-align:center}
h1{color:${cor};font-size:26px;margin:0 0 12px}</style></head>
<body><div class="box"><h1>${titulo}</h1><p>${msg}</p></div></body></html>`;
}

// ============================ UPLOADER — YOUTUBE ============================
async function pbYtUpload(canalId: string, filePath: string, opts: { titulo: string; descricao?: string; tags?: string[]; ehShorts?: boolean; privacy?: string; thumbFile?: string | null }): Promise<{ id: string; url: string }> {
  const token = await pbYtToken(canalId);
  const size = fs.statSync(filePath).size;
  let titulo = (opts.titulo || 'Sem título').slice(0, 100);
  if (opts.ehShorts && !titulo.toLowerCase().includes('#shorts')) titulo = `${titulo} #Shorts`.slice(0, 100);
  const initBody = {
    snippet: { title: titulo, description: (opts.descricao || '').slice(0, 5000), tags: (opts.tags || []).slice(0, 30), categoryId: '22' },
    status: { privacyStatus: opts.privacy || 'public', selfDeclaredMadeForKids: false },
  };
  const initResp = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
    method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': pbMime(filePath), 'X-Upload-Content-Length': String(size) },
    body: JSON.stringify(initBody),
  });
  if (!initResp.ok) throw new Error(`YT init (${initResp.status}): ${(await initResp.text()).slice(0, 300)}`);
  const uploadUrl = initResp.headers.get('location');
  if (!uploadUrl) throw new Error('YT não retornou Location.');
  const up = await fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': pbMime(filePath), 'Content-Length': String(size) }, body: fs.createReadStream(filePath) as any, duplex: 'half' } as any);
  if (!up.ok) throw new Error(`YT upload (${up.status}): ${(await up.text()).slice(0, 300)}`);
  const result = await up.json() as any;
  // thumbnail (opcional, não falha o upload)
  if (opts.thumbFile && fs.existsSync(opts.thumbFile)) {
    try {
      const tsize = fs.statSync(opts.thumbFile).size;
      await fetch(`https://www.googleapis.com/upload/youtube/v3/thumbnails/set?videoId=${result.id}`, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': pbMime(opts.thumbFile), 'Content-Length': String(tsize) }, body: fs.createReadStream(opts.thumbFile) as any, duplex: 'half' } as any);
    } catch (e) { pbLog('yt-thumb falhou', (e as any)?.message); }
  }
  return { id: result.id, url: `https://youtu.be/${result.id}` };
}

// ============================ UPLOADER — INSTAGRAM ============================
async function pbIgPoll(igToken: string, containerId: string, maxIter = 60): Promise<void> {
  let erros = 0;
  for (let i = 0; i < maxIter; i++) {
    await pbSleep(2000);
    const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${containerId}?fields=status_code,status&access_token=${encodeURIComponent(igToken)}`);
    if (!r.ok) { if (++erros >= 5) throw new Error('IG poll: 5 erros seguidos (container morto)'); continue; }
    erros = 0; const d = await r.json() as any;
    if (d.status_code === 'FINISHED') return;
    if (d.status_code === 'ERROR' || d.status_code === 'EXPIRED') throw new Error(`IG container ${d.status_code}: ${d.status || ''}`);
  }
  throw new Error('IG poll: timeout ~2min');
}
async function pbIgCriarContainer(igId: string, igToken: string, params: Record<string, string>): Promise<string> {
  const body = new URLSearchParams({ ...params, access_token: igToken });
  const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${igId}/media`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
  const d = await r.json() as any;
  if (!r.ok || !d.id) throw new Error(`IG container falhou: ${JSON.stringify(d).slice(0, 250)}`);
  return d.id;
}
async function pbIgPublicarContainer(igId: string, igToken: string, creationId: string): Promise<string> {
  for (let tent = 0; tent < 3; tent++) {
    const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${igId}/media_publish`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ creation_id: creationId, access_token: igToken }) });
    const d = await r.json() as any;
    if (r.ok && d.id) return d.id;
    if (JSON.stringify(d).includes('9007') && tent < 2) { await pbSleep(tent === 0 ? 10000 : 20000); continue; }
    throw new Error(`IG publish falhou: ${JSON.stringify(d).slice(0, 250)}`);
  }
  throw new Error('IG publish: esgotou tentativas');
}
async function pbIgRuploadBytes(containerId: string, igToken: string, filePath: string): Promise<void> {
  const bytes = fs.readFileSync(filePath);
  const r = await fetch(`https://rupload.facebook.com/ig-api-upload/${PB_META_VER}/${containerId}`, {
    method: 'POST', headers: { Authorization: `OAuth ${igToken}`, offset: '0', file_size: String(bytes.length) }, body: bytes,
  });
  if (!r.ok) throw new Error(`IG rupload (${r.status}): ${(await r.text()).slice(0, 200)}`);
}
async function pbIgPermalink(postId: string, igToken: string): Promise<string> {
  try { const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${postId}?fields=permalink&access_token=${encodeURIComponent(igToken)}`); const d = await r.json() as any; return d.permalink || ''; } catch { return ''; }
}
async function pbIgPublicar(canalId: string, a: { filePath: string; publicUrl: string; formato: string; caption: string }): Promise<{ id: string; url: string }> {
  const canal = pbCanal(canalId); const igId = canal?.ig_business_id;
  const tok = pbTokLer(canalId, 'instagram');
  if (!igId || !tok?.accessToken) throw new Error('Instagram não conectado neste canal.');
  const igToken = tok.accessToken; const f = a.formato;
  // IMAGEM (feed / stories / item de carrossel) — exige URL pública
  if (pbIsImagem(a.filePath)) {
    if (f === 'stories') {
      const c = await pbIgCriarContainer(igId, igToken, { image_url: a.publicUrl, media_type: 'STORIES' });
      await pbIgPoll(igToken, c); const id = await pbIgPublicarContainer(igId, igToken, c); return { id, url: '' };
    }
    const c = await pbIgCriarContainer(igId, igToken, { image_url: a.publicUrl, caption: a.caption });
    await pbIgPoll(igToken, c); const id = await pbIgPublicarContainer(igId, igToken, c);
    return { id, url: (await pbIgPermalink(id, igToken)) || `https://instagram.com/` };
  }
  // VÍDEO
  if (f === 'stories') {
    const c = await pbIgCriarContainer(igId, igToken, { media_type: 'STORIES', upload_type: 'resumable' });
    await pbIgRuploadBytes(c, igToken, a.filePath); await pbIgPoll(igToken, c);
    const id = await pbIgPublicarContainer(igId, igToken, c); return { id, url: '' };
  }
  const tipo = (f === 'horizontal') ? 'VIDEO' : 'REELS'; // vertical/reels => REELS; horizontal => VIDEO feed
  const params: Record<string, string> = { media_type: tipo, upload_type: 'resumable', caption: a.caption };
  if (tipo === 'REELS') params.share_to_feed = 'true';
  let c = '';
  try {
    c = await pbIgCriarContainer(igId, igToken, params);
    await pbIgRuploadBytes(c, igToken, a.filePath);
    await pbIgPoll(igToken, c);
  } catch (e) {
    // fallback: container por URL pública (crawler do Meta baixa)
    pbLog('ig rupload falhou, tentando por URL', (e as any)?.message);
    const p2: Record<string, string> = { media_type: tipo, video_url: a.publicUrl, caption: a.caption };
    if (tipo === 'REELS') p2.share_to_feed = 'true';
    c = await pbIgCriarContainer(igId, igToken, p2);
    await pbIgPoll(igToken, c);
  }
  const id = await pbIgPublicarContainer(igId, igToken, c);
  return { id, url: (await pbIgPermalink(id, igToken)) || `https://instagram.com/` };
}
// carrossel (pasta de imagens no OneDrive)
async function pbIgCarrossel(canalId: string, refsPublicos: string[], caption: string): Promise<{ id: string; url: string }> {
  const canal = pbCanal(canalId); const igId = canal?.ig_business_id; const tok = pbTokLer(canalId, 'instagram');
  if (!igId || !tok?.accessToken) throw new Error('Instagram não conectado neste canal.');
  const igToken = tok.accessToken;
  const filhos: string[] = [];
  for (const url of refsPublicos.slice(0, 10)) {
    const c = await pbIgCriarContainer(igId, igToken, { image_url: url, is_carousel_item: 'true' });
    await pbIgPoll(igToken, c); filhos.push(c);
  }
  if (filhos.length < 2) throw new Error('Carrossel precisa de 2+ imagens.');
  const master = await pbIgCriarContainer(igId, igToken, { media_type: 'CAROUSEL', children: filhos.join(','), caption });
  await pbIgPoll(igToken, master);
  const id = await pbIgPublicarContainer(igId, igToken, master);
  return { id, url: (await pbIgPermalink(id, igToken)) || `https://instagram.com/` };
}

// ============================ UPLOADER — FACEBOOK ============================
async function pbFbPublicar(canalId: string, a: { filePath: string; publicUrl: string; formato: string; titulo: string; descricao: string }): Promise<{ id: string; url: string }> {
  const canal = pbCanal(canalId); const pageId = canal?.fb_page_id;
  const tok = pbTokLer(canalId, 'facebook');
  if (!pageId || !tok?.accessToken) throw new Error('Facebook não conectado neste canal.');
  const pageTok = tok.accessToken; const f = a.formato;
  const desc = [a.titulo, a.descricao].filter(Boolean).join('\n\n');
  // FOTO
  if (pbIsImagem(a.filePath)) {
    const body = new URLSearchParams({ url: a.publicUrl, access_token: pageTok }); if (desc) body.set('message', desc);
    const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}/photos`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
    const d = await r.json() as any; if (!r.ok) throw new Error(`FB foto: ${JSON.stringify(d).slice(0, 200)}`);
    const pid = d.post_id || d.id; return { id: pid, url: `https://www.facebook.com/${pid}` };
  }
  // VÍDEO horizontal => feed video por file_url (sem subir bytes)
  if (f === 'horizontal') {
    const body = new URLSearchParams({ file_url: a.publicUrl, title: a.titulo, description: a.descricao || '', access_token: pageTok });
    const r = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}/videos`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
    const d = await r.json() as any; if (!r.ok || !d.id) throw new Error(`FB vídeo: ${JSON.stringify(d).slice(0, 200)}`);
    return { id: d.id, url: `https://www.facebook.com/${pageId}/videos/${d.id}` };
  }
  if (f === 'stories') throw new Error('PULAR'); // stories FB fica pra depois
  // REELS (vertical) — 3 fases com bytes
  const bytes = fs.readFileSync(a.filePath);
  const r1 = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}/video_reels`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ upload_phase: 'start', access_token: pageTok }) });
  const d1 = await r1.json() as any; if (!r1.ok || !d1.video_id || !d1.upload_url) throw new Error(`FB reels start: ${JSON.stringify(d1).slice(0, 200)}`);
  const r2 = await fetch(d1.upload_url, { method: 'POST', headers: { Authorization: `OAuth ${pageTok}`, offset: '0', file_size: String(bytes.length) }, body: bytes });
  if (!r2.ok) throw new Error(`FB reels transfer (${r2.status}): ${(await r2.text()).slice(0, 150)}`);
  const r3 = await fetch(`https://graph.facebook.com/${PB_META_VER}/${pageId}/video_reels`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ upload_phase: 'finish', video_id: d1.video_id, video_state: 'PUBLISHED', description: desc, access_token: pageTok }) });
  const d3 = await r3.json() as any; if (!r3.ok) throw new Error(`FB reels finish: ${JSON.stringify(d3).slice(0, 200)}`);
  return { id: d1.video_id, url: `https://www.facebook.com/reel/${d1.video_id}` };
}

// ============================ UPLOADER — THREADS ============================
async function pbThreadsPublicar(canalId: string, a: { publicUrl: string; filePath: string; formato: string; texto: string }): Promise<{ id: string; url: string }> {
  const canal = pbCanal(canalId); const uid = canal?.threads_user_id; const tok = pbTokLer(canalId, 'threads');
  if (!uid || !tok?.accessToken) throw new Error('Threads não conectado neste canal.');
  const token = tok.accessToken; const texto = (a.texto || '').slice(0, 500);
  const criar: Record<string, string> = { access_token: token, text: texto };
  if (pbIsImagem(a.filePath)) { criar.media_type = 'IMAGE'; criar.image_url = a.publicUrl; }
  else { criar.media_type = 'VIDEO'; criar.video_url = a.publicUrl; }
  const r1 = await fetch(`https://graph.threads.net/v1.0/${uid}/threads`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(criar) });
  const d1 = await r1.json() as any; if (!r1.ok || !d1.id) throw new Error(`Threads container: ${JSON.stringify(d1).slice(0, 200)}`);
  await pbSleep(criar.media_type === 'VIDEO' ? 8000 : 2000); // dá um tempo pro processamento
  const r2 = await fetch(`https://graph.threads.net/v1.0/${uid}/threads_publish`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ access_token: token, creation_id: d1.id }) });
  const d2 = await r2.json() as any; if (!r2.ok || !d2.id) throw new Error(`Threads publish: ${JSON.stringify(d2).slice(0, 200)}`);
  return { id: d2.id, url: `https://www.threads.net/@_/post/${d2.id}` };
}

// ============================ UPLOADER — PINTEREST ============================
async function pbPinterestPublicar(canalId: string, a: { filePath: string; formato: string; titulo: string; descricao: string }): Promise<{ id: string; url: string }> {
  const canal = pbCanal(canalId); const boardId = canal?.pinterest_board_id;
  if (!boardId) throw new Error('Pinterest sem board vinculado neste canal.');
  const token = await pbPinToken(canalId);
  const titulo = (a.titulo || '').slice(0, 100); const descricao = (a.descricao || '').slice(0, 500);
  let mediaSource: any;
  if (pbIsImagem(a.filePath)) {
    const b64 = fs.readFileSync(a.filePath).toString('base64');
    mediaSource = { source_type: 'image_base64', content_type: pbMime(a.filePath), data: b64 };
  } else {
    // vídeo: registra mídia -> upload presigned -> poll -> cria pin com media_id
    const reg = await fetch('https://api.pinterest.com/v5/media', { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ media_type: 'video' }) });
    const rd = await reg.json() as any; if (!reg.ok || !rd.media_id) throw new Error(`Pinterest register: ${JSON.stringify(rd).slice(0, 200)}`);
    const fd = new FormData();
    for (const [k, v] of Object.entries(rd.upload_parameters || {})) fd.append(k, String(v));
    fd.append('file', new Blob([fs.readFileSync(a.filePath)]), pbBasename(a.filePath));
    const up = await fetch(rd.upload_url, { method: 'POST', body: fd as any });
    if (!(up.ok || up.status === 204)) throw new Error(`Pinterest upload (${up.status})`);
    let pronto = false;
    for (let i = 0; i < 30; i++) { await pbSleep(3000); const st = await (await fetch(`https://api.pinterest.com/v5/media/${rd.media_id}`, { headers: { Authorization: `Bearer ${token}` } })).json() as any; if (st.status === 'succeeded') { pronto = true; break; } if (st.status === 'failed') throw new Error('Pinterest media failed'); }
    if (!pronto) throw new Error('Pinterest media timeout');
    mediaSource = { source_type: 'video_id', media_id: rd.media_id };
  }
  const r = await fetch('https://api.pinterest.com/v5/pins', { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ board_id: boardId, title: titulo, description: descricao, media_source: mediaSource }) });
  const d = await r.json() as any; if (!r.ok || !d.id) throw new Error(`Pinterest pin: ${JSON.stringify(d).slice(0, 200)}`);
  return { id: d.id, url: `https://www.pinterest.com/pin/${d.id}/` };
}

// ============================ DESPACHANTE (posta 1 item) ============================
function pbCarrosselUrls(onedriveRef: string): string[] {
  try {
    const arr = _odRcloneListar(onedriveRef) as any[]; // lista arquivos sob base/<ref>
    const imgs = (arr || []).filter((x) => pbIsImagem(x.Path || x.Name || '')).map((x) => (x.Path ? String(x.Path) : `${onedriveRef}/${x.Name}`));
    const urls: string[] = [];
    for (const rel of imgs.slice(0, 10)) { const m = pbBaixarMidia(rel); urls.push(m.publicUrl); }
    return urls;
  } catch { return []; }
}
async function pbPostarItem(item: any): Promise<void> {
  const canalId = String(item.canal_id);
  db.prepare(`UPDATE fila_publicacao SET status='publicando', erro=NULL, atualizado_em=? WHERE id=?`).run(agora(), item.id);
  const plataformas: string[] = (() => { try { return JSON.parse(item.plataformas_json || '[]'); } catch { return []; } })();
  const formato = String(item.formato || 'vertical');
  const titulo = item.titulo || (item.arquivo_nome ? item.arquivo_nome.replace(/\.[^.]+$/, '') : 'Sem título');
  const descYt = item.descricao || '';
  const captionSocial = item.descricao_social || item.descricao || '';
  const ehVertical = (formato === 'vertical' || formato === 'reels' || formato === 'stories');

  // baixa a mídia principal (carrossel é pasta -> trata à parte)
  let midia: { file: string; publicUrl: string; nome: string } | null = null;
  let carrosselUrls: string[] = [];
  try {
    if (formato === 'carrossel') { carrosselUrls = pbCarrosselUrls(item.onedrive_ref); if (!carrosselUrls.length) midia = pbBaixarMidia(item.onedrive_ref); }
    else { midia = pbBaixarMidia(item.onedrive_ref); }
  } catch (e) {
    db.prepare(`UPDATE fila_publicacao SET status='agendado', data_agendada=?, erro=?, atualizado_em=? WHERE id=?`).run(new Date(Date.now() + 30 * 60000).toISOString(), 'falha ao baixar do OneDrive: ' + String((e as any)?.message || e), agora(), item.id);
    return;
  }
  let thumbFile: string | null = null;
  if (item.thumb_ref) { try { thumbFile = pbBaixarMidia(item.thumb_ref).file; } catch { /* */ } }

  const resultados: Record<string, string> = {};
  const videoIds: Record<string, string> = {};
  let sucessos = 0; let tentados = 0;

  for (const p of plataformas) {
    try {
      let r: { id: string; url: string };
      const file = midia?.file || ''; const publicUrl = midia?.publicUrl || '';
      if (p === 'youtube') {
        if (!midia || pbIsImagem(file) || formato === 'carrossel') { resultados[p] = 'PULADO: YouTube só vídeo aqui'; continue; }
        r = await pbYtUpload(canalId, file, { titulo, descricao: descYt, ehShorts: ehVertical, privacy: pbFlagPriv(canalId), thumbFile });
      } else if (p === 'instagram') {
        if (formato === 'carrossel') { if (!carrosselUrls.length) { resultados[p] = 'PULADO: carrossel sem imagens'; continue; } r = await pbIgCarrossel(canalId, carrosselUrls, captionSocial); }
        else { if (!midia) { resultados[p] = 'PULADO: sem mídia'; continue; } r = await pbIgPublicar(canalId, { filePath: file, publicUrl, formato, caption: captionSocial }); }
      } else if (p === 'facebook') {
        if (formato === 'carrossel' || !midia) { resultados[p] = 'PULADO: FB não faz carrossel aqui'; continue; }
        r = await pbFbPublicar(canalId, { filePath: file, publicUrl, formato, titulo, descricao: captionSocial });
      } else if (p === 'threads') {
        if (formato === 'carrossel' || !midia) { resultados[p] = 'PULADO: Threads sem carrossel aqui'; continue; }
        r = await pbThreadsPublicar(canalId, { publicUrl, filePath: file, formato, texto: captionSocial || titulo });
      } else if (p === 'pinterest') {
        if (formato === 'carrossel' || !midia) { resultados[p] = 'PULADO: Pinterest sem carrossel aqui'; continue; }
        r = await pbPinterestPublicar(canalId, { filePath: file, formato, titulo, descricao: captionSocial || descYt });
      } else if (p === 'tiktok' || p === 'kwai') {
        resultados[p] = 'PULADO: ' + p + ' precisa do proxy BR (fase navegador)'; continue;
      } else { resultados[p] = 'PULADO: plataforma desconhecida'; continue; }
      resultados[p] = 'OK: ' + (r.url || r.id); videoIds[p] = r.id; sucessos++; tentados++;
    } catch (e) {
      const msg = String((e as any)?.message || e);
      if (msg === 'PULAR' || msg.startsWith('PULAR')) { resultados[p] = 'PULADO: ' + msg.replace(/^PULAR:?\s*/, ''); }
      else { resultados[p] = 'ERRO: ' + msg; tentados++; }
    }
  }

  db.prepare(`UPDATE fila_publicacao SET resultado_por_plataforma_json=?, video_ids_externos_json=?, atualizado_em=? WHERE id=?`)
    .run(JSON.stringify(resultados), JSON.stringify(videoIds), agora(), item.id);

  if (sucessos > 0) {
    // sucesso (total ou parcial): marca publicado, nunca re-posta
    db.prepare(`UPDATE fila_publicacao SET status='publicado', tentativas_consecutivas=0, erro=?, atualizado_em=? WHERE id=?`)
      .run(sucessos < tentados ? 'parcial: ' + JSON.stringify(resultados) : null, agora(), item.id);
    pbLog('publicado', item.id, `${sucessos}/${tentados} rede(s)`);
  } else if (tentados === 0) {
    // nada aplicável (ex.: imagem só com youtube) — tira da fila
    db.prepare(`UPDATE fila_publicacao SET status='publicado', erro='nada a postar (todas as redes puladas)', atualizado_em=? WHERE id=?`).run(agora(), item.id);
  } else {
    // todas falharam: backoff exponencial até 20 tentativas, depois quarentena
    const n = (item.tentativas_consecutivas || 0) + 1;
    if (n >= 20) {
      db.prepare(`UPDATE fila_publicacao SET status='quarentena', tentativas_consecutivas=?, erro=?, atualizado_em=? WHERE id=?`).run(n, JSON.stringify(resultados).slice(0, 500), agora(), item.id);
    } else {
      const horas = Math.min(24, Math.pow(2, n - 1)); const jitterMs = Math.floor(Math.random() * 30 * 60000);
      const prox = new Date(Date.now() + horas * 3600000 + jitterMs).toISOString();
      db.prepare(`UPDATE fila_publicacao SET status='agendado', tentativas_consecutivas=?, data_agendada=?, erro=?, atualizado_em=? WHERE id=?`).run(n, prox, JSON.stringify(resultados).slice(0, 500), agora(), item.id);
    }
  }
}

// ============================ WORKER (inerte até publicador_ativo=1) ============================
function pbComTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error('timeout ' + ms + 'ms')), ms))]);
}
let _pubBusy = false;
async function tickPublicador() {
  if (pubFlag('publicador_ativo', '0') !== '1') return;
  if (_pubBusy) return; _pubBusy = true;
  try {
    const nowIso = new Date().toISOString();
    const itens = db.prepare(`SELECT * FROM fila_publicacao WHERE status='agendado' AND data_agendada<=? AND COALESCE(aguardando_instrumento,0)=0 ORDER BY COALESCE(prioridade,0) DESC, data_agendada ASC LIMIT 5`).all(nowIso) as any[];
    for (const item of itens) {
      try { await pbComTimeout(pbPostarItem(item), 8 * 60 * 1000); }
      catch (e) { try { db.prepare(`UPDATE fila_publicacao SET status='agendado', data_agendada=?, erro=?, atualizado_em=? WHERE id=?`).run(new Date(Date.now() + 30 * 60000).toISOString(), 'timeout/erro worker: ' + String((e as any)?.message || e), agora(), item.id); } catch { /* */ } }
    }
  } finally { _pubBusy = false; }
}
setInterval(() => { tickPublicador().catch(() => { }); }, 30000);

// ============================ ENDPOINTS DE CONTROLE / CALENDÁRIO ============================
// configura client_id/secret de uma plataforma (cifrado)
app.post('/pub/pub-cred', (req, res) => {
  try { const { plataforma, clientId, clientSecret } = req.body || {}; if (!plataforma) return res.status(400).json({ ok: false, erro: 'plataforma obrigatória' }); pbCredSalvar(plataforma, clientId || '', clientSecret || ''); res.json({ ok: true }); }
  catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// liga/desliga o publicador
app.post('/pub/publicador/ligar', (_req, res) => { setPubFlag('publicador_ativo', '1'); res.json({ ok: true, ativo: true }); });
// [yt-privacidade] escolhe public|unlisted|private do YouTube (global e por canal). Padrão: public.
function pbFlagPriv(canalId) {
  var v = pubFlag('pub_yt_privacy_' + String(canalId || ''), '') || pubFlag('pub_yt_privacy', 'public');
  return (v === 'unlisted' || v === 'private') ? v : 'public';
}
app.get('/pub/publicador/privacidade', (req, res) => {
  var canal = String((req.query && req.query.canal) || '');
  res.json({ ok: true, global: pubFlag('pub_yt_privacy', 'public'), canal: canal, canalValor: pubFlag('pub_yt_privacy_' + canal, ''), efetivo: pbFlagPriv(canal) });
});
app.post('/pub/publicador/privacidade', (req, res) => {
  var b = req.body || {}; var v = String(b.valor || 'public');
  if (['public','unlisted','private'].indexOf(v) < 0) return res.status(400).json({ ok: false, erro: 'valor: public|unlisted|private' });
  if (b.canal) setPubFlag('pub_yt_privacy_' + String(b.canal), v); else setPubFlag('pub_yt_privacy', v);
  res.json({ ok: true, privacidade: v, canal: b.canal || null });
});

app.post('/pub/publicador/desligar', (_req, res) => { setPubFlag('publicador_ativo', '0'); res.json({ ok: true, ativo: false }); });
app.get('/pub/publicador/status', (_req, res) => {
  try {
    const porStatus = db.prepare(`SELECT status, COUNT(*) n FROM fila_publicacao GROUP BY status`).all() as any[];
    const prox = db.prepare(`SELECT id, canal_id, formato, plataformas_json, titulo, data_agendada FROM fila_publicacao WHERE status='agendado' ORDER BY data_agendada ASC LIMIT 10`).all();
    res.json({ ok: true, ativo: pubFlag('publicador_ativo', '0') === '1', porStatus, proximos: prox });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// posta 1 item AGORA (teste), mesmo com o publicador desligado. body: {id?}
app.post('/pub/publicador/testar', async (req, res) => {
  try {
    const id = req.body?.id;
    const item = id ? db.prepare(`SELECT * FROM fila_publicacao WHERE id=?`).get(String(id)) as any
      : db.prepare(`SELECT * FROM fila_publicacao WHERE status='agendado' ORDER BY data_agendada ASC LIMIT 1`).get() as any;
    if (!item) return res.status(404).json({ ok: false, erro: 'nenhum item pra testar' });
    await pbComTimeout(pbPostarItem(item), 8 * 60 * 1000);
    const depois = db.prepare(`SELECT status, resultado_por_plataforma_json, erro FROM fila_publicacao WHERE id=?`).get(item.id);
    res.json({ ok: true, item: item.id, resultado: depois });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// lista conexões por canal/plataforma
app.get('/pub/conexoes/listar', (_req, res) => {
  try {
    const canais = db.prepare(`SELECT id, nome, instrumento FROM canais_publicacao WHERE ativo=1`).all() as any[];
    const toks = db.prepare(`SELECT canal_id, plataforma, conta, expires_at FROM oauth_tokens_pub`).all() as any[];
    res.json({ ok: true, canais, conexoes: toks });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});
// importa/espelha a agenda do app -> motor. body: {itens:[{id,canalId,onedriveRef,thumbRef?,formato,plataformas:[],titulo,descricao?,descricaoSocial?,dataAgendada}]}
app.post('/pub/fila/importar', (req, res) => {
  try {
    const itens: any[] = (req.body?.itens) || [];
    const stmt = db.prepare(`INSERT INTO fila_publicacao(id,canal_id,projeto,onedrive_ref,thumb_ref,formato,plataformas_json,titulo,descricao,descricao_social,data_agendada,status,criado_em,atualizado_em)
      VALUES(?,?,?,?,?,?,?,?,?,?,?, 'agendado', ?, ?)
      ON CONFLICT(id) DO UPDATE SET canal_id=excluded.canal_id, onedrive_ref=excluded.onedrive_ref, thumb_ref=excluded.thumb_ref,
        formato=excluded.formato, plataformas_json=excluded.plataformas_json, titulo=excluded.titulo, descricao=excluded.descricao,
        descricao_social=excluded.descricao_social, data_agendada=excluded.data_agendada, atualizado_em=excluded.atualizado_em`);
    let n = 0;
    const tx = db.transaction((rows: any[]) => {
      for (const it of rows) {
        const id = String(it.id || randomUUID());
        stmt.run(id, String(it.canalId ?? it.canal_id ?? ''), it.projeto || null, it.onedriveRef || it.onedrive_ref || '', it.thumbRef || it.thumb_ref || null,
          it.formato || 'vertical', JSON.stringify(it.plataformas || it.plataformas_json || []), it.titulo || '', it.descricao || null, it.descricaoSocial || it.descricao_social || null,
          it.dataAgendada || it.data_agendada || new Date().toISOString(), agora(), agora());
        n++;
      }
    });
    tx(itens);
    res.json({ ok: true, importados: n });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

console.log('[etapa28] publicador na nuvem carregado (INERTE ate publicador_ativo=1): YT/IG/FB/Threads/Pinterest + worker + import de calendario');


// ============================================================================
// [etapa29] CAPI — Conversions API do Meta (envia vendas pro Meta) — INERTE até capi_ativo=1
// ----------------------------------------------------------------------------
// ADITIVO. Bloco inserido no index.ts do motor (antes do app.listen), igual ao etapa28.
// Reaproveita os helpers já existentes: db (better-sqlite3), app (express), cifra/decifra,
// pubFlag/setPubFlag, agora(), createHash, randomUUID.
//
// O que faz:
//  - Recebe webhook de venda (Hotmart/Kiwify/Guru/LastLink) em rota PÚBLICA /capi/webhook/:fonte
//  - Normaliza (email/telefone/valor/produto), faz HASH SHA-256 dos dados pessoais
//  - Envia o evento (Purchase/Lead/InitiateCheckout) pro Meta via Graph API (CAPI)
//  - Grava cada envio em capi_eventos (alimenta a aba "Eventos do pixel" do app)
//  - Config do pixel (pixel_id + token) cifrada em capi_pixels, por produto/canal
//
// SEGURANÇA: enquanto pubFlag('capi_ativo') != '1', NADA é enviado ao Meta — só grava o
// evento como 'inerte' pra você ver que chegou. Ligar é POST /capi/ligar.
// ============================================================================
{
  // --- tabelas (idempotente) ---
  db.exec(`
    CREATE TABLE IF NOT EXISTS capi_pixels (
      id TEXT PRIMARY KEY,
      produto TEXT,
      pixel_id TEXT NOT NULL,
      access_token_enc BLOB NOT NULL,
      test_event_code TEXT,
      conta TEXT,
      criado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS capi_eventos (
      id TEXT PRIMARY KEY,
      ts TEXT,
      status TEXT,            -- ok | erro | inerte
      fonte TEXT,             -- hotmart | kiwify | guru | lastlink | teste
      evento TEXT,            -- Purchase | Lead | InitiateCheckout | ViewContent
      pixel_id TEXT,
      valor_centavos INTEGER,
      moeda TEXT,
      pagina TEXT,
      produto TEXT,
      resposta TEXT,
      erro TEXT
    );
  `);

  const capiHash = (v: string | null | undefined): string | undefined => {
    if (!v) return undefined;
    const s = String(v).trim().toLowerCase();
    if (!s) return undefined;
    return createHash('sha256').update(s).digest('hex');
  };
  const capiHashFone = (v: string | null | undefined): string | undefined => {
    if (!v) return undefined;
    const so = String(v).replace(/\D/g, '');
    if (!so) return undefined;
    // E.164 sem '+', assume BR se vier sem DDI
    const comDdi = so.length <= 11 ? `55${so}` : so;
    return createHash('sha256').update(comDdi).digest('hex');
  };

  interface CapiPixel { id: string; produto: string | null; pixel_id: string; token: string; test_event_code: string | null; conta: string | null }
  const capiPixelLer = (id: string): CapiPixel | null => {
    const r: any = db.prepare('SELECT * FROM capi_pixels WHERE id = ?').get(id);
    if (!r) return null;
    return { id: r.id, produto: r.produto, pixel_id: r.pixel_id, token: decifra(r.access_token_enc), test_event_code: r.test_event_code, conta: r.conta };
  };
  const capiPixelPadrao = (): CapiPixel | null => {
    const r: any = db.prepare('SELECT * FROM capi_pixels ORDER BY criado_em ASC LIMIT 1').get();
    if (!r) return null;
    return { id: r.id, produto: r.produto, pixel_id: r.pixel_id, token: decifra(r.access_token_enc), test_event_code: r.test_event_code, conta: r.conta };
  };

  const capiLog = (e: {
    status: string; fonte: string; evento: string; pixel_id?: string; valor_centavos?: number | null;
    pagina?: string | null; produto?: string | null; resposta?: string | null; erro?: string | null;
  }): void => {
    db.prepare(`INSERT INTO capi_eventos (id, ts, status, fonte, evento, pixel_id, valor_centavos, moeda, pagina, produto, resposta, erro)
      VALUES (@id,@ts,@status,@fonte,@evento,@pixel_id,@valor_centavos,'BRL',@pagina,@produto,@resposta,@erro)`).run({
      id: randomUUID(), ts: agora(), status: e.status, fonte: e.fonte, evento: e.evento,
      pixel_id: e.pixel_id || null, valor_centavos: e.valor_centavos ?? null,
      pagina: e.pagina || null, produto: e.produto || null, resposta: e.resposta || null, erro: e.erro || null
    });
  };

  // --- envio pro Meta (Graph API CAPI) ---
  // [#5 rastreio EMQ] Advanced Matching máximo: além de email/telefone, mandamos nome/cidade/UF/CEP/país
  // (hash SHA-256), fbc/fbp (cookies do clique), IP e user-agent (sem hash) e external_id estável → sobe o EMQ.
  interface CapiDados { email?: string | null; telefone?: string | null; valorCentavos?: number | null; produto?: string | null; pagina?: string | null; eventId?: string | null;
    fbc?: string | null; fbp?: string | null; clientIp?: string | null; userAgent?: string | null;
    firstName?: string | null; lastName?: string | null; city?: string | null; state?: string | null; zip?: string | null; country?: string | null; externalId?: string | null }
  // normaliza + hash SHA-256 (nomes/cidade/UF: minúsculo sem espaço nas bordas; CEP: só dígitos; país: 2 letras).
  const capiHashCampo = (v: string | null | undefined, tipo?: 'zip' | 'pais'): string | undefined => {
    if (v == null) return undefined;
    let s = String(v).trim().toLowerCase();
    if (tipo === 'zip') s = s.replace(/[^a-z0-9]/g, '');
    else if (tipo === 'pais') { s = s.replace(/[^a-z]/g, ''); if (s === 'brasil' || s === 'brazil') s = 'br'; s = s.slice(0, 2); }
    else s = s.replace(/\s+/g, ' ').trim();
    if (!s) return undefined;
    return createHash('sha256').update(s).digest('hex');
  };
  // etapa30: token CENTRAL (1 token do System User serve pra todos os pixels). Guardado em capi_pixels id='__central__'.
  const capiTokenCentral = (): string => {
    const r: any = db.prepare("SELECT access_token_enc FROM capi_pixels WHERE id = '__central__'").get();
    if (!r) return '';
    try { return decifra(r.access_token_enc); } catch { return ''; }
  };
  const capiEnviar = async (px: CapiPixel, eventName: string, d: CapiDados): Promise<{ ok: boolean; resposta: string }> => {
    const tokenUsar = px.token || capiTokenCentral(); // usa o token do pixel OU o central
    const userData: Record<string, any> = {};
    const em = capiHash(d.email); if (em) userData.em = [em];
    const ph = capiHashFone(d.telefone); if (ph) userData.ph = [ph];
    // [#5 rastreio EMQ] mais sinais de correspondência (quanto mais, maior o match quality)
    const fn = capiHashCampo(d.firstName); if (fn) userData.fn = [fn];
    const ln = capiHashCampo(d.lastName); if (ln) userData.ln = [ln];
    const ct = capiHashCampo(d.city); if (ct) userData.ct = [ct];
    const st = capiHashCampo(d.state); if (st) userData.st = [st];
    const zp = capiHashCampo(d.zip, 'zip'); if (zp) userData.zp = [zp];
    const co = capiHashCampo(d.country, 'pais'); if (co) userData.country = [co];
    const exid = d.externalId ? (capiHash(d.externalId) || capiHash(d.email)) : capiHash(d.email); if (exid) userData.external_id = [exid];
    if (d.fbc) userData.fbc = String(d.fbc);
    if (d.fbp) userData.fbp = String(d.fbp);
    if (d.clientIp) userData.client_ip_address = String(d.clientIp);
    if (d.userAgent) userData.client_user_agent = String(d.userAgent);
    const evento: any = {
      event_name: eventName,
      event_time: Math.floor(Date.now() / 1000),
      action_source: 'website',
      event_id: d.eventId || randomUUID(),
      user_data: userData
    };
    if (d.pagina) evento.event_source_url = d.pagina;
    if (d.valorCentavos != null || d.produto) {
      evento.custom_data = {};
      if (d.valorCentavos != null) { evento.custom_data.value = Number((d.valorCentavos / 100).toFixed(2)); evento.custom_data.currency = 'BRL'; }
      if (d.produto) evento.custom_data.content_name = d.produto;
    }
    const body: any = { data: [evento] };
    if (px.test_event_code) body.test_event_code = px.test_event_code;
    const url = `https://graph.facebook.com/v21.0/${encodeURIComponent(px.pixel_id)}/events?access_token=${encodeURIComponent(tokenUsar)}`;
    const resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const txt = await resp.text();
    return { ok: resp.ok, resposta: txt.slice(0, 500) };
  };

  // ============================================================================
  // [#5 rastreio] Servico /track/* — Meta CAPI + Google GA4 unificados (add-only)
  // Config por projeto em track_config (tokens cifrados). Usado pela tela "Pixels"
  // (Rastreamento avancado). O Purchase real (servidor) entra pelos webhooks de pagamento.
  // ============================================================================
  db.exec(`
    CREATE TABLE IF NOT EXISTS track_config (
      projeto TEXT PRIMARY KEY,
      meta_pixel_id TEXT,
      meta_token_enc BLOB,
      ga4_mid TEXT,
      ga4_secret_enc BLOB,
      test_code TEXT,
      dominio TEXT,
      ativo INTEGER DEFAULT 1,
      criado_em TEXT
    );
  `);
  interface TrackCfg { projeto:string; meta_pixel_id:string|null; meta_token:string; ga4_mid:string|null; ga4_secret:string; test_code:string|null; dominio:string|null; ativo:number }
  const trackLer = (projeto:string): TrackCfg | null => {
    const r:any = db.prepare('SELECT * FROM track_config WHERE projeto = ?').get(projeto);
    if (!r) return null;
    let mt=''; try{ mt = r.meta_token_enc ? decifra(r.meta_token_enc) : ''; }catch{}
    let gs=''; try{ gs = r.ga4_secret_enc ? decifra(r.ga4_secret_enc) : ''; }catch{}
    return { projeto:r.projeto, meta_pixel_id:r.meta_pixel_id, meta_token:mt, ga4_mid:r.ga4_mid, ga4_secret:gs, test_code:r.test_code, dominio:r.dominio, ativo: r.ativo==null?1:r.ativo };
  };
  const ga4MapEvento = (n:string):string => (({ Purchase:'purchase', Lead:'generate_lead', InitiateCheckout:'begin_checkout', ViewContent:'view_item', AddToCart:'add_to_cart', CompleteRegistration:'sign_up' } as any)[n]) || String(n||'').toLowerCase();
  const ga4HashFone = (v?:string|null):string|undefined => { if(!v) return undefined; const so=String(v).replace(/\D/g,''); if(!so) return undefined; const comDdi = so.length<=11 ? ('55'+so) : so; return createHash('sha256').update('+'+comDdi).digest('hex'); };
  const ga4Enviar = async (mid:string, secret:string, eventName:string, d:CapiDados): Promise<{ ok:boolean; status:number; resposta:string }> => {
    const name = ga4MapEvento(eventName);
    const params:any = { engagement_time_msec: 1 };
    if (d.valorCentavos != null) { params.currency='BRL'; params.value = Number((d.valorCentavos/100).toFixed(2)); }
    params.transaction_id = d.eventId || randomUUID();
    if (d.produto) params.items = [{ item_name: String(d.produto) }];
    const userData:any = {};
    const em = capiHash(d.email); if (em) userData.sha256_email_address = [em];
    const ph = ga4HashFone(d.telefone); if (ph) userData.sha256_phone_number = [ph];
    // [#5 rastreio EMQ] Enhanced Conversions: nome (hash) + cidade/UF/CEP/país (texto) no address
    const fn = capiHashCampo(d.firstName); const ln = capiHashCampo(d.lastName);
    const addr:any = {};
    if (fn) addr.sha256_first_name = fn;
    if (ln) addr.sha256_last_name = ln;
    if (d.city) addr.city = String(d.city).trim().toLowerCase();
    if (d.state) addr.region = String(d.state).trim().toLowerCase();
    if (d.zip) addr.postal_code = String(d.zip).replace(/\s+/g, '');
    if (d.country) addr.country = String(d.country).trim().toLowerCase().slice(0, 2);
    if (Object.keys(addr).length) userData.address = [addr];
    const body:any = { client_id: String(d.externalId || d.eventId || randomUUID()), events: [{ name, params }] };
    if (Object.keys(userData).length) body.user_data = userData;
    const url = 'https://www.google-analytics.com/mp/collect?measurement_id=' + encodeURIComponent(mid) + '&api_secret=' + encodeURIComponent(secret);
    const resp = await fetch(url, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    let txt=''; try{ txt = await resp.text(); }catch{}
    return { ok: resp.ok, status: resp.status, resposta: (txt || ('HTTP '+resp.status)).slice(0,500) };
  };
  const trackEnviarTudo = async (cfg:TrackCfg, eventName:string, d:CapiDados, fonte:string): Promise<{ meta:any; ga4:any }> => {
    const eid = d.eventId || randomUUID(); const dd:CapiDados = { ...d, eventId: eid };
    let meta:any = { ok:false, status:0, pulou:true };
    let ga4:any  = { ok:false, status:0, pulou:true };
    if (cfg.meta_pixel_id && (cfg.meta_token || capiTokenCentral())) {
      try {
        const px:CapiPixel = { id:cfg.projeto, produto:cfg.projeto, pixel_id:cfg.meta_pixel_id, token: cfg.meta_token || capiTokenCentral(), test_event_code: cfg.test_code || null, conta:null };
        const r = await capiEnviar(px, eventName, dd);
        meta = { ok:r.ok, status: r.ok?200:0, resposta:r.resposta, erro: r.ok?null:r.resposta };
        capiLog({ status:r.ok?'ok':'erro', fonte, evento:eventName, pixel_id:cfg.meta_pixel_id, valor_centavos:dd.valorCentavos, pagina:dd.pagina, produto:dd.produto, resposta:r.resposta, erro:r.ok?null:r.resposta });
      } catch(err:any){ meta={ ok:false, status:0, erro:String(err?.message||err) }; try{ capiLog({status:'erro',fonte,evento:eventName,pixel_id:cfg.meta_pixel_id,erro:String(err?.message||err)}); }catch{} }
    }
    if (cfg.ga4_mid && cfg.ga4_secret) {
      try {
        const r = await ga4Enviar(cfg.ga4_mid, cfg.ga4_secret, eventName, dd);
        ga4 = { ok:r.ok, status:r.status, resposta:r.resposta, erro: r.ok?null:r.resposta };
        capiLog({ status:r.ok?'ok':'erro', fonte, evento:'[GA4] '+eventName, pixel_id:cfg.ga4_mid, valor_centavos:dd.valorCentavos, pagina:dd.pagina, produto:dd.produto, resposta:r.resposta, erro:r.ok?null:r.resposta });
      } catch(err:any){ ga4={ ok:false, status:0, erro:String(err?.message||err) }; try{ capiLog({status:'erro',fonte,evento:'[GA4] '+eventName,pixel_id:cfg.ga4_mid,erro:String(err?.message||err)}); }catch{} }
    }
    return { meta, ga4 };
  };

  // --- parse dos webhooks por fonte (campos mais comuns; tolerante) ---
  const capiParse = (fonte: string, p: any): { evento: string; d: CapiDados } => {
    const g = (o: any, ...ks: string[]): any => { for (const k of ks) { if (o && o[k] != null) return o[k]; } return undefined; };
    const reaisParaCentavos = (v: any): number | null => { const n = Number(v); return isNaN(n) ? null : Math.round(n * 100); };
    let email: any, telefone: any, valor: any, produto: any, pagina: any, evento = 'Purchase';
    try {
      if (fonte === 'hotmart') {
        const buyer = g(p?.data, 'buyer') || p?.buyer || {};
        const purchase = g(p?.data, 'purchase') || p?.purchase || {};
        const prod = g(p?.data, 'product') || p?.product || {};
        email = g(buyer, 'email'); telefone = g(buyer, 'checkout_phone', 'phone');
        valor = g(purchase?.price, 'value') ?? g(purchase, 'full_price', 'price');
        produto = g(prod, 'name'); pagina = g(purchase, 'origin', 'checkout_country');
        const status = String(g(purchase, 'status') || '').toUpperCase();
        evento = status.includes('APPROVED') || status.includes('COMPLETE') ? 'Purchase' : 'InitiateCheckout';
      } else if (fonte === 'kiwify') {
        const c = p?.Customer || p?.customer || {};
        email = g(c, 'email'); telefone = g(c, 'mobile', 'phone');
        valor = g(p, 'order_amount') ?? g(p?.Commissions, 'charge_amount');
        if (valor && Number(valor) > 1000) valor = Number(valor) / 100; // kiwify às vezes manda centavos
        produto = g(p?.Product, 'product_name') || g(p, 'product_name');
        const st = String(g(p, 'order_status') || '').toLowerCase();
        evento = st.includes('paid') || st.includes('approved') ? 'Purchase' : 'InitiateCheckout';
      } else if (fonte === 'guru') {
        const c = p?.contact || p?.customer || {};
        email = g(c, 'email'); telefone = g(c, 'phone_number', 'phone');
        valor = g(p, 'payment')?.total ?? g(p, 'value', 'total');
        produto = g(p, 'product')?.name || g(p, 'product_name');
        const st = String(g(p, 'status') || '').toLowerCase();
        evento = st.includes('approved') || st.includes('paid') ? 'Purchase' : 'InitiateCheckout';
      } else { // lastlink / genérico
        email = g(p, 'email') || g(p?.Buyer, 'Email') || g(p?.data, 'email');
        telefone = g(p, 'phone', 'telefone');
        valor = g(p, 'value', 'amount', 'total', 'price');
        produto = g(p, 'product', 'product_name', 'produto');
      }
    } catch { /* tolerante */ }
    return { evento, d: { email, telefone, valorCentavos: reaisParaCentavos(valor), produto: produto ? String(produto) : null, pagina: pagina ? String(pagina) : null } };
  };

  // --- processa 1 evento (envia se ligado; sempre loga) ---
  const capiProcessar = async (fonte: string, pix: CapiPixel | null, eventName: string, d: CapiDados): Promise<void> => {
    const ligado = pubFlag('capi_ativo', '0') === '1';
    if (!pix) { capiLog({ status: 'erro', fonte, evento: eventName, valor_centavos: d.valorCentavos, pagina: d.pagina, produto: d.produto, erro: 'nenhum pixel configurado' }); return; }
    if (!ligado) { capiLog({ status: 'inerte', fonte, evento: eventName, pixel_id: pix.pixel_id, valor_centavos: d.valorCentavos, pagina: d.pagina, produto: d.produto, resposta: 'capi_ativo=0 (não enviado)' }); return; }
    try {
      const r = await capiEnviar(pix, eventName, d);
      capiLog({ status: r.ok ? 'ok' : 'erro', fonte, evento: eventName, pixel_id: pix.pixel_id, valor_centavos: d.valorCentavos, pagina: d.pagina, produto: d.produto, resposta: r.resposta, erro: r.ok ? null : r.resposta });
    } catch (err: any) {
      capiLog({ status: 'erro', fonte, evento: eventName, pixel_id: pix.pixel_id, valor_centavos: d.valorCentavos, pagina: d.pagina, produto: d.produto, erro: String(err?.message || err) });
    }
  };

  // [etapa31] liga o gancho do CAPI ao webhook de entrega (/webhook/payment/:provider).
  // Recebe os dados JÁ normalizados do pagamento (produtoNome/valor em centavos) e escolhe
  // o pixel por produto (igual ao /capi/webhook). Assim a venda que já dispara a entrega
  // também vai pro pixel Meta, sem criar webhook novo na Guru/LastLink.
  capiHookPagamento = async (fonte, dados) => {
    const eventName = dados.evento || 'Purchase';
    // [#5 rastreio] Se houver config em track_config pro projeto, usa o servico /track
    // (Meta CAPI + GA4 juntos) e NAO cai no fluxo antigo de capi_pixels (evita envio duplo).
    try {
      const projT = projetoDoProduto(dados.produtoId || undefined, dados.produto || undefined);
      if (projT) {
        const cfgT = trackLer(String(projT));
        if (cfgT && cfgT.ativo !== 0 && (cfgT.meta_pixel_id || cfgT.ga4_mid)) {
          await trackEnviarTudo(cfgT, eventName, { email: dados.email, telefone: dados.telefone, valorCentavos: dados.valorCentavos, produto: dados.produto, pagina: null, externalId: dados.email || dados.telefone || null }, fonte);
          return;
        }
      }
    } catch { /* se o track falhar, cai no fluxo antigo abaixo */ }
    // [etapa32] Roteia por INSTRUMENTO reusando projetoDoProduto (palavra-chave baixo/violao/teclado):
    // robusto p/ QUALQUER nome de produto, sem cadastrar nome por nome. Os vínculos têm
    // id = 'teclado'/'baixo'/'violao', que batem com o retorno de projetoDoProduto.
    let pix: CapiPixel | null = null;
    try {
      const proj = projetoDoProduto(dados.produtoId || undefined, dados.produto || undefined);
      if (proj) pix = capiPixelLer(String(proj));
    } catch { /* se o roteador falhar, cai nos fallbacks abaixo */ }
    // Fallback 1: match EXATO por nome (caso algum vínculo tenha o nome cheio do produto).
    if (!pix && dados.produto) {
      const porProd: any = db.prepare('SELECT id FROM capi_pixels WHERE lower(produto)=lower(?) LIMIT 1').get(dados.produto);
      if (porProd) pix = capiPixelLer(porProd.id);
    }
    // Fallback 2: padrão (linha mais antiga). Com os 3 vínculos de instrumento, quase nunca usado.
    if (!pix) pix = capiPixelPadrao();
    await capiProcessar(fonte, pix, eventName, {
      email: dados.email, telefone: dados.telefone,
      valorCentavos: dados.valorCentavos, produto: dados.produto, pagina: null,
    });
  };

  // [#9 VSL camada2 batch B] marco do player de VSL -> Meta CAPI + GA4 (reusa track_config do projeto).
  // Só dispara quando há config ativa pro projeto; valor/produto ficam nulos (é topo/meio de funil).
  vslTrackHook = async (projeto, eventName, dados) => {
    try {
      const cfgT = trackLer(String(projeto || ''));
      if (cfgT && cfgT.ativo !== 0 && (cfgT.meta_pixel_id || cfgT.ga4_mid)) {
        await trackEnviarTudo(cfgT, eventName, { email: dados.email || null, telefone: dados.telefone || null, valorCentavos: null, produto: null, pagina: dados.pagina || null }, 'vsl');
      }
    } catch { /* best-effort: nunca quebra o ingest do player */ }
  };

  // ---------- ROTAS ----------
  // Config do pixel (pixel_id + token cifrado). id = produto/canal (ex: 'teclado').
  app.post('/capi/pixel', (req: any, res: any) => {
    const { id, produto, pixel_id, access_token, test_event_code, conta } = req.body || {};
    if (!id || !pixel_id || !access_token) return res.status(400).json({ erro: 'id, pixel_id e access_token obrigatórios' });
    db.prepare(`INSERT INTO capi_pixels (id, produto, pixel_id, access_token_enc, test_event_code, conta, criado_em)
      VALUES (@id,@produto,@pixel_id,@tok,@tec,@conta,@cri)
      ON CONFLICT(id) DO UPDATE SET produto=@produto, pixel_id=@pixel_id, access_token_enc=@tok, test_event_code=@tec, conta=@conta`).run({
      id, produto: produto || null, pixel_id, tok: cifra(String(access_token)), tec: test_event_code || null, conta: conta || null, cri: agora()
    });
    res.json({ ok: true });
  });
  app.get('/capi/pixels', (_req: any, res: any) => {
    const rs: any[] = db.prepare('SELECT id, produto, pixel_id, test_event_code, conta, criado_em FROM capi_pixels').all();
    res.json({ pixels: rs });
  });

  // Webhook público de venda — Hotmart/Kiwify/Guru/LastLink chamam aqui. SEM x-api-key.
  app.post('/capi/webhook/:fonte', async (req: any, res: any) => {
    const fonte = String(req.params.fonte || 'generico').toLowerCase();
    try {
      const { evento, d } = capiParse(fonte, req.body || {});
      // escolhe pixel: por produto (se casar) senão o padrão
      let pix = capiPixelPadrao();
      if (d.produto) {
        const porProd: any = db.prepare('SELECT id FROM capi_pixels WHERE lower(produto)=lower(?) LIMIT 1').get(d.produto);
        if (porProd) pix = capiPixelLer(porProd.id) || pix;
      }
      await capiProcessar(fonte, pix, evento, d);
      res.json({ ok: true }); // sempre 200 pro gateway não ficar re-tentando
    } catch (err: any) {
      capiLog({ status: 'erro', fonte, evento: 'Purchase', erro: String(err?.message || err) });
      res.json({ ok: true });
    }
  });

  // Log de eventos (pra aba "Eventos do pixel")
  app.get('/capi/eventos', (req: any, res: any) => {
    const lim = Math.min(Number(req.query.limite || 200), 1000);
    const rs: any[] = db.prepare('SELECT * FROM capi_eventos ORDER BY ts DESC LIMIT ?').all(lim);
    const ok = db.prepare("SELECT COUNT(*) n FROM capi_eventos WHERE status='ok'").get() as any;
    const er = db.prepare("SELECT COUNT(*) n FROM capi_eventos WHERE status='erro'").get() as any;
    res.json({ eventos: rs, totalOk: ok?.n || 0, totalErro: er?.n || 0 });
  });

  // Teste: envia 1 evento agora (respeita capi_ativo; se desligado, grava 'inerte')
  app.post('/capi/testar', async (req: any, res: any) => {
    const { id, email, valor, produto } = req.body || {};
    const pix = id ? capiPixelLer(String(id)) : capiPixelPadrao();
    await capiProcessar('teste', pix, 'Purchase', {
      email: email || 'teste@melodiaseriffs.com.br',
      valorCentavos: valor != null ? Math.round(Number(valor) * 100) : 9700,
      produto: produto || 'Teste CAPI', pagina: PUBLIC_BASE_URL
    });
    res.json({ ok: true, enviado: pubFlag('capi_ativo', '0') === '1' });
  });

  // Controle
  app.get('/capi/status', (_req: any, res: any) => res.json({ ativo: pubFlag('capi_ativo', '0') === '1', pixels: (db.prepare('SELECT COUNT(*) n FROM capi_pixels').get() as any)?.n || 0 }));
  app.post('/capi/ligar', (_req: any, res: any) => { setPubFlag('capi_ativo', '1'); res.json({ ok: true, ativo: true }); });
  app.post('/capi/desligar', (_req: any, res: any) => { setPubFlag('capi_ativo', '0'); res.json({ ok: true, ativo: false }); });

  // ---------- ROTAS /track/* (#5 rastreio: Meta CAPI + Google GA4) ----------
  app.get('/track/config', (req:any, res:any) => {
    const projeto = String(req.query.projeto || 'teclado');
    const r:any = db.prepare('SELECT projeto, meta_pixel_id, ga4_mid, test_code, dominio, ativo, (meta_token_enc IS NOT NULL) AS temMetaToken, (ga4_secret_enc IS NOT NULL) AS temGa4Secret FROM track_config WHERE projeto = ?').get(projeto);
    const config = r ? { meta_pixel_id:r.meta_pixel_id||'', ga4_mid:r.ga4_mid||'', test_code:r.test_code||'', dominio:r.dominio||'', ativo: r.ativo==null?1:r.ativo, temMetaToken: !!r.temMetaToken, temGa4Secret: !!r.temGa4Secret } : { meta_pixel_id:'', ga4_mid:'', test_code:'', dominio:'', ativo:1, temMetaToken:false, temGa4Secret:false };
    res.json({ ok:true, config });
  });
  app.post('/track/config', (req:any, res:any) => {
    const b = req.body || {};
    const projeto = String(b.projeto || '').trim();
    if (!projeto) return res.status(400).json({ erro: 'projeto obrigatorio' });
    const encMeta = (b.meta_token && String(b.meta_token).trim()) ? cifra(String(b.meta_token).trim()) : null;
    const encGa4  = (b.ga4_secret && String(b.ga4_secret).trim()) ? cifra(String(b.ga4_secret).trim()) : null;
    db.prepare('INSERT INTO track_config (projeto, meta_pixel_id, meta_token_enc, ga4_mid, ga4_secret_enc, test_code, dominio, ativo, criado_em) VALUES (@projeto,@mpid,@mtok,@gmid,@gsec,@tc,@dom,@ativo,@cri) ON CONFLICT(projeto) DO UPDATE SET meta_pixel_id=@mpid, meta_token_enc=COALESCE(@mtok, meta_token_enc), ga4_mid=@gmid, ga4_secret_enc=COALESCE(@gsec, ga4_secret_enc), test_code=@tc, dominio=@dom, ativo=@ativo').run({
      projeto, mpid: (b.meta_pixel_id||'').trim()||null, mtok: encMeta, gmid: (b.ga4_mid||'').trim()||null, gsec: encGa4,
      tc: (b.test_code||'').trim()||null, dom: (b.dominio||'').trim()||null, ativo: (b.ativo===0?0:1), cri: agora()
    });
    res.json({ ok:true });
  });
  app.post('/track/testar', async (req:any, res:any) => {
    const projeto = String((req.body||{}).projeto || 'teclado');
    const cfg = trackLer(projeto);
    if (!cfg || (!cfg.meta_pixel_id && !cfg.ga4_mid)) return res.json({ ok:false, erro:'Configure o Pixel do Meta e/ou o GA4 deste projeto antes de testar.' });
    const d:CapiDados = { email:'teste@melodiaseriffs.com.br', telefone:'5516999999999', valorCentavos: 9700, produto: 'Teste de rastreio ('+projeto+')', pagina: (cfg.dominio? ('https://'+cfg.dominio) : PUBLIC_BASE_URL), eventId: randomUUID() };
    const r = await trackEnviarTudo(cfg, 'Purchase', d, 'teste');
    let dica = '';
    if (!cfg.meta_pixel_id) dica = 'Sem Pixel do Meta configurado (so GA4).';
    else if (!cfg.meta_token && !capiTokenCentral()) dica = 'Falta o Token do CAPI (Meta) — cole o token pra o Meta receber.';
    else if (cfg.test_code) dica = 'Enviado com Test Event Code — confira em Eventos de teste no Gerenciador de Eventos.';
    res.json({ ok:true, meta: r.meta, ga4: r.ga4, dica });
  });
  app.get('/track/alerta-config', (_req:any, res:any) => res.json({ ok:true, whatsapp: pubFlag('track_alerta_wpp','') }));
  app.post('/track/alerta-config', (req:any, res:any) => { setPubFlag('track_alerta_wpp', String((req.body||{}).whatsapp || '').trim()); res.json({ ok:true }); });
  app.get('/track/snippet.js', (req:any, res:any) => {
    const projeto = String(req.query.projeto || 'teclado');
    const cfg = trackLer(projeto);
    res.type('application/javascript');
    if (!cfg || (!cfg.meta_pixel_id && !cfg.ga4_mid)) { res.send('/* track: projeto '+projeto+' sem config */'); return; }
    const metaPixel = cfg.meta_pixel_id || '';
    const ga4Mid = cfg.ga4_mid || '';
    const base = PUBLIC_BASE_URL || ('https://'+(cfg.dominio||'motor.melodiaseriffs.com.br'));
    const js = '(function(){\n'
      + 'function setCk(n,v){try{document.cookie=n+"="+v+";path=/;max-age=7776000;SameSite=Lax";}catch(e){}}\n'
      + 'function getParam(n){try{return new URLSearchParams(location.search).get(n)||"";}catch(e){return "";}}\n'
      + 'var fbclid=getParam("fbclid"),gclid=getParam("gclid");\n'
      + 'if(fbclid){setCk("_mr_fbc","fb.1."+Date.now()+"."+fbclid);} if(gclid){setCk("_mr_gclid",gclid);}\n'
      + 'function uuid(){return "xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx".replace(/[xy]/g,function(c){var r=Math.random()*16|0,v=c==="x"?r:(r&0x3|0x8);return v.toString(16);});}\n'
      + 'var MR_PROJ=' + JSON.stringify(projeto) + ',MR_BASE=' + JSON.stringify(base) + ';\n'
      + (metaPixel ? ('!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version="2.0";n.queue=[];t=b.createElement(e);t.async=!0;t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}(window,document,"script","https://connect.facebook.net/en_US/fbevents.js");fbq("init",' + JSON.stringify(metaPixel) + ');\n') : '')
      + (ga4Mid ? ('var _gs=document.createElement("script");_gs.async=1;_gs.src="https://www.googletagmanager.com/gtag/js?id="+' + JSON.stringify(ga4Mid) + ';document.head.appendChild(_gs);window.dataLayer=window.dataLayer||[];window.gtag=function(){dataLayer.push(arguments)};gtag("js",new Date());gtag("config",' + JSON.stringify(ga4Mid) + ');\n') : '')
      + 'window.mrTrack=function(evt,params){params=params||{};var eid=uuid();\n'
      + 'try{if(window.fbq)fbq("track",evt,params,{eventID:eid});}catch(e){}\n'
      + 'try{if(window.gtag){var m={Purchase:"purchase",Lead:"generate_lead",InitiateCheckout:"begin_checkout",ViewContent:"view_item"};gtag("event",(m[evt]||String(evt).toLowerCase()),params);}}catch(e){}\n'
      + 'try{navigator.sendBeacon(MR_BASE+"/track/evento",new Blob([JSON.stringify({projeto:MR_PROJ,evento:evt,event_id:eid,url:location.href,params:params})],{type:"application/json"}));}catch(e){}\n'
      + '};\n'
      + 'window.mrTrack("PageView");\n'
      + '})();';
    res.send(js);
  });
  app.post('/track/evento', async (req:any, res:any) => {
    try {
      const b = req.body || {}; const projeto = String(b.projeto||'teclado'); const evento = String(b.evento||'PageView');
      const cfg = trackLer(projeto);
      if (cfg && cfg.ativo !== 0 && (cfg.meta_pixel_id || cfg.ga4_mid) && evento !== 'PageView') {
        const p = b.params || {};
        // [#5 rastreio EMQ] fbc/fbp dos cookies + IP + user-agent do próprio request (sobe o match quality)
        const ck = String(req.headers.cookie || '');
        const lerCk = (n: string): string => { const m = ck.match(new RegExp('(?:^|;\\s*)' + n + '=([^;]+)')); return m ? decodeURIComponent(m[1]) : ''; };
        const fbc = lerCk('_fbc') || lerCk('_mr_fbc') || (p.fbc || '');
        const fbp = lerCk('_fbp') || (p.fbp || '');
        const ip = String((req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.ip || req.socket?.remoteAddress || '');
        const ua = String(req.headers['user-agent'] || '');
        const d:CapiDados = { email: p.email||null, telefone: p.telefone||p.phone||null, valorCentavos: (p.value!=null? Math.round(Number(p.value)*100): null), produto: p.content_name||p.item_name||null, pagina: b.url||null, eventId: b.event_id||undefined,
          fbc: fbc||null, fbp: fbp||null, clientIp: ip||null, userAgent: ua||null,
          firstName: p.first_name||p.fn||null, lastName: p.last_name||p.ln||null, city: p.city||p.ct||null, state: p.state||p.st||null, zip: p.zip||p.zp||null, country: p.country||null, externalId: p.external_id||p.email||null };
        await trackEnviarTudo(cfg, evento, d, 'navegador');
      }
    } catch { /* best-effort */ }
    res.json({ ok:true });
  });
  console.log('[#5 rastreio] /track/* carregado — config (Meta CAPI + GA4), snippet.js, testar, evento, alerta');
  console.log('[etapa29] CAPI carregado (INERTE ate capi_ativo=1) — /capi/webhook/:fonte, /capi/pixel, /capi/eventos, /capi/testar');
}

{
  // ===== [etapa30] Importar do Meta (lista contas+pixels) + TOKEN CENTRAL (1 token p/ todos) =====
  const capiCentralTok = (): string => {
    const r: any = db.prepare("SELECT access_token_enc FROM capi_pixels WHERE id = '__central__'").get();
    if (!r) return '';
    try { return decifra(r.access_token_enc); } catch { return ''; }
  };

  // Salva o token central (System User), cifrado, em capi_pixels id='__central__'.
  app.post('/capi/token-central', (req: any, res: any) => {
    const { token } = req.body || {};
    if (!token) return res.status(400).json({ erro: 'token obrigatorio' });
    db.prepare(`INSERT INTO capi_pixels (id, produto, pixel_id, access_token_enc, test_event_code, conta, criado_em)
      VALUES ('__central__','(token central)','__central__',@tok,NULL,NULL,@cri)
      ON CONFLICT(id) DO UPDATE SET access_token_enc=@tok`).run({ tok: cifra(String(token)), cri: agora() });
    res.json({ ok: true });
  });
  app.get('/capi/token-central/status', (_req: any, res: any) => { res.json({ tem: !!capiCentralTok() }); });

  // Lista TODAS as contas de anuncio + pixels do Meta usando o token central.
  app.get('/capi/importar', async (_req: any, res: any) => {
    const tok = capiCentralTok();
    if (!tok) return res.status(400).json({ erro: 'sem_token_central' });
    try {
      const gfetch = async (u: string): Promise<any> => { const r = await fetch(u); return r.json(); };
      const base = 'https://graph.facebook.com/v21.0';
      const contasResp = await gfetch(`${base}/me/adaccounts?fields=account_id,name&limit=500&access_token=${encodeURIComponent(tok)}`);
      if (contasResp?.error) return res.status(400).json({ erro: 'meta_erro', detalhe: contasResp.error });
      const contas: any[] = (contasResp?.data || []).map((c: any) => ({ id: String(c.account_id), nome: c.name }));
      const pixMap: Record<string, { id: string; nome: string; contas: string[] }> = {};
      for (const c of contas) {
        try {
          const pr = await gfetch(`${base}/act_${c.id}/adspixels?fields=id,name&limit=200&access_token=${encodeURIComponent(tok)}`);
          for (const p of (pr?.data || [])) {
            if (!pixMap[p.id]) pixMap[p.id] = { id: String(p.id), nome: p.name, contas: [] };
            pixMap[p.id].contas.push(c.nome);
          }
        } catch { /* conta sem acesso a pixels: ignora */ }
      }
      res.json({ ok: true, contas, pixels: Object.values(pixMap) });
    } catch (e: any) { res.status(500).json({ erro: String(e?.message || e) }); }
  });

  // Registra varios pixels de uma vez (produto -> pixel_id). Sem token proprio: usam o central.
  app.post('/capi/pixels/bulk', (req: any, res: any) => {
    const lista: any[] = (req.body && req.body.pixels) || [];
    if (!Array.isArray(lista) || !lista.length) return res.status(400).json({ erro: 'pixels_obrigatorio' });
    const vazio = cifra('');
    const stmt = db.prepare(`INSERT INTO capi_pixels (id, produto, pixel_id, access_token_enc, test_event_code, conta, criado_em)
      VALUES (@id,@produto,@pixel_id,@tok,NULL,@conta,@cri)
      ON CONFLICT(id) DO UPDATE SET produto=@produto, pixel_id=@pixel_id, conta=@conta`);
    const tx = db.transaction((rows: any[]) => {
      for (const r of rows) stmt.run({ id: String(r.id || r.produto || r.pixel_id), produto: r.produto || null, pixel_id: String(r.pixel_id), tok: vazio, conta: r.conta || null, cri: agora() });
    });
    tx(lista);
    res.json({ ok: true, salvos: lista.length });
  });

  console.log('[etapa30] CAPI importar carregado — /capi/token-central, /capi/importar, /capi/pixels/bulk');
}


// ============================================================================
// [etapa33] LIVES / TRANSMISSÃO — "ao vivo gravado" no YouTube (FFmpeg + Live API)
// ----------------------------------------------------------------------------
// ADITIVO. Bloco inserido no index.ts do motor (antes do app.listen), igual às
// etapas 28/29/30. Reaproveita helpers existentes: db (better-sqlite3), app
// (express), agora(), randomUUID, pbYtToken(canalId), pbBaixarMidia(ref),
// pbCanalGet/Set, MEDIA_DIR, spawn (node:child_process).
//
// O QUE FAZ:
//  - Agenda um vídeo (do OneDrive ou de uma URL) pra ir "ao vivo" no YouTube no
//    horário marcado, aparecendo como uma live de verdade (simulated live).
//  - Pode mandar pra VÁRIOS canais do YouTube ao mesmo tempo (multistream YT).
//  - Visibilidade por transmissão: private | unlisted | public.
//  - Qualidade: auto (deixa o vídeo mandar) / 720p / 1080p / 4k.
//  - FFmpeg empurra o arquivo em tempo real (-re) com cópia de stream (sem
//    recodificar vídeo = CPU baixa), áudio normalizado pra AAC.
//  - Worker tickLives roda no horário, cria o broadcast no YouTube, baixa o
//    vídeo, transmite, e ao terminar encerra a live (complete).
//
// SEGURANÇA: INERTE por padrão. Enquanto pub_kv 'lives_ativo' != '1' o worker
// NÃO dispara nada sozinho. Ligar = POST /lives/ligar. Dá pra testar 1 por vez
// com POST /lives/testar mesmo desligado.
//
// PRÉ-REQUISITOS no YouTube: o canal precisa estar HABILITADO pra transmitir ao
// vivo (telefone verificado, sem restrições) e conectado via /pub/youtube/connect
// (o token já existe — scope "youtube" cobre liveBroadcasts).
// ============================================================================
{
  // --- tabela (idempotente) ---
  db.exec(`
    CREATE TABLE IF NOT EXISTS fila_lives (
      id TEXT PRIMARY KEY,
      projeto TEXT,
      titulo TEXT,
      descricao TEXT,
      fonte_tipo TEXT,            -- 'onedrive' | 'url'
      fonte_ref TEXT,             -- onedrive_ref OU a URL do vídeo/live
      tipo TEXT DEFAULT 'ao_vivo_gravado',
      qualidade TEXT DEFAULT 'auto',       -- auto | 720p | 1080p | 4k
      privacidade TEXT DEFAULT 'private',  -- private | unlisted | public
      destinos_json TEXT DEFAULT '[]',     -- [{plataforma:'youtube', canal_id:'...'}]
      data_agendada TEXT,
      status TEXT DEFAULT 'agendado',      -- agendado|preparando|no_ar|concluido|erro|cancelado
      broadcasts_json TEXT DEFAULT '[]',   -- por destino (ids, rtmp, watch_url, status)
      em_loop INTEGER DEFAULT 0,
      erro TEXT,
      criado_em TEXT, atualizado_em TEXT
    );
  `);
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN broadcasts_json TEXT DEFAULT '[]'`); } catch { /* já existe */ }
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN capa_ref TEXT`); } catch { /* já existe */ }        // [lives-capa] caminho da capa (imagem) no OneDrive
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN limpo INTEGER DEFAULT 0`); } catch { /* já existe */ } // [lives-limpeza] 1 = vídeo-fonte já apagado do OneDrive
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN tentativas_live INTEGER DEFAULT 0`); } catch { /* já existe */ } // [lives-saude] auto-reinício
  // [lives-chatyt] IA + mensagens programadas no chat AO VIVO do YouTube
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN chat_ia INTEGER DEFAULT 0`); } catch { /* já existe */ }
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN chat_ia_prompt TEXT`); } catch { /* já existe */ }
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN chat_msgs_json TEXT DEFAULT '[]'`); } catch { /* já existe */ } // [{momento:seg, texto}]
  db.exec(`CREATE TABLE IF NOT EXISTS live_chat_estado (
    live_id TEXT, canal_id TEXT, live_chat_id TEXT, next_token TEXT, started_at TEXT, fired_json TEXT DEFAULT '[]', last_poll TEXT,
    PRIMARY KEY (live_id, canal_id)
  );`);
  // [lives-mensagens] sequência de mensagens por live (antecipação individual/grupos, "ao vivo", pós-live) + cortes pós-live
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN mensagens_json TEXT DEFAULT '[]'`); } catch { /* já existe */ }        // [{fase,offset_min,alvo_tipo,tag,grupos,texto}]
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN mensagens_fired_json TEXT DEFAULT '[]'`); } catch { /* já existe */ }
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN auto_cortes INTEGER DEFAULT 0`); } catch { /* já existe */ }
  try { db.exec(`ALTER TABLE fila_lives ADD COLUMN cortes_feitos INTEGER DEFAULT 0`); } catch { /* já existe */ }

  // [multistream-rtmp] destinos RTMP manuais (Facebook e Instagram) — chave de transmissão que você cola.
  // Não usa API do Facebook/Instagram (sem App Review): o motor só empurra o vídeo pro RTMPS do destino.
  db.exec(`
    CREATE TABLE IF NOT EXISTS lives_rtmp_destinos (
      id TEXT PRIMARY KEY,
      plataforma TEXT DEFAULT 'facebook',  -- 'facebook' | 'instagram' | 'tiktok' | 'custom'
      nome TEXT,
      rtmp_base TEXT,        -- ex.: rtmps://live-api-s.facebook.com:443/rtmp/
      stream_key TEXT,       -- a chave de transmissão (segredo)
      page_url TEXT,         -- link da página/perfil (opcional)
      ativo INTEGER DEFAULT 1,
      criado_em TEXT, atualizado_em TEXT
    );
  `);
  const RTMP_BASE_DEFAULT: Record<string, string> = {
    facebook: 'rtmps://live-api-s.facebook.com:443/rtmp/',
    instagram: 'rtmps://live-upload.instagram.com:443/rtmp/',
    tiktok: '',   // o TikTok dá o Servidor (URL) + chave na hora, por sessão — você cola os dois
    custom: '',
  };
  const RTMP_PLATAFORMAS = ['facebook', 'instagram', 'tiktok', 'custom'];
  function rtmpDestLer(id: string): any { return db.prepare(`SELECT * FROM lives_rtmp_destinos WHERE id=?`).get(String(id)) as any; }

  const livLog = (evt: string, ...a: any[]) => { try { console.log(`[etapa33] ${evt}`, ...a); } catch { /* */ } };

  // processos ffmpeg em andamento, por live id -> array de {canal_id, child, done}
  const livesRun: Map<string, Array<{ canal_id: string; child: any; done: boolean }>> = new Map();

  // ---------------------- YouTube Live API (fetch) ----------------------
  const YT = 'https://www.googleapis.com/youtube/v3';
  async function ytApi(token: string, method: string, path: string, body?: any): Promise<any> {
    const r = await fetch(`${YT}${path}`, {
      method,
      headers: { Authorization: `Bearer ${token}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    const txt = await r.text();
    let j: any = {}; try { j = txt ? JSON.parse(txt) : {}; } catch { j = { raw: txt }; }
    if (!r.ok) {
      const msg = j?.error?.message || j?.raw || `HTTP ${r.status}`;
      throw new Error(`YouTube ${method} ${path} falhou (${r.status}): ${String(msg).slice(0, 300)}`);
    }
    return j;
  }

  // cria broadcast + stream, vincula, devolve ingestão RTMP + watch url + liveChatId
  async function ytLiveCriar(canalId: string, opts: { titulo: string; descricao?: string; privacy: string; scheduledStartTime?: string }): Promise<{
    broadcastId: string; streamId: string; rtmp: string; key: string; watchUrl: string; liveChatId: string;
  }> {
    const token = await pbYtTokenLives(canalId);
    const privacy = ['private', 'unlisted', 'public'].includes(opts.privacy) ? opts.privacy : 'private';
    // [lives-link] se veio um horário agendado válido e no futuro, usa ele (pro YouTube mostrar "agendada para X"
    // e o link já valer pra divulgação antecipada); senão começa em ~30s como antes.
    const agendadoMs = opts.scheduledStartTime ? Date.parse(opts.scheduledStartTime) : NaN;
    const inicio = (!isNaN(agendadoMs) && agendadoMs > Date.now() + 15 * 1000)
      ? new Date(agendadoMs).toISOString()
      : new Date(Date.now() + 30 * 1000).toISOString();
    // 1) broadcast (auto-start quando o stream ficar ativo; auto-stop ao parar; DVR liga replay)
    const bc = await ytApi(token, 'POST', '/liveBroadcasts?part=snippet,status,contentDetails', {
      snippet: { title: (opts.titulo || 'Live').slice(0, 100), description: (opts.descricao || '').slice(0, 5000), scheduledStartTime: inicio },
      status: { privacyStatus: privacy, selfDeclaredMadeForKids: false },
      contentDetails: { enableAutoStart: true, enableAutoStop: true, enableDvr: true, latencyPreference: 'normal', enableContentEncryption: false, recordFromStart: true },
    });
    const broadcastId = bc.id;
    const liveChatId = bc?.snippet?.liveChatId || '';
    // 2) stream (resolução/fps variável = aceita 720/1080/4k conforme o vídeo manda)
    const st = await ytApi(token, 'POST', '/liveStreams?part=snippet,cdn,contentDetails', {
      snippet: { title: (opts.titulo || 'Live').slice(0, 100) },
      cdn: { frameRate: 'variable', ingestionType: 'rtmp', resolution: 'variable' },
      contentDetails: { isReusable: false },
    });
    const streamId = st.id;
    const rtmp = st?.cdn?.ingestionInfo?.ingestionAddress || '';
    const key = st?.cdn?.ingestionInfo?.streamName || '';
    // 3) vincula o stream ao broadcast
    await ytApi(token, 'POST', `/liveBroadcasts/bind?id=${encodeURIComponent(broadcastId)}&streamId=${encodeURIComponent(streamId)}&part=id,contentDetails`);
    return { broadcastId, streamId, rtmp, key, watchUrl: `https://youtu.be/${broadcastId}`, liveChatId };
  }

  async function ytLiveTransicao(canalId: string, broadcastId: string, status: string): Promise<void> {
    const token = await pbYtTokenLives(canalId);
    await ytApi(token, 'POST', `/liveBroadcasts/transition?broadcastStatus=${encodeURIComponent(status)}&id=${encodeURIComponent(broadcastId)}&part=status`);
  }

  // [lives-chatyt] lê o chat ao vivo do YouTube (liveChatMessages.list) e posta mensagens (insert).
  async function ytLiveChatList(canalId: string, liveChatId: string, pageToken?: string): Promise<{ msgs: any[]; next: string }> {
    const token = await pbYtTokenLives(canalId);
    const qs = `/liveChat/messages?liveChatId=${encodeURIComponent(liveChatId)}&part=snippet,authorDetails&maxResults=200${pageToken ? ('&pageToken=' + encodeURIComponent(pageToken)) : ''}`;
    const r = await ytApi(token, 'GET', qs);
    const msgs = (r?.items || []).map((it: any) => ({ id: it.id, texto: String(it?.snippet?.displayMessage || ''), autor: String(it?.authorDetails?.displayName || ''), isDono: !!(it?.authorDetails?.isChatOwner), ts: it?.snippet?.publishedAt }));
    return { msgs, next: String(r?.nextPageToken || '') };
  }
  async function ytLiveChatInsert(canalId: string, liveChatId: string, texto: string): Promise<void> {
    const token = await pbYtTokenLives(canalId);
    await ytApi(token, 'POST', `/liveChat/messages?part=snippet`, { snippet: { liveChatId, type: 'textMessageEvent', textMessageDetails: { messageText: String(texto).slice(0, 200) } } });
  }
  // número de espectadores AO VIVO no YouTube (concurrentViewers) — pro contador em tempo real
  async function ytConcurrentViewers(canalId: string, videoId: string): Promise<number> {
    try { const token = await pbYtTokenLives(canalId); const r = await ytApi(token, 'GET', `/videos?part=liveStreamingDetails&id=${encodeURIComponent(videoId)}`); return Number(r?.items?.[0]?.liveStreamingDetails?.concurrentViewers || 0) || 0; } catch { return 0; }
  }

  // [lives-capa] define a CAPA (thumbnail) do vídeo da live. videoId = o próprio broadcastId.
  // imagePath = caminho local da imagem (png/jpg/webp), já baixada do OneDrive. Usa o endpoint de
  // upload de mídia (thumbnails.set). Requisito do YouTube: canal verificado (telefone).
  async function ytThumbnailSet(canalId: string, videoId: string, imagePath: string): Promise<void> {
    const token = await pbYtTokenLives(canalId);
    const buf = fs.readFileSync(imagePath);
    const ext = String(imagePath.split('.').pop() || '').toLowerCase();
    const ct = ext === 'png' ? 'image/png' : (ext === 'webp' ? 'image/webp' : 'image/jpeg');
    const r = await fetch(`https://www.googleapis.com/upload/youtube/v3/thumbnails/set?videoId=${encodeURIComponent(videoId)}&uploadType=media`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': ct },
      body: buf as any,
    });
    if (!r.ok) { const t = await r.text().catch(() => ''); throw new Error(`thumbnail (capa) falhou (${r.status}): ${String(t).slice(0, 300)}`); }
  }

  // ---------------------- FFmpeg push ----------------------
  // Empurra INPUT (arquivo local ou URL) pro RTMP, em tempo real (-re).
  // Vídeo em cópia (sem recodificar = CPU baixa); áudio pra AAC (compatível FLV/RTMP).
  function ffmpegPush(input: string, rtmpFull: string, preInput: string[] = []): any {
    const args = [
      '-hide_banner', '-loglevel', 'warning',
      '-re',
      ...preInput,
      '-i', input,
      '-c:v', 'copy',
      '-c:a', 'aac', '-ar', '44100', '-b:a', '128k',
      '-f', 'flv',
      rtmpFull,
    ];
    const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let errTail = '';
    try { child.stderr.on('data', (d: any) => { errTail = (errTail + String(d)).slice(-1200); }); } catch { /* */ }
    (child as any)._errTail = () => errTail;
    return child;
  }

  // [multistream-rtmp] push pro Facebook/Instagram/TikTok (RTMPS). Essas redes são mais exigentes com
  // keyframe/GOP que o YouTube, então aqui RE-CODIFICA com GOP de ~2s (-g 60) em vez de copiar — garante aceite.
  function ffmpegPushRtmp(input: string, rtmpFull: string, preInput: string[] = []): any {
    const args = [
      '-hide_banner', '-loglevel', 'warning',
      '-re',
      ...preInput,
      '-i', input,
      '-c:v', 'libx264', '-preset', 'veryfast', '-profile:v', 'high', '-pix_fmt', 'yuv420p',
      '-b:v', '4000k', '-maxrate', '4000k', '-bufsize', '8000k',
      '-g', '60', '-keyint_min', '60', '-sc_threshold', '0',
      '-c:a', 'aac', '-ar', '44100', '-b:a', '128k',
      '-f', 'flv',
      rtmpFull,
    ];
    const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let errTail = '';
    try { child.stderr.on('data', (d: any) => { errTail = (errTail + String(d)).slice(-1200); }); } catch { /* */ }
    (child as any)._errTail = () => errTail;
    return child;
  }

  // atualiza um destino dentro de broadcasts_json
  function livSetDestino(liveId: string, canalId: string, patch: Record<string, any>) {
    const row = db.prepare(`SELECT broadcasts_json FROM fila_lives WHERE id=?`).get(liveId) as any;
    let arr: any[] = []; try { arr = JSON.parse(row?.broadcasts_json || '[]'); } catch { arr = []; }
    const i = arr.findIndex((x) => String(x.canal_id) === String(canalId));
    if (i >= 0) arr[i] = { ...arr[i], ...patch }; else arr.push({ canal_id: canalId, ...patch });
    db.prepare(`UPDATE fila_lives SET broadcasts_json=?, atualizado_em=? WHERE id=?`).run(JSON.stringify(arr), agora(), liveId);
  }

  // [lives-link] lê UM destino de dentro de broadcasts_json (pra reaproveitar broadcast pré-criado)
  function livDestinoLer(liveId: string, canalId: string): any | null {
    const row = db.prepare(`SELECT broadcasts_json FROM fila_lives WHERE id=?`).get(liveId) as any;
    let arr: any[] = []; try { arr = JSON.parse(row?.broadcasts_json || '[]'); } catch { arr = []; }
    return arr.find((x) => String(x.canal_id) === String(canalId)) || null;
  }

  // encerra (complete) e marca concluído quando todos os ffmpeg da live terminarem
  function livTalvezConcluir(liveId: string) {
    const procs = livesRun.get(liveId) || [];
    if (procs.length && procs.every((p) => p.done)) {
      livesRun.delete(liveId);
      const row = db.prepare(`SELECT status FROM fila_lives WHERE id=?`).get(liveId) as any;
      if (row && row.status !== 'cancelado') {
        db.prepare(`UPDATE fila_lives SET status='concluido', atualizado_em=? WHERE id=?`).run(agora(), liveId);
        livLog('live concluida', liveId);
      }
    }
  }

  // [lives-religar] duracao do video (ffprobe, em segundo plano, nao trava o motor).
  function livDuracaoSegundos(input: string): Promise<number> {
    return new Promise((resolve) => {
      try {
        const p = spawn('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input], { stdio: ['ignore', 'pipe', 'ignore'] });
        let out = '';
        try { p.stdout.on('data', (d: any) => { out += String(d); }); } catch { /* */ }
        const to = setTimeout(() => { try { p.kill('SIGKILL'); } catch { /* */ } resolve(0); }, 15000);
        p.on('exit', () => { clearTimeout(to); const d = parseFloat(String(out || '').trim()); resolve(isFinite(d) && d > 0 ? d : 0); });
        p.on('error', () => { clearTimeout(to); resolve(0); });
      } catch { resolve(0); }
    });
  }

  // [lives-religar] empurra o ffmpeg com RECONEXAO AUTOMATICA: se a conexao cair com erro ANTES
  // do fim do video, religa sozinho (seek pro ponto onde parou) na MESMA chave/broadcast, ate N vezes.
  // So marca 'erro' de verdade se estourar as tentativas; se o video ja tinha chegado ao fim, marca 'encerrado'.
  function livPushResiliente(o: { liveId: string; canalId: string; rec: any; input: string; rtmpFull: string; preInput: string[]; isRtmp: boolean; durRef: { seg: number }; inicioMs: number; broadcastId?: string; platNome?: string }): any {
    const MAXREL = 10;        // maximo de religamentos por destino na mesma live
    const DELAYMS = 4000;     // espera antes de religar (dentro da janela de tolerancia do YouTube)
    const MARGEMFIM = 20;     // se faltam <=20s pro fim do video, considera que ja terminou (nao religa)
    const fmt = (sg: number): string => { sg = Math.max(0, Math.floor(sg)); const m = Math.floor(sg / 60); const ss = sg % 60; return m + 'm' + (ss < 10 ? '0' : '') + ss + 's'; };
    function subir(seekS: number): any {
      const pre2 = seekS > 2 ? ['-ss', String(Math.floor(seekS)), ...o.preInput] : o.preInput;
      const child = o.isRtmp ? ffmpegPushRtmp(o.input, o.rtmpFull, pre2) : ffmpegPush(o.input, o.rtmpFull, pre2);
      o.rec.child = child;
      child.on('exit', (code: number) => {
        const ok = code === 0;
        const decorridoS = (Date.now() - o.inicioMs) / 1000;
        const dur = Number(o.durRef.seg) || 0;
        const faltamS = dur > 0 ? (dur - decorridoS) : 999999;
        let st = ''; try { st = (db.prepare(`SELECT status FROM fila_lives WHERE id=?`).get(o.liveId) as any)?.status || ''; } catch { /* */ }
        const pararManual = o.rec.parar === true || st === 'cancelado';
        const tent = Number(o.rec.religamentos) || 0;
        if (!ok && !pararManual && faltamS > MARGEMFIM && tent < MAXREL) {
          o.rec.religamentos = tent + 1;
          livSetDestino(o.liveId, o.canalId, { status: 'reconectando', erro: ('conexao caiu - religando ' + (tent + 1) + '/' + MAXREL).slice(0, 400) });
          livLog('religando live', o.liveId, o.canalId, 'tentativa=' + (tent + 1), 'seek=' + Math.floor(decorridoS) + 's');
          try { tgEnviar(`🔁 <b>Live religando sozinha</b>\n${o.platNome || o.canalId}: a conexao caiu, reconectando (${tent + 1}/${MAXREL}) a partir de ${fmt(decorridoS)}. A live continua.`).catch(() => { /* */ }); } catch { /* */ }
          setTimeout(() => { try { subir(decorridoS); } catch { /* */ } }, DELAYMS);
          return;
        }
        o.rec.done = true;
        const terminouBem = ok || faltamS <= MARGEMFIM; // acabou normal OU ja tinha chegado ao fim do video
        livSetDestino(o.liveId, o.canalId, { status: terminouBem ? 'encerrado' : 'erro', erro: terminouBem ? null : ('ffmpeg code ' + code + ': ' + (child._errTail ? child._errTail() : '')).slice(0, 400) });
        if (!o.isRtmp && o.broadcastId) ytLiveTransicao(o.canalId, String(o.broadcastId), 'complete').catch(() => { /* ja pode estar completo */ });
        if (!terminouBem && !pararManual) { try { tgEnviar(`🔴 <b>Live caiu de vez</b>\n${o.platNome || o.canalId}: parou e nao reconectou apos ${MAXREL} tentativas.`).catch(() => { /* */ }); } catch { /* */ } }
        livTalvezConcluir(o.liveId);
      });
      return child;
    }
    return subir(0);
  }

  // dispara UMA live agora (usado pelo worker e pelo /lives/testar)
  async function livExecutar(liveId: string): Promise<void> {
    const live = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(liveId) as any;
    if (!live) throw new Error('live não encontrada');
    if (livesRun.has(liveId)) return; // já rodando
    db.prepare(`UPDATE fila_lives SET status='preparando', erro=NULL, atualizado_em=? WHERE id=?`).run(agora(), liveId);

    // 1) resolve a fonte do vídeo (OneDrive -> baixa local; URL -> usa direto)
    let input = '';
    let inputPre: string[] = [];
    try {
      // [lives-fonte-grande] local-se-pronto > direto do OneDrive > espera download (sem limite de 3min)
      const fr = await livResolverInput(String(live.fonte_tipo), String(live.fonte_ref || ''));
      input = fr.input; inputPre = fr.preInput || [];
      if (!input) throw new Error('fonte do vídeo vazia');
      livLog('fonte da live', liveId, 'modo=' + fr.modo);
    } catch (e: any) {
      db.prepare(`UPDATE fila_lives SET status='erro', erro=?, atualizado_em=? WHERE id=?`).run('fonte: ' + String(e?.message || e), agora(), liveId);
      throw e;
    }

    // [lives-religar] duracao do video em segundo plano (pra decidir, se cair, se foi no MEIO ou ja no FIM) + marco de inicio
    const livDurRef = { seg: 0 };
    livDuracaoSegundos(input).then((d) => { livDurRef.seg = d; }).catch(() => { /* */ });
    const livInicioMs = Date.now();

    // 2) para cada destino YouTube: cria broadcast + empurra ffmpeg
    let destinos: any[] = []; try { destinos = JSON.parse(live.destinos_json || '[]'); } catch { destinos = []; }
    const yts = destinos.filter((d) => String(d.plataforma || 'youtube') === 'youtube' && d.canal_id);
    // [multistream-rtmp] destinos RTMP manuais (Facebook / Instagram / TikTok)
    const rtmps = destinos.filter((d) => RTMP_PLATAFORMAS.includes(String(d.plataforma)) && (d.rtmp_id || d.canal_id));
    if (!yts.length && !rtmps.length) {
      db.prepare(`UPDATE fila_lives SET status='erro', erro=?, atualizado_em=? WHERE id=?`).run('sem destino', agora(), liveId);
      throw new Error('sem destino');
    }

    const procs: Array<{ canal_id: string; child: any; done: boolean }> = [];
    livesRun.set(liveId, procs);
    let algumOk = false;

    for (const d of yts) {
      const canalId = String(d.canal_id);
      try {
        // [lives-link] se a live já foi PREPARADA (broadcast criado antes, pro link antecipado),
        // reaproveita o broadcast/stream existente em vez de criar outro. Senão cria agora (igual antes).
        let info: { broadcastId: string; streamId: string; rtmp: string; key: string; watchUrl: string; liveChatId: string };
        let jaPreparado = false;
        const pre = livDestinoLer(liveId, canalId);
        if (pre && pre.broadcast_id && pre.rtmp && pre.key) {
          info = { broadcastId: String(pre.broadcast_id), streamId: String(pre.stream_id || ''), rtmp: String(pre.rtmp), key: String(pre.key), watchUrl: String(pre.watch_url || ('https://youtu.be/' + pre.broadcast_id)), liveChatId: String(pre.live_chat_id || '') };
          jaPreparado = true;
        } else {
          info = await ytLiveCriar(canalId, { titulo: live.titulo || 'Live', descricao: live.descricao || '', privacy: live.privacidade || 'private', scheduledStartTime: String(live.data_agendada || '') });
        }
        livSetDestino(liveId, canalId, { plataforma: 'youtube', broadcast_id: info.broadcastId, stream_id: info.streamId, rtmp: info.rtmp, key: info.key, watch_url: info.watchUrl, live_chat_id: info.liveChatId, status: 'conectando', erro: null });
        // [lives-capa] se tem capa e o broadcast foi criado AGORA (no preparar já aplicou), sobe a capa
        if (!jaPreparado && live.capa_ref) {
          try { const capaFile = pbBaixarMidia(String(live.capa_ref)).file; await ytThumbnailSet(canalId, info.broadcastId, capaFile); }
          catch (e: any) { livLog('capa falhou', liveId, canalId, String(e?.message || e)); }
        }
        const rec: any = { canal_id: canalId, child: null, done: false, religamentos: 0 };
        procs.push(rec);
        algumOk = true;
        livSetDestino(liveId, canalId, { status: 'no_ar' });
        livLog('empurrando ffmpeg', liveId, canalId, info.watchUrl);
        // [lives-religar] push com reconexao automatica (se a conexao cair no meio, religa sozinho)
        livPushResiliente({ liveId, canalId, rec, input, rtmpFull: `${info.rtmp}/${info.key}`, preInput: inputPre, isRtmp: false, durRef: livDurRef, inicioMs: livInicioMs, broadcastId: info.broadcastId, platNome: 'YouTube ' + canalId });
      } catch (e: any) {
        livSetDestino(liveId, canalId, { plataforma: 'youtube', status: 'erro', erro: String(e?.message || e).slice(0, 400) });
        livLog('falha destino', liveId, canalId, String(e?.message || e));
      }
    }

    // [multistream-rtmp] destinos Facebook / Instagram / TikTok (chave RTMP manual) — re-encode e empurra
    for (const d of rtmps) {
      let rid = String(d.rtmp_id || d.canal_id || '');
      if (rid.startsWith('rtmp:')) rid = rid.slice(5);
      const key = 'rtmp:' + rid;
      try {
        const dd = rtmpDestLer(rid);
        if (!dd || !dd.stream_key) throw new Error('destino sem chave de transmissão');
        const base = String(dd.rtmp_base || RTMP_BASE_DEFAULT[String(dd.plataforma)] || '').replace(/\/+$/, '') + '/';
        if (base === '/') throw new Error('servidor RTMP não informado');
        const full = base + String(dd.stream_key);
        livSetDestino(liveId, key, { plataforma: dd.plataforma || 'facebook', nome: dd.nome || '', page_url: dd.page_url || '', watch_url: dd.page_url || '', status: 'conectando', erro: null });
        const rec: any = { canal_id: key, child: null, done: false, religamentos: 0 };
        procs.push(rec);
        algumOk = true;
        livSetDestino(liveId, key, { status: 'no_ar' });
        livLog('empurrando ffmpeg RTMP', liveId, dd.plataforma, rid);
        const platNome = dd.plataforma === 'instagram' ? 'Instagram' : dd.plataforma === 'tiktok' ? 'TikTok' : dd.plataforma === 'facebook' ? 'Facebook' : String(dd.plataforma || 'destino');
        // [lives-religar] push com reconexao automatica
        livPushResiliente({ liveId, canalId: key, rec, input, rtmpFull: full, preInput: inputPre, isRtmp: true, durRef: livDurRef, inicioMs: livInicioMs, broadcastId: '', platNome: platNome + (dd.nome ? ' (' + String(dd.nome).slice(0, 40) + ')' : '') });
      } catch (e: any) {
        livSetDestino(liveId, key, { plataforma: String(d.plataforma || 'facebook'), status: 'erro', erro: String(e?.message || e).slice(0, 400) });
        livLog('falha destino RTMP', liveId, rid, String(e?.message || e));
      }
    }

    if (algumOk) {
      db.prepare(`UPDATE fila_lives SET status='no_ar', atualizado_em=? WHERE id=?`).run(agora(), liveId);
    } else {
      livesRun.delete(liveId);
      db.prepare(`UPDATE fila_lives SET status='erro', erro=?, atualizado_em=? WHERE id=?`).run('nenhum destino subiu', agora(), liveId);
      throw new Error('nenhum destino subiu');
    }
  }

  // ---------------------- Worker (inerte até lives_ativo=1) ----------------------
  async function tickLives() {
    if (pubFlag('lives_ativo', '0') !== '1') return;
    const agoraIso = agora();
    const pend = db.prepare(`SELECT id FROM fila_lives WHERE status='agendado' AND (data_agendada IS NULL OR data_agendada <= ?) ORDER BY data_agendada ASC LIMIT 3`).all(agoraIso) as any[];
    for (const p of pend) {
      if (livesRun.has(p.id)) continue;
      try { await livExecutar(p.id); } catch (e: any) { livLog('tickLives erro', p.id, String(e?.message || e)); }
    }
  }
  setInterval(() => { tickLives().catch(() => { }); }, 15000);

  // ---------------------- [lives-limpeza] auto-apagar vídeos de live usados do OneDrive ----------------------
  // INERTE até pub_kv 'lives_limpeza_ativo'='1'. Apaga (hard delete) o vídeo-fonte do OneDrive E a cópia local na VPS (/data/media) das lives
  // JÁ concluídas/canceladas há mais de N horas (padrão 48). A GRAVAÇÃO fica salva no YouTube — não se perde.
  // Guarda: não apaga arquivo que ainda esteja em uso por outra live pendente/agendada.
  async function tickLimpezaLives() {
    if (pubFlag('lives_limpeza_ativo', '0') !== '1') return;
    const horas = Math.max(1, Number(pubFlag('lives_limpeza_horas', '48')) || 48);
    const limite = new Date(Date.now() - horas * 3600 * 1000).toISOString();
    const rows = db.prepare(`SELECT id, fonte_ref, capa_ref FROM fila_lives WHERE fonte_tipo='onedrive' AND (limpo IS NULL OR limpo=0) AND status IN ('concluido','cancelado') AND atualizado_em <= ? ORDER BY atualizado_em ASC LIMIT 10`).all(limite) as any[];
    for (const r of rows) {
      const ref = String(r.fonte_ref || '');
      if (!ref) { db.prepare(`UPDATE fila_lives SET limpo=1 WHERE id=?`).run(r.id); continue; }
      const emUso = db.prepare(`SELECT COUNT(*) n FROM fila_lives WHERE fonte_ref=? AND status NOT IN ('concluido','cancelado','erro')`).get(ref) as any;
      if (emUso && emUso.n > 0) continue; // ainda tem live pendente usando esse arquivo -> não apaga
      // [lives-limpeza-local] apaga TAMBEM a copia local na VPS (/data/media) — mesma regra de 48h
      try { const lf = _odDestPath(ref); if (fs.existsSync(lf)) { fs.unlinkSync(lf); livLog('vps local limpo', r.id, lf); } } catch { /* */ }
      try { const cr = String(r.capa_ref || ''); if (cr) { const lc = _odDestPath(cr); if (fs.existsSync(lc)) fs.unlinkSync(lc); } } catch { /* */ }
      try {
        _rclone(['deletefile', _odRemotePath(ref)], { timeoutMs: 60000 });
        db.prepare(`UPDATE fila_lives SET limpo=1, atualizado_em=? WHERE id=?`).run(agora(), r.id);
        livLog('onedrive limpo', r.id, ref);
      } catch (e: any) {
        const msg = String(e?.message || e).toLowerCase();
        livLog('limpeza falhou', r.id, ref, msg);
        if (msg.includes('not found') || msg.includes("didn't find") || msg.includes('no such')) {
          db.prepare(`UPDATE fila_lives SET limpo=1 WHERE id=?`).run(r.id); // arquivo já não existe -> não insistir
        }
      }
    }
  }
  setInterval(() => { tickLimpezaLives().catch(() => { }); }, 30 * 60 * 1000); // checa a cada 30 min
  // endpoints de controle da limpeza
  app.get('/lives/limpeza/status', (_req: any, res: any) => res.json({ ok: true, ativo: pubFlag('lives_limpeza_ativo', '0') === '1', horas: Number(pubFlag('lives_limpeza_horas', '48')) || 48 }));
  app.post('/lives/limpeza/config', (req: any, res: any) => {
    const b = req.body || {};
    if (b.ativo != null) setPubFlag('lives_limpeza_ativo', b.ativo ? '1' : '0');
    if (b.horas != null && Number(b.horas) > 0) setPubFlag('lives_limpeza_horas', String(Math.floor(Number(b.horas))));
    res.json({ ok: true, ativo: pubFlag('lives_limpeza_ativo', '0') === '1', horas: Number(pubFlag('lives_limpeza_horas', '48')) || 48 });
  });

  // [lives-saude] PARTE 6 — auto-reinício de live + aviso Telegram.
  // Caso principal: o motor/container reiniciou no meio de uma live (status fica 'no_ar'
  // no banco mas o ffmpeg não está mais rodando) -> reergue a transmissão e avisa no Telegram.
  // INERTE até pub_kv 'lives_auto_restart'='1'. Até 3 tentativas por live.
  async function tickLivesSaude() {
    if (pubFlag('lives_auto_restart', '0') !== '1') return;
    const rows = db.prepare(`SELECT id,titulo,tentativas_live FROM fila_lives WHERE status='no_ar'`).all() as any[];
    for (const r of rows) {
      if (livesRun.has(r.id)) continue; // ainda rodando de verdade -> ok
      const tent = Number(r.tentativas_live) || 0;
      if (tent >= 3) {
        db.prepare(`UPDATE fila_lives SET status='erro', erro=?, atualizado_em=? WHERE id=?`).run('caiu e não reergueu após 3 tentativas', agora(), r.id);
        tgEnviar(`🔴 <b>Live caiu</b>\n"${String(r.titulo || 'Live').slice(0, 80)}"\nNão consegui reerguer após 3 tentativas. Verifique no app.`).catch(() => { /* */ });
        continue;
      }
      db.prepare(`UPDATE fila_lives SET tentativas_live=?, atualizado_em=? WHERE id=?`).run(tent + 1, agora(), r.id);
      livLog('auto-reinício de live', r.id, 'tentativa', tent + 1);
      tgEnviar(`🟡 <b>Reerguendo live</b>\n"${String(r.titulo || 'Live').slice(0, 80)}" (tentativa ${tent + 1}/3).`).catch(() => { /* */ });
      livExecutar(r.id).catch((e: any) => { livLog('auto-reinício falhou', r.id, String(e?.message || e)); });
    }
  }
  setInterval(() => { tickLivesSaude().catch(() => { }); }, 2 * 60 * 1000); // checa a cada 2 min
  app.get('/lives/auto-restart/status', (_req: any, res: any) => res.json({ ok: true, ativo: pubFlag('lives_auto_restart', '0') === '1' }));
  app.post('/lives/auto-restart/config', (req: any, res: any) => {
    const b = req.body || {}; if (b.ativo != null) setPubFlag('lives_auto_restart', b.ativo ? '1' : '0');
    res.json({ ok: true, ativo: pubFlag('lives_auto_restart', '0') === '1' });
  });

  // [lives-chatyt] configura IA + mensagens programadas do chat do YouTube numa live
  app.post('/lives/chat-config', (req: any, res: any) => {
    try {
      const b = req.body || {}; const id = String(b.id || ''); if (!id) return res.status(400).json({ ok: false, erro: 'id_obrigatorio' });
      const msgs = Array.isArray(b.chat_msgs) ? b.chat_msgs.map((m: any) => ({ momento: Math.max(0, Number(m.momento) || 0), texto: String(m.texto || '').slice(0, 200) })).filter((m: any) => m.texto) : [];
      db.prepare(`UPDATE fila_lives SET chat_ia=?, chat_ia_prompt=?, chat_msgs_json=?, atualizado_em=? WHERE id=?`).run(b.chat_ia ? 1 : 0, String(b.chat_ia_prompt || ''), JSON.stringify(msgs), agora(), id);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  // espectadores AO VIVO por destino (YouTube real; FB/IG/TikTok = pendente API da Meta/TikTok)
  app.get('/lives/:id/viewers', async (req: any, res: any) => {
    try {
      const live = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(String(req.params.id)) as any;
      if (!live) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
      let bc: any[] = []; try { bc = JSON.parse(live.broadcasts_json || '[]'); } catch { bc = []; }
      const out: any[] = []; let total = 0;
      for (const d of bc) {
        if ((d.plataforma === 'youtube' || !d.plataforma) && d.broadcast_id && d.canal_id) {
          const v = await ytConcurrentViewers(String(d.canal_id), String(d.broadcast_id));
          out.push({ plataforma: 'youtube', canal_id: d.canal_id, viewers: v }); total += v;
        } else if (d.plataforma) {
          out.push({ plataforma: d.plataforma, canal_id: d.canal_id || null, viewers: null, pendente: 'api_oficial' }); // FB/IG/TikTok via chave manual não expõe contagem
        }
      }
      res.json({ ok: true, por_destino: out, total_youtube: total });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [lives-chatyt] worker: enquanto a live está no ar, posta as mensagens programadas no horário e
  // (se chat_ia) a IA responde o chat do YouTube — identificada como o canal. Só age em lives configuradas.
  async function tickLiveChat() {
    const lives = db.prepare(`SELECT id,titulo,chat_ia,chat_ia_prompt,chat_msgs_json,broadcasts_json FROM fila_lives WHERE status='no_ar'`).all() as any[];
    for (const live of lives) {
      let msgs: any[] = []; try { msgs = JSON.parse(live.chat_msgs_json || '[]'); } catch { msgs = []; }
      const temIA = !!live.chat_ia && !!LLM_API_KEY;
      if (!temIA && !msgs.length) continue; // nada configurado -> não gasta cota
      let bc: any[] = []; try { bc = JSON.parse(live.broadcasts_json || '[]'); } catch { bc = []; }
      for (const d of bc) {
        const canalId = String(d.canal_id || ''); const liveChatId = String(d.live_chat_id || '');
        if (!canalId || !liveChatId) continue;
        let est = db.prepare(`SELECT * FROM live_chat_estado WHERE live_id=? AND canal_id=?`).get(live.id, canalId) as any;
        if (!est) { db.prepare(`INSERT OR IGNORE INTO live_chat_estado(live_id,canal_id,live_chat_id,next_token,started_at,fired_json,last_poll) VALUES(?,?,?,?,?,?,?)`).run(live.id, canalId, liveChatId, '', agora(), '[]', ''); est = db.prepare(`SELECT * FROM live_chat_estado WHERE live_id=? AND canal_id=?`).get(live.id, canalId) as any; }
        const elapsed = Math.floor((Date.now() - new Date(est.started_at || agora()).getTime()) / 1000);
        let fired: number[] = []; try { fired = JSON.parse(est.fired_json || '[]'); } catch { fired = []; }
        // mensagens programadas
        for (let i = 0; i < msgs.length; i++) {
          if (fired.includes(i)) continue; if (elapsed < (Number(msgs[i].momento) || 0)) continue;
          try { await ytLiveChatInsert(canalId, liveChatId, String(msgs[i].texto)); fired.push(i); } catch { /* */ }
        }
        // IA responde (poll das novas mensagens)
        let nextTok = String(est.next_token || '');
        if (temIA) {
          try {
            const r = await ytLiveChatList(canalId, liveChatId, nextTok || undefined);
            nextTok = r.next || nextTok;
            if (est.next_token) { // só responde a partir do 2º poll (evita responder histórico)
              let respostas = 0;
              for (const m of r.msgs) {
                if (respostas >= 2) break; if (m.isDono) continue; const q = String(m.texto || '').trim(); if (q.length < 3) continue;
                try {
                  const sys = String(live.chat_ia_prompt || '').trim() || 'Você é a equipe da escola de música Melodias e Riffs respondendo o chat ao vivo de uma live no YouTube. Responda em português, muito breve (1 frase), acolhedora e útil, identificada como a escola. Não invente preços nem promoções.';
                  const resp = await _chamarLLM([{ role: 'system', content: sys }, { role: 'user', content: m.autor + ': ' + q }], '', 0.6, []);
                  const txt = String(resp?.content || '').slice(0, 200).trim(); if (txt) { await ytLiveChatInsert(canalId, liveChatId, txt); respostas++; }
                } catch { /* */ }
              }
            }
          } catch { /* */ }
        }
        db.prepare(`UPDATE live_chat_estado SET next_token=?, fired_json=?, last_poll=? WHERE live_id=? AND canal_id=?`).run(nextTok, JSON.stringify(fired), agora(), live.id, canalId);
      }
    }
  }
  setInterval(() => { tickLiveChat().catch(() => { }); }, 15000); // a cada 15s enquanto houver live no ar

  // [lives-mensagens] enfileira uma mensagem pra uma TAG (contatos) e/ou GRUPOS (reusa a fila_envio existente)
  function liveEnfileirarMsg(projeto: string, alvoTipo: string, tag: string, grupos: string[], texto: string): number {
    const alvos: Array<{ para: string; grupo: boolean }> = [];
    if (alvoTipo === 'grupos') { (grupos || []).forEach((g) => { if (g) alvos.push({ para: String(g), grupo: true }); }); }
    else if (tag) {
      const rows = db.prepare(`SELECT DISTINCT c.telefone tel FROM contatos c JOIN contato_tags ct ON ct.contato_id=c.id JOIN tags t ON t.id=ct.tag_id WHERE c.projeto=? AND t.nome=? AND c.telefone IS NOT NULL AND c.telefone<>''`).all(projeto, tag) as any[];
      rows.forEach((r) => alvos.push({ para: String(r.tel), grupo: false }));
    }
    if (!alvos.length) return 0;
    const ins = db.prepare(`INSERT INTO fila_envio(id,projeto,para,is_grupo,texto,agendado_para,criado_em) VALUES(?,?,?,?,?,?,?)`);
    const tx = db.transaction(() => alvos.forEach((a) => ins.run(randomUUID(), projeto, a.para, a.grupo ? 1 : 0, texto, agora(), agora())));
    tx(); return alvos.length;
  }
  // worker: dispara a sequência de mensagens por live (antecipação / ao vivo / pós-live)
  async function tickLiveMensagens() {
    const lives = db.prepare(`SELECT id,projeto,status,data_agendada,atualizado_em,mensagens_json,mensagens_fired_json FROM fila_lives WHERE status IN ('agendado','no_ar','concluido')`).all() as any[];
    for (const live of lives) {
      let msgs: any[] = []; try { msgs = JSON.parse(live.mensagens_json || '[]'); } catch { msgs = []; }
      if (!msgs.length) continue;
      let fired: number[] = []; try { fired = JSON.parse(live.mensagens_fired_json || '[]'); } catch { fired = []; }
      const now = Date.now(); const start = live.data_agendada ? new Date(live.data_agendada).getTime() : 0; let changed = false;
      for (let i = 0; i < msgs.length; i++) {
        if (fired.includes(i)) continue; const m = msgs[i]; const off = (Number(m.offset_min) || 0) * 60000; let due = false;
        if (m.fase === 'antecipacao') due = start > 0 && now >= (start - off);
        else if (m.fase === 'ao_vivo') due = live.status === 'no_ar';
        else if (m.fase === 'pos') due = live.status === 'concluido' && now >= (new Date(live.atualizado_em || agora()).getTime() + off);
        if (!due) continue;
        const grupos = Array.isArray(m.grupos) ? m.grupos : String(m.grupos || '').split(/[,\n;]+/).map((s: string) => s.trim()).filter(Boolean);
        try { liveEnfileirarMsg(String(live.projeto || ''), String(m.alvo_tipo || 'tag'), String(m.tag || ''), grupos, String(m.texto || '')); } catch { /* */ }
        fired.push(i); changed = true;
      }
      if (changed) db.prepare(`UPDATE fila_lives SET mensagens_fired_json=? WHERE id=?`).run(JSON.stringify(fired), live.id);
    }
  }
  setInterval(() => { tickLiveMensagens().catch(() => { }); }, 60 * 1000);
  app.post('/lives/mensagens-config', (req: any, res: any) => {
    try {
      const b = req.body || {}; const id = String(b.id || ''); if (!id) return res.status(400).json({ ok: false, erro: 'id_obrigatorio' });
      const msgs = Array.isArray(b.mensagens) ? b.mensagens.map((m: any) => ({ fase: ['antecipacao', 'ao_vivo', 'pos'].includes(String(m.fase)) ? String(m.fase) : 'antecipacao', offset_min: Number(m.offset_min) || 0, alvo_tipo: m.alvo_tipo === 'grupos' ? 'grupos' : 'tag', tag: String(m.tag || ''), grupos: Array.isArray(m.grupos) ? m.grupos : String(m.grupos || '').split(/[,\n;]+/).map((s: string) => s.trim()).filter(Boolean), texto: String(m.texto || '').slice(0, 1000) })).filter((m: any) => m.texto) : [];
      db.prepare(`UPDATE fila_lives SET mensagens_json=?, auto_cortes=?, atualizado_em=? WHERE id=?`).run(JSON.stringify(msgs), b.auto_cortes ? 1 : 0, agora(), id);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [lives-cortes] worker: quando a live termina, se auto_cortes=1, gera cortes do vídeo-fonte e avisa no Telegram
  async function tickLivesCortes() {
    const lives = db.prepare(`SELECT id,titulo,fonte_tipo,fonte_ref FROM fila_lives WHERE status='concluido' AND auto_cortes=1 AND cortes_feitos=0 AND fonte_ref IS NOT NULL AND fonte_ref<>''`).all() as any[];
    for (const live of lives) {
      db.prepare(`UPDATE fila_lives SET cortes_feitos=1 WHERE id=?`).run(live.id);
      try {
        const base = (PUBLIC_BASE_URL || `http://localhost:${PORT}`).replace(/\/+$/, '');
        await fetch(`${base}/cortes/gerar`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-api-key': ENGINE_API_KEY }, body: JSON.stringify({ fonte_tipo: live.fonte_tipo || 'onedrive', fonte_ref: live.fonte_ref, n: 5 }) });
        tgEnviar(`✂️ <b>Cortes da live</b>\nGerando cortes automáticos de "${String(live.titulo || 'Live').slice(0, 70)}". Aparecem na aba Formatos (Cortes) quando ficarem prontos.`).catch(() => { /* */ });
      } catch { /* */ }
    }
  }
  setInterval(() => { tickLivesCortes().catch(() => { }); }, 2 * 60 * 1000);

  // [lives-chat-unificado] todos os comentários do YouTube (de todos os canais da live) num lugar só + responder
  app.get('/lives/:id/comentarios', async (req: any, res: any) => {
    try {
      const live = db.prepare(`SELECT broadcasts_json FROM fila_lives WHERE id=?`).get(String(req.params.id)) as any;
      if (!live) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
      let bc: any[] = []; try { bc = JSON.parse(live.broadcasts_json || '[]'); } catch { bc = []; }
      const todos: any[] = [];
      for (const d of bc) {
        if ((d.plataforma === 'youtube' || !d.plataforma) && d.canal_id && d.live_chat_id) {
          try { const r = await ytLiveChatList(String(d.canal_id), String(d.live_chat_id)); r.msgs.forEach((m: any) => todos.push({ rede: 'youtube', canal_id: d.canal_id, autor: m.autor, texto: m.texto, ts: m.ts })); } catch { /* */ }
        }
      }
      todos.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
      res.json({ ok: true, comentarios: todos.slice(-80) });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/lives/:id/responder', async (req: any, res: any) => {
    try {
      const texto = String((req.body || {}).texto || '').trim(); if (!texto) return res.json({ ok: false });
      const live = db.prepare(`SELECT broadcasts_json FROM fila_lives WHERE id=?`).get(String(req.params.id)) as any;
      if (!live) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
      let bc: any[] = []; try { bc = JSON.parse(live.broadcasts_json || '[]'); } catch { bc = []; }
      let enviados = 0;
      for (const d of bc) { if ((d.plataforma === 'youtube' || !d.plataforma) && d.canal_id && d.live_chat_id) { try { await ytLiveChatInsert(String(d.canal_id), String(d.live_chat_id), texto); enviados++; } catch { /* */ } } }
      res.json({ ok: true, enviados });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [lives-upload] upload direto da CAPA (imagem pequena) via base64 -> OneDrive. Vídeo grande continua pela pasta/URL.
  app.post('/lives/upload-capa', (req: any, res: any) => {
    try {
      const b = req.body || {}; const pasta = String(b.pasta || '').replace(/^\/+|\/+$/g, ''); let nome = String(b.nome || '').replace(/[^a-zA-Z0-9._ -]/g, '_'); const b64 = String(b.b64 || '');
      if (!pasta || !nome || !b64) return res.status(400).json({ ok: false, erro: 'faltou_dados' });
      if (!/\.(jpe?g|png|webp)$/i.test(nome)) nome += '.jpg';
      const data = b64.includes(',') ? b64.split(',')[1] : b64;
      const buf = Buffer.from(data, 'base64');
      if (buf.length > 8 * 1024 * 1024) return res.status(413).json({ ok: false, erro: 'imagem_grande' });
      const tmp = `${MEDIA_DIR}/up-${randomUUID().slice(0, 8)}-${nome}`;
      fs.writeFileSync(tmp, buf);
      try { _rclone(['copyto', tmp, _odRemotePath(pasta + '/' + nome)], { timeoutMs: 120000 }); }
      finally { try { fs.unlinkSync(tmp); } catch { /* */ } }
      res.json({ ok: true, caminho: pasta + '/' + nome });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [lives-pasta] auto-casamento: lê a pasta Lives do CANAL no OneDrive (Postagem Automatica/<od>/Lives)
  // e devolve o vídeo + a capa já pareados. Com 1 de cada = automático; com vários = devolve a lista pra escolher.
  const LIVES_OD: Record<string, string> = { teclado: 'Melodias e Riffs Teclado', baixo: 'Melodias e Riffs Baixo', violao: 'Melodias e Riffs Violão', comunidade: 'Comunidade da Harpa Cristã' };
  app.get('/lives/pasta-auto', (req: any, res: any) => {
    try {
      const canal = String(req.query.canal || '').toLowerCase().trim();
      const od = LIVES_OD[canal]; if (!od) return res.status(400).json({ ok: false, erro: 'canal_invalido' });
      const sub = od + '/Lives';
      const EXTV = ['mp4', 'mov', 'mkv', 'webm', 'avi', 'm4v', 'flv']; const EXTI = ['png', 'jpg', 'jpeg', 'webp', 'bmp', 'gif'];
      let rows: any[] = []; try { rows = (_odRcloneListar(sub) as any[]) || []; } catch { rows = []; }
      const mapa = rows.filter((r) => !r.IsDir).map((r) => { const rel = String(r.Path || r.Name || ''); const nome = String(r.Name || rel.split('/').pop() || rel); const ext = String(nome.split('.').pop() || '').toLowerCase(); return { nome, caminho: sub + '/' + rel, ext, tamanho: Number(r.Size || 0) }; });
      const videos = mapa.filter((r) => EXTV.includes(r.ext)).sort((a, b) => b.tamanho - a.tamanho);
      const imagens = mapa.filter((r) => EXTI.includes(r.ext)).sort((a, b) => String(a.nome).localeCompare(String(b.nome)));
      res.json({ ok: true, pasta: sub, video_ref: videos.length === 1 ? videos[0].caminho : '', capa_ref: imagens.length === 1 ? imagens[0].caminho : '', videos, imagens, auto: videos.length === 1 });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // ---------------------- Endpoints ----------------------
  // agenda uma transmissão
  app.post('/lives/agendar', (req: any, res: any) => {
    try {
      const b = req.body || {};
      const destinos = Array.isArray(b.destinos) ? b.destinos : [];
      if (!destinos.length) return res.status(400).json({ ok: false, erro: 'destinos_obrigatorio' });
      if (!b.fonte_ref) return res.status(400).json({ ok: false, erro: 'fonte_ref_obrigatorio' });
      // [live-valida-video] trava: a FONTE da live tem que ser VIDEO, nunca imagem.
      //   (bug 20/10: a live do Teclado pegou o .jpg da capa no lugar do .mp4 e encerrou na hora.)
      {
        const _ft = String(b.fonte_tipo || 'onedrive');
        const _fr = String(b.fonte_ref || '');
        const _ehArquivo = ['onedrive', 'arquivo', 'local', 'od', 'rclone'].includes(_ft);
        if (_ehArquivo) {
          const _ext = (_fr.split('?')[0].split('#')[0].split('.').pop() || '').toLowerCase();
          const _VIDEO = ['mp4', 'mov', 'mkv', 'avi', 'webm', 'flv', 'm4v', 'mpg', 'mpeg', 'wmv', 'ts', 'm2ts', '3gp'];
          const _IMG = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic', 'heif', 'tif', 'tiff', 'svg'];
          if (_IMG.includes(_ext)) {
            const _sug = _fr.replace(/\.[^.]+$/, '.mp4');
            return res.status(400).json({ ok: false, erro: 'fonte_e_imagem', mensagem: `O arquivo escolhido como VIDEO da live e uma imagem (.${_ext}), nao um video. Selecione o video (ex.: ${_sug}). A imagem deve ir so no campo de CAPA.` });
          }
          if (_ext && !_VIDEO.includes(_ext)) {
            return res.status(400).json({ ok: false, erro: 'fonte_nao_video', mensagem: `O arquivo escolhido como VIDEO da live (.${_ext}) nao e um formato de video reconhecido. Use um arquivo de video (mp4, mov, mkv, webm, ...).` });
          }
        }
      }
      const id = String(b.id || randomUUID());
      const priv = ['private', 'unlisted', 'public'].includes(String(b.privacidade)) ? String(b.privacidade) : 'private';
      db.prepare(`INSERT INTO fila_lives(id,projeto,titulo,descricao,fonte_tipo,fonte_ref,capa_ref,tipo,qualidade,privacidade,destinos_json,data_agendada,status,broadcasts_json,em_loop,criado_em,atualizado_em)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET projeto=excluded.projeto,titulo=excluded.titulo,descricao=excluded.descricao,fonte_tipo=excluded.fonte_tipo,fonte_ref=excluded.fonte_ref,capa_ref=excluded.capa_ref,qualidade=excluded.qualidade,privacidade=excluded.privacidade,destinos_json=excluded.destinos_json,data_agendada=excluded.data_agendada,atualizado_em=excluded.atualizado_em`)
        .run(id, String(b.projeto || ''), String(b.titulo || 'Live'), String(b.descricao || ''), String(b.fonte_tipo || 'onedrive'), String(b.fonte_ref || ''), String(b.capa_ref || ''), String(b.tipo || 'ao_vivo_gravado'), String(b.qualidade || 'auto'), priv, JSON.stringify(destinos), String(b.data_agendada || agora()), 'agendado', '[]', b.em_loop ? 1 : 0, agora(), agora());
      // [lives-fonte-grande] ja comeca a baixar o video (e capa) em segundo plano, sem limite de tempo
      try { livPreBaixarFonte(String(b.fonte_tipo || 'onedrive'), String(b.fonte_ref || ''), String(b.capa_ref || '')); } catch { /* */ }
      res.json({ ok: true, id });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [lives-link] PREPARA a live: cria o broadcast no YouTube ANTES (com o horário agendado),
  // aplica a capa, e devolve o LINK (watch_url) pra divulgar com antecedência — sem transmitir ainda.
  // Mantém status 'agendado', então o worker/`/lives/testar` depois reaproveita esse broadcast e empurra o vídeo.
  app.post('/lives/preparar', async (req: any, res: any) => {
    try {
      const id = String((req.body || {}).id || '');
      if (!id) return res.status(400).json({ ok: false, erro: 'id_obrigatorio' });
      const live = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(id) as any;
      if (!live) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
      let destinos: any[] = []; try { destinos = JSON.parse(live.destinos_json || '[]'); } catch { destinos = []; }
      const yts = destinos.filter((d) => String(d.plataforma || 'youtube') === 'youtube' && d.canal_id);
      if (!yts.length) return res.status(400).json({ ok: false, erro: 'sem destino YouTube' });
      const links: any[] = [];
      for (const d of yts) {
        const canalId = String(d.canal_id);
        const ja = livDestinoLer(id, canalId);
        if (ja && ja.broadcast_id && ja.rtmp && ja.key) { links.push({ canal_id: canalId, watch_url: ja.watch_url, broadcast_id: ja.broadcast_id, ja: true }); continue; }
        try {
          const info = await ytLiveCriar(canalId, { titulo: live.titulo || 'Live', descricao: live.descricao || '', privacy: live.privacidade || 'private', scheduledStartTime: String(live.data_agendada || '') });
          livSetDestino(id, canalId, { plataforma: 'youtube', broadcast_id: info.broadcastId, stream_id: info.streamId, rtmp: info.rtmp, key: info.key, watch_url: info.watchUrl, live_chat_id: info.liveChatId, status: 'pronto', erro: null });
          if (live.capa_ref) { try { const capaFile = pbBaixarMidia(String(live.capa_ref)).file; await ytThumbnailSet(canalId, info.broadcastId, capaFile); } catch (e: any) { livLog('capa (preparar) falhou', id, canalId, String(e?.message || e)); } }
          links.push({ canal_id: canalId, watch_url: info.watchUrl, broadcast_id: info.broadcastId, ja: false });
        } catch (e: any) {
          livSetDestino(id, canalId, { plataforma: 'youtube', status: 'erro', erro: String(e?.message || e).slice(0, 400) });
          links.push({ canal_id: canalId, erro: String(e?.message || e).slice(0, 300) });
        }
      }
      res.json({ ok: links.some((l) => l.watch_url), id, links });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // lista transmissões (por projeto/status opcional)
  app.get('/lives', (req: any, res: any) => {
    try {
      const cond: string[] = []; const args: any[] = [];
      if (req.query.projeto) { cond.push('projeto=?'); args.push(String(req.query.projeto)); }
      if (req.query.status) { cond.push('status=?'); args.push(String(req.query.status)); }
      const where = cond.length ? ('WHERE ' + cond.join(' AND ')) : '';
      const rows = db.prepare(`SELECT * FROM fila_lives ${where} ORDER BY data_agendada DESC LIMIT 200`).all(...args) as any[];
      res.json({ ok: true, ativo: pubFlag('lives_ativo', '0') === '1', lives: rows });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // cancela (se estiver no ar, encerra os ffmpeg e completa os broadcasts)
  app.post('/lives/cancelar', async (req: any, res: any) => {
    try {
      const id = String((req.body || {}).id || '');
      const row = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(id) as any;
      if (!row) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
      const procs = livesRun.get(id) || [];
      for (const p of procs) { try { (p as any).parar = true; } catch { /* */ } try { p.child.kill('SIGINT'); } catch { /* */ } }
      // completa broadcasts no YouTube
      try {
        const arr = JSON.parse(row.broadcasts_json || '[]');
        for (const d of arr) { if (d.broadcast_id && d.canal_id) { ytLiveTransicao(String(d.canal_id), String(d.broadcast_id), 'complete').catch(() => { /* */ }); } }
      } catch { /* */ }
      livesRun.delete(id);
      db.prepare(`UPDATE fila_lives SET status='cancelado', atualizado_em=? WHERE id=?`).run(agora(), id);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // remove da lista (não pode estar no ar)
  app.post('/lives/remover', (req: any, res: any) => {
    const id = String((req.body || {}).id || '');
    if (livesRun.has(id)) return res.status(409).json({ ok: false, erro: 'no_ar' });
    db.prepare(`DELETE FROM fila_lives WHERE id=?`).run(id);
    res.json({ ok: true });
  });

  // liga/desliga o worker
  app.post('/lives/ligar', (_req: any, res: any) => { setPubFlag('lives_ativo', '1'); res.json({ ok: true, ativo: true }); });
  app.post('/lives/desligar', (_req: any, res: any) => { setPubFlag('lives_ativo', '0'); res.json({ ok: true, ativo: false }); });
  app.get('/lives/_status', (_req: any, res: any) => {
    const porStatus = db.prepare(`SELECT status, COUNT(*) n FROM fila_lives GROUP BY status`).all() as any[];
    res.json({ ok: true, ativo: pubFlag('lives_ativo', '0') === '1', rodando: Array.from(livesRun.keys()), porStatus });
  });

  // dispara UMA agora (teste), mesmo com o worker desligado. body:{id}
  app.post('/lives/testar', async (req: any, res: any) => {
    try {
      const id = String((req.body || {}).id || '');
      if (!id) return res.status(400).json({ ok: false, erro: 'id_obrigatorio' });
      await livExecutar(id);
      const row = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(id) as any;
      res.json({ ok: true, live: row });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // canais do YouTube disponíveis pra escolher como destino
  app.get('/lives/destinos', (_req: any, res: any) => {
    try {
      const canais = db.prepare(`SELECT id,nome,projeto,instrumento,cor,yt_channel_id FROM canais_publicacao WHERE ativo=1`).all() as any[];
      const out = canais.map((c) => {
        let conectado = false; try { conectado = !!pbTokLer(String(c.id), 'youtube'); } catch { conectado = false; }
        return { canal_id: c.id, nome: c.nome, projeto: c.projeto, instrumento: c.instrumento, cor: c.cor, yt_channel_id: c.yt_channel_id, youtube_conectado: conectado };
      }).filter((c) => c.youtube_conectado);
      // [multistream-rtmp] inclui os destinos RTMP manuais (Facebook/Instagram/TikTok) como selecionáveis
      let rtmpOut: any[] = [];
      try {
        const rs = db.prepare(`SELECT id,plataforma,nome,page_url FROM lives_rtmp_destinos WHERE ativo=1 ORDER BY criado_em ASC`).all() as any[];
        rtmpOut = rs.map((r) => ({ canal_id: 'rtmp:' + r.id, rtmp_id: r.id, plataforma: r.plataforma, nome: r.nome, instrumento: '', page_url: r.page_url, youtube_conectado: false }));
      } catch { rtmpOut = []; }
      res.json({ ok: true, destinos: [...out, ...rtmpOut] });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // [multistream-rtmp] gerenciar destinos RTMP manuais (Facebook / Instagram / TikTok)
  app.get('/lives/rtmp-destinos', (_req: any, res: any) => {
    try {
      const rows = db.prepare(`SELECT id,plataforma,nome,rtmp_base,page_url,ativo,criado_em FROM lives_rtmp_destinos ORDER BY criado_em ASC`).all() as any[];
      // NÃO devolve a stream_key (segredo) — só sinaliza que existe
      const out = rows.map((r) => ({ id: r.id, plataforma: r.plataforma, nome: r.nome, rtmp_base: r.rtmp_base, page_url: r.page_url, ativo: r.ativo, tem_chave: true }));
      res.json({ ok: true, destinos: out, rtmp_base_default: RTMP_BASE_DEFAULT });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/lives/rtmp-destinos', (req: any, res: any) => {
    try {
      const b = req.body || {};
      const plataforma = RTMP_PLATAFORMAS.includes(String(b.plataforma)) ? String(b.plataforma) : 'facebook';
      const nome = String(b.nome || '').trim();
      const streamKey = String(b.stream_key || '').trim();
      const rtmpBase = (String(b.rtmp_base || '').trim() || RTMP_BASE_DEFAULT[plataforma] || '');
      const pageUrl = String(b.page_url || '').trim();
      if (!nome) return res.status(400).json({ ok: false, erro: 'nome_obrigatorio' });
      const id = String(b.id || '').trim();
      if (id) {
        const atual = rtmpDestLer(id);
        if (!atual) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
        const key = streamKey || String(atual.stream_key || '');   // sem chave nova = mantém a antiga
        db.prepare(`UPDATE lives_rtmp_destinos SET plataforma=?, nome=?, rtmp_base=?, stream_key=?, page_url=?, atualizado_em=? WHERE id=?`).run(plataforma, nome, rtmpBase, key, pageUrl, agora(), id);
        return res.json({ ok: true, id });
      }
      if (!streamKey) return res.status(400).json({ ok: false, erro: 'chave_obrigatoria' });
      const novoId = randomUUID();
      db.prepare(`INSERT INTO lives_rtmp_destinos(id,plataforma,nome,rtmp_base,stream_key,page_url,ativo,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,1,?,?)`).run(novoId, plataforma, nome, rtmpBase, streamKey, pageUrl, agora(), agora());
      res.json({ ok: true, id: novoId });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/lives/rtmp-destinos/remover', (req: any, res: any) => {
    try { const id = String((req.body || {}).id || ''); db.prepare(`DELETE FROM lives_rtmp_destinos WHERE id=?`).run(id); res.json({ ok: true }); }
    catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // detalhe de UMA transmissão (por último: rota com :id não pode capturar /lives/_status e /lives/destinos)
  app.get('/lives/:id', (req: any, res: any) => {
    const row = db.prepare(`SELECT * FROM fila_lives WHERE id=?`).get(String(req.params.id)) as any;
    if (!row) return res.status(404).json({ ok: false, erro: 'nao_encontrada' });
    res.json({ ok: true, live: row });
  });

  console.log('[etapa33] lives/transmissao carregado (INERTE ate lives_ativo=1): ao-vivo-gravado YouTube + FFmpeg + worker + /lives/*');
}


// WEBINARIO — configuracao da audiencia demonstrativa e mensagens programadas.
// Bloco aditivo e isolado: nao altera o worker de lives nem os webhooks existentes.
db.exec(`CREATE TABLE IF NOT EXISTS webinario_simulacoes (
  projeto TEXT PRIMARY KEY,
  contador_json TEXT NOT NULL DEFAULT '{}',
  comentarios_json TEXT NOT NULL DEFAULT '[]',
  atualizado_em TEXT NOT NULL
);`);

app.get('/webinario/simulacao/:projeto', (req: any, res: any) => {
  try {
    const projeto = String(req.params.projeto || '').trim().slice(0, 80);
    const row = db.prepare(`SELECT contador_json, comentarios_json, atualizado_em FROM webinario_simulacoes WHERE projeto=?`).get(projeto) as any;
    if (!row) return res.json({ ok: true, simulacao: null });
    res.json({ ok: true, simulacao: { config: JSON.parse(row.contador_json || '{}'), comentarios: JSON.parse(row.comentarios_json || '[]'), atualizado_em: row.atualizado_em } });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

app.post('/webinario/simulacao/:projeto', (req: any, res: any) => {
  try {
    const projeto = String(req.params.projeto || '').trim().slice(0, 80);
    if (!projeto) return res.status(400).json({ ok: false, erro: 'projeto_obrigatorio' });
    const b = req.body || {};
    const cfg = b.config || {};
    const minimo = Math.max(0, Math.round(Number(cfg.minimo) || 0));
    const maximo = Math.max(minimo, Math.min(1000000, Math.round(Number(cfg.maximo) || minimo)));
    const config = {
      ativo: !!cfg.ativo,
      minimo,
      maximo,
      inicial: Math.max(minimo, Math.min(maximo, Math.round(Number(cfg.inicial) || minimo))),
      intervaloMin: Math.max(1, Math.min(300, Math.round(Number(cfg.intervaloMin) || 4))),
      intervaloMax: Math.max(1, Math.min(300, Math.round(Number(cfg.intervaloMax) || 10))),
      variacaoMax: Math.max(1, Math.min(1000, Math.round(Number(cfg.variacaoMax) || 3)))
    };
    const entrada = Array.isArray(b.comentarios) ? b.comentarios.slice(0, 20000) : [];
    const comentarios = entrada.map((c: any, i: number) => ({
      id: String(c?.id || randomUUID()).slice(0, 120),
      tempoSeg: Math.max(0, Math.min(86400, Math.round(Number(c?.tempoSeg) || i * 15))),
      nome: String(c?.nome || `Participante ${i + 1}`).trim().slice(0, 160),
      texto: String(c?.texto || '').trim().slice(0, 4000),
      origem: String(c?.origem || 'Manual').trim().slice(0, 80),
      ativo: c?.ativo !== false
    })).filter((c: any) => c.texto);
    const atualizado = agora();
    db.prepare(`INSERT INTO webinario_simulacoes(projeto,contador_json,comentarios_json,atualizado_em) VALUES(?,?,?,?)
      ON CONFLICT(projeto) DO UPDATE SET contador_json=excluded.contador_json,comentarios_json=excluded.comentarios_json,atualizado_em=excluded.atualizado_em`)
      .run(projeto, JSON.stringify(config), JSON.stringify(comentarios), atualizado);
    res.json({ ok: true, total_comentarios: comentarios.length, atualizado_em: atualizado });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

console.log('[webinario] simulacao e mensagens programadas carregadas: /webinario/simulacao/:projeto');

// ============================================================================
// [etapa34] FORMATOS VERTICAIS — gera 1080x1920 a partir do vídeo horizontal
// ----------------------------------------------------------------------------
// ADITIVO. Bloco antes do app.listen. Reaproveita: db, app, agora(), randomUUID,
// spawn, pbBaixarMidia, MEDIA_DIR, MEDIA_PUBLIC_BASE/PUBLIC_BASE_URL.
//
// Pega um vídeo (OneDrive ou URL) e produz a versão vertical conforme o modo:
//   0 = tela cheia (zoom, corta laterais)
//   1 = quadradinho + borda preta
//   2 = quadradinho + texto (cima/baixo)
//   3 = quadradinho + arte PNG (1080x1920, centro vazado) por cima
//   4 = girado (vídeo rotacionado 90°, preenche tudo — assiste deitado)
// Tamanho (modos 1/2/3): pequeno|medio|grande (largura do vídeo no meio).
//
// O resultado é salvo em MEDIA_DIR e servido em /media/<arquivo> (pronto pra
// o Instagram/PC2 pegar ou pra baixar). Recodifica (CPU) — é geração única,
// não ao vivo. Roda async: POST devolve job_id; GET acompanha.
//
// PENDENTE DE DEPLOY + TESTE no VPS (código novo, ainda não rodado).
// ============================================================================
{
  db.exec(`
    CREATE TABLE IF NOT EXISTS formatos_jobs (
      id TEXT PRIMARY KEY,
      modo INTEGER,
      fonte_ref TEXT,
      saida_arquivo TEXT,
      saida_url TEXT,
      status TEXT DEFAULT 'processando',   -- processando | pronto | erro
      erro TEXT,
      criado_em TEXT, atualizado_em TEXT
    );
  `);

  const fvJobs: Map<string, any> = new Map(); // id -> child
  const FV_FONTE = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf';
  const fvLarguras: Record<string, number> = { pequeno: 680, medio: 880, grande: 1080 };

  // escapa texto pro drawtext do ffmpeg
  const fvEscText = (s: string): string => String(s || '').replace(/\\/g, '').replace(/:/g, '\\:').replace(/'/g, '').slice(0, 120);

  // monta os argumentos do ffmpeg conforme o modo
  function fvArgs(input: string, pngPath: string | null, modo: number, opts: any, outPath: string): string[] {
    const larg = fvLarguras[String(opts?.tamanho || 'grande')] || 1080;
    const base = ['-hide_banner', '-loglevel', 'error', '-y', '-i', input];
    const enc = ['-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart', outPath];
    const padQuad = `scale=${larg}:-2,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black`;
    if (modo === 0) {
      return [...base, '-vf', 'scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920', ...enc];
    }
    if (modo === 1) {
      return [...base, '-vf', padQuad, ...enc];
    }
    if (modo === 2) {
      const cima = fvEscText(opts?.texto_cima || '');
      const baixo = fvEscText(opts?.texto_baixo || '');
      let vf = padQuad;
      const dt = (txt: string, y: string) => `,drawtext=fontfile=${FV_FONTE}:text='${txt}':fontcolor=white:fontsize=56:box=1:boxcolor=black@0.45:boxborderw=16:x=(w-text_w)/2:y=${y}`;
      if (cima) vf += dt(cima, '150');
      if (baixo) vf += dt(baixo, 'h-th-180');
      return [...base, '-vf', vf, ...enc];
    }
    if (modo === 3 && pngPath) {
      return ['-hide_banner', '-loglevel', 'error', '-y', '-i', input, '-i', pngPath,
        '-filter_complex', `[0:v]${padQuad}[bg];[bg][1:v]overlay=0:0`, ...enc];
    }
    if (modo === 4) {
      return [...base, '-vf', 'transpose=1', ...enc];
    }
    // fallback = quadradinho
    return [...base, '-vf', padQuad, ...enc];
  }

  function fvResolve(tipo: string, ref: string): string {
    if (String(tipo) === 'url') return String(ref || '');
    return pbBaixarMidia(String(ref || '')).file; // onedrive
  }

  // dispara a geração (async)
  app.post('/formatos/vertical', (req: any, res: any) => {
    try {
      const b = req.body || {};
      const modo = Number(b.modo);
      if (!(modo >= 0 && modo <= 4)) return res.status(400).json({ ok: false, erro: 'modo_invalido (0..4)' });
      if (!b.fonte_ref) return res.status(400).json({ ok: false, erro: 'fonte_ref_obrigatorio' });
      const id = randomUUID();
      const nome = String(b.saida_nome || `vertical-${id.slice(0, 8)}`).replace(/[^a-zA-Z0-9._-]/g, '_') + '.mp4';
      const outPath = `${MEDIA_DIR}/${nome}`;
      let input = '';
      let pngPath: string | null = null;
      try {
        input = fvResolve(String(b.fonte_tipo || 'onedrive'), String(b.fonte_ref));
        if (modo === 3) {
          if (!b.png_ref) return res.status(400).json({ ok: false, erro: 'png_ref_obrigatorio no modo 3' });
          pngPath = pbBaixarMidia(String(b.png_ref)).file;
        }
      } catch (e: any) {
        return res.status(400).json({ ok: false, erro: 'fonte: ' + String(e?.message || e) });
      }
      const base = String(MEDIA_PUBLIC_BASE || PUBLIC_BASE_URL || '').replace(/\/+$/, '');
      db.prepare(`INSERT INTO formatos_jobs(id,modo,fonte_ref,saida_arquivo,saida_url,status,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,?)`)
        .run(id, modo, String(b.fonte_ref), nome, `${base}/media/${nome}`, 'processando', agora(), agora());

      const args = fvArgs(input, pngPath, modo, b.opts || {}, outPath);
      const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
      fvJobs.set(id, child);
      let errTail = '';
      try { child.stderr.on('data', (d: any) => { errTail = (errTail + String(d)).slice(-800); }); } catch { /* */ }
      child.on('exit', (code: number) => {
        fvJobs.delete(id);
        if (code === 0) db.prepare(`UPDATE formatos_jobs SET status='pronto', atualizado_em=? WHERE id=?`).run(agora(), id);
        else db.prepare(`UPDATE formatos_jobs SET status='erro', erro=?, atualizado_em=? WHERE id=?`).run(('ffmpeg code ' + code + ': ' + errTail).slice(0, 500), agora(), id);
      });
      res.json({ ok: true, id, saida_url: `${base}/media/${nome}` });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  app.get('/formatos/vertical/:id', (req: any, res: any) => {
    const row = db.prepare(`SELECT * FROM formatos_jobs WHERE id=?`).get(String(req.params.id)) as any;
    if (!row) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
    res.json({ ok: true, job: row });
  });

  app.get('/formatos/vertical', (_req: any, res: any) => {
    const rows = db.prepare(`SELECT * FROM formatos_jobs ORDER BY criado_em DESC LIMIT 100`).all() as any[];
    res.json({ ok: true, jobs: rows });
  });

  console.log('[etapa34] formatos verticais carregado: POST/GET /formatos/vertical (zoom|borda|texto|PNG|girado)');
}


// ============================================================================
// [webinario-sala] PARTE 4 — Webinário: sala AO VIVO de verdade.
// Aditivo e isolado. NÃO toca no bloco webinario_simulacoes (do GPT) nem nos
// workers de lives/webhooks. Rotas /webinarios (config, com x-api-key) e
// /w/:slug + /w-api/:slug/* (sala pública). Contador REAL (presença), chat REAL,
// popups de venda REAIS (tabela transacoes), ofertas/linha do tempo, chat IA
// opcional (reusa _chamarLLM, identificado como a escola). SEM contador inflado
// nem comentários falsos — essa parte não é construída aqui.
// ============================================================================
{
  db.exec(`
    CREATE TABLE IF NOT EXISTS webinarios (
      id TEXT PRIMARY KEY, slug TEXT UNIQUE, nome TEXT,
      video_tipo TEXT DEFAULT 'url', video_ref TEXT,
      modo INTEGER DEFAULT 2,
      youtube_url TEXT, mascara INTEGER DEFAULT 1,
      chat_real INTEGER DEFAULT 1, chat_ia INTEGER DEFAULT 0,
      contador INTEGER DEFAULT 1, acumulado INTEGER DEFAULT 1,
      popups INTEGER DEFAULT 1, autoridade INTEGER DEFAULT 1,
      ofertas_json TEXT DEFAULT '[]', ia_prompt TEXT, projeto TEXT,
      ativo INTEGER DEFAULT 1, criado_em TEXT, atualizado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS webinario_chat (
      id TEXT PRIMARY KEY, slug TEXT, nome TEXT, texto TEXT, origem TEXT DEFAULT 'espectador', criado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS webinario_presenca (
      slug TEXT, sessao TEXT, lead TEXT, pct INTEGER DEFAULT 0, ultimo_ping TEXT, criado_em TEXT,
      PRIMARY KEY (slug, sessao)
    );
  `);

  const wbSlug = (s: string): string => String(s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 48) || ('w-' + randomUUID().slice(0, 6));
  const wbBase = (): string => String(PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  const wbGet = (slugOrId: string): any => db.prepare(`SELECT * FROM webinarios WHERE slug=? OR id=?`).get(String(slugOrId), String(slugOrId)) as any;

  // [sim] AUDIÊNCIA SIMULADA por webinário (contador + comentários programados)
  db.exec(`CREATE TABLE IF NOT EXISTS webinario_sim (
    slug TEXT PRIMARY KEY, ativo INTEGER DEFAULT 0, cfg_json TEXT DEFAULT '{}', coment_json TEXT DEFAULT '[]', atualizado_em TEXT
  )`);
  const simGet = (slug: string): any => {
    const row = db.prepare(`SELECT * FROM webinario_sim WHERE slug=?`).get(String(slug)) as any;
    if (!row || !row.ativo) return null;
    let cfg: any = {}; let coment: any[] = [];
    try { cfg = JSON.parse(row.cfg_json || '{}'); } catch { cfg = {}; }
    try { coment = JSON.parse(row.coment_json || '[]'); } catch { coment = []; }
    return { cfg, comentarios: coment };
  };

  // config pública (o que a sala precisa; sem nada sensível)
  function wbConfigPublico(wb: any): any {
    let ofertas: any[] = []; try { ofertas = JSON.parse(wb.ofertas_json || '[]'); } catch { ofertas = []; }
    let videoUrl = '';
    if (Number(wb.modo) === 1) videoUrl = String(wb.video_tipo) === 'url' ? String(wb.video_ref || '') : '';
    const s = simGet(wb.slug);
    const sim = s ? {
      vMin: Number(s.cfg.vMin) || 0, vMax: Number(s.cfg.vMax) || 0,
      vInicial: Number(s.cfg.vInicial) || Number(s.cfg.vMin) || 0,
      variacao: Number(s.cfg.variacao) || 2, intervalo: Number(s.cfg.intervalo) || 6,
      comentarios: (s.comentarios || []).map((c: any) => ({ t: Number(c.tempoSeg) || 0, nome: String(c.nome || 'Participante'), texto: String(c.texto || '') })).filter((c: any) => c.texto)
    } : null;
    return {
      slug: wb.slug, nome: wb.nome || 'Webinário', modo: Number(wb.modo) || 2,
      youtube_url: wb.youtube_url || '', video_url: videoUrl,
      mascara: !!wb.mascara, chat_real: !!wb.chat_real, chat_ia: !!wb.chat_ia,
      contador: !!wb.contador, acumulado: !!wb.acumulado, popups: !!wb.popups, autoridade: !!wb.autoridade,
      ofertas, sim
    };
  }

  // ---------- config (app, com x-api-key) ----------
  app.get('/webinarios', (_req: any, res: any) => {
    try { const rows = db.prepare(`SELECT * FROM webinarios ORDER BY criado_em DESC LIMIT 100`).all() as any[]; res.json({ ok: true, webinarios: rows, base: wbBase() }); }
    catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.get('/webinarios/:id', (req: any, res: any) => {
    const wb = wbGet(String(req.params.id)); if (!wb) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
    res.json({ ok: true, webinario: wb, link: wbBase() + '/w/' + wb.slug });
  });
  app.post('/webinarios', (req: any, res: any) => {
    try {
      const b = req.body || {};
      const nome = String(b.nome || 'Webinário').trim();
      const id = String(b.id || '').trim() || randomUUID();
      const existe = b.id ? wbGet(String(b.id)) : null;
      const slug = existe ? existe.slug : wbSlug(nome + '-' + id.slice(0, 4));
      const num = (v: any, d: number) => (v === undefined || v === null || v === '') ? d : (Number(v) ? 1 : 0);
      const row = {
        video_tipo: String(b.video_tipo || 'url'), video_ref: String(b.video_ref || ''),
        modo: [1, 2, 3].includes(Number(b.modo)) ? Number(b.modo) : 2,
        youtube_url: String(b.youtube_url || ''), mascara: num(b.mascara, 1),
        chat_real: num(b.chat_real, 1), chat_ia: num(b.chat_ia, 0),
        contador: num(b.contador, 1), acumulado: num(b.acumulado, 1),
        popups: num(b.popups, 1), autoridade: num(b.autoridade, 1),
        ofertas_json: JSON.stringify(Array.isArray(b.ofertas) ? b.ofertas : []),
        ia_prompt: String(b.ia_prompt || ''), projeto: String(b.projeto || '')
      };
      db.prepare(`INSERT INTO webinarios(id,slug,nome,video_tipo,video_ref,modo,youtube_url,mascara,chat_real,chat_ia,contador,acumulado,popups,autoridade,ofertas_json,ia_prompt,projeto,ativo,criado_em,atualizado_em)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
        ON CONFLICT(id) DO UPDATE SET nome=excluded.nome,video_tipo=excluded.video_tipo,video_ref=excluded.video_ref,modo=excluded.modo,youtube_url=excluded.youtube_url,mascara=excluded.mascara,chat_real=excluded.chat_real,chat_ia=excluded.chat_ia,contador=excluded.contador,acumulado=excluded.acumulado,popups=excluded.popups,autoridade=excluded.autoridade,ofertas_json=excluded.ofertas_json,ia_prompt=excluded.ia_prompt,projeto=excluded.projeto,atualizado_em=excluded.atualizado_em`)
        .run(id, slug, nome, row.video_tipo, row.video_ref, row.modo, row.youtube_url, row.mascara, row.chat_real, row.chat_ia, row.contador, row.acumulado, row.popups, row.autoridade, row.ofertas_json, row.ia_prompt, row.projeto, agora(), agora());
      res.json({ ok: true, id, slug, link: wbBase() + '/w/' + slug });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/webinarios/remover', (req: any, res: any) => {
    try { const id = String((req.body || {}).id || ''); db.prepare(`DELETE FROM webinarios WHERE id=?`).run(id); res.json({ ok: true }); }
    catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // ---------- APIs públicas da sala ----------
  app.get('/w-api/:slug', (req: any, res: any) => {
    const wb = wbGet(String(req.params.slug)); if (!wb || !wb.ativo) return res.status(404).json({ ok: false });
    res.json({ ok: true, cfg: wbConfigPublico(wb) });
  });
  // presença/contador real
  app.post('/w-api/:slug/ping', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const b = req.body || {};
      const sessao = String(b.sessao || '').slice(0, 60); if (!sessao) return res.json({ ok: false });
      const pct = Math.max(0, Math.min(100, Number(b.pct) || 0));
      const lead = String(b.lead || '').slice(0, 120);
      db.prepare(`INSERT INTO webinario_presenca(slug,sessao,lead,pct,ultimo_ping,criado_em) VALUES(?,?,?,?,?,?)
        ON CONFLICT(slug,sessao) DO UPDATE SET pct=MAX(webinario_presenca.pct,excluded.pct),lead=COALESCE(NULLIF(excluded.lead,''),webinario_presenca.lead),ultimo_ping=excluded.ultimo_ping`)
        .run(slug, sessao, lead, pct, agora(), agora());
      const corte = new Date(Date.now() - 45000).toISOString();
      const online = (db.prepare(`SELECT COUNT(*) n FROM webinario_presenca WHERE slug=? AND ultimo_ping>=?`).get(slug, corte) as any).n;
      const acumulado = (db.prepare(`SELECT COUNT(*) n FROM webinario_presenca WHERE slug=?`).get(slug) as any).n;
      res.json({ ok: true, online, acumulado });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  // chat real
  app.get('/w-api/:slug/chat', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug);
      const rows = db.prepare(`SELECT id,nome,texto,origem,criado_em FROM webinario_chat WHERE slug=? ORDER BY criado_em DESC LIMIT 60`).all(slug) as any[];
      res.json({ ok: true, mensagens: rows.reverse() });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  const wbChatRate: Map<string, number> = new Map();
  app.post('/w-api/:slug/chat', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const b = req.body || {};
      const wb = wbGet(slug); if (!wb || !wb.chat_real) return res.status(403).json({ ok: false });
      const nome = String(b.nome || 'Visitante').slice(0, 40).trim() || 'Visitante';
      const texto = String(b.texto || '').slice(0, 400).trim(); if (!texto) return res.json({ ok: false });
      const chave = slug + ':' + String(b.sessao || ''); const last = wbChatRate.get(chave) || 0;
      if (Date.now() - last < 1500) return res.status(429).json({ ok: false, erro: 'devagar' });
      wbChatRate.set(chave, Date.now());
      db.prepare(`INSERT INTO webinario_chat(id,slug,nome,texto,origem,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), slug, nome, texto, 'espectador', agora());
      // chat IA (opcional) — responde identificado como a escola, fire-and-forget
      if (wb.chat_ia && LLM_API_KEY) {
        (async () => {
          try {
            const sys = String(wb.ia_prompt || '').trim() || 'Você é a equipe da escola de música Melodias e Riffs respondendo o chat de um webinário ao vivo. Responda em português, de forma breve (1-2 frases), acolhedora e útil, identificada como a escola. Não invente promoções, preços ou dados. Se for pergunta de compra, oriente a clicar no botão/link da oferta na tela.';
            const r = await _chamarLLM([{ role: 'system', content: sys }, { role: 'user', content: nome + ' perguntou: ' + texto }], wb.ia_modelo || '', 0.6, []);
            const resp = String(r?.content || '').slice(0, 400).trim();
            if (resp) db.prepare(`INSERT INTO webinario_chat(id,slug,nome,texto,origem,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), slug, 'Equipe Melodias e Riffs', resp, 'ia', agora());
          } catch { /* ignora */ }
        })();
      }
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  // mensagem do HOST (pelo app, com x-api-key) — entra no chat como a escola
  app.post('/webinarios/:slug/host-msg', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const texto = String((req.body || {}).texto || '').slice(0, 400).trim();
      if (!texto) return res.json({ ok: false });
      db.prepare(`INSERT INTO webinario_chat(id,slug,nome,texto,origem,criado_em) VALUES(?,?,?,?,?,?)`).run(randomUUID(), slug, String((req.body || {}).nome || 'Professor Ezequias'), texto, 'host', agora());
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  // [sim] CONFIG da audiência simulada por webinário (app, x-api-key)
  app.get('/webinarios/:slug/simulacao', (req: any, res: any) => {
    try {
      const wb = wbGet(String(req.params.slug)); if (!wb) return res.status(404).json({ ok: false });
      const row = db.prepare(`SELECT * FROM webinario_sim WHERE slug=?`).get(wb.slug) as any;
      let cfg: any = {}; let coment: any[] = [];
      if (row) { try { cfg = JSON.parse(row.cfg_json || '{}'); } catch { cfg = {}; } try { coment = JSON.parse(row.coment_json || '[]'); } catch { coment = []; } }
      res.json({ ok: true, ativo: !!(row && row.ativo), cfg, comentarios: coment });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/webinarios/:slug/simulacao', (req: any, res: any) => {
    try {
      const wb = wbGet(String(req.params.slug)); if (!wb) return res.status(404).json({ ok: false });
      const b = req.body || {}; const bc = b.cfg || {};
      const cfg = {
        vMin: Math.max(0, Math.round(Number(bc.vMin) || 0)),
        vMax: Math.max(0, Math.round(Number(bc.vMax) || 0)),
        vInicial: Math.max(0, Math.round(Number(bc.vInicial) || 0)),
        variacao: Math.max(0, Math.min(500, Math.round(Number(bc.variacao) || 2))),
        intervalo: Math.max(2, Math.min(120, Math.round(Number(bc.intervalo) || 6)))
      };
      const coment = (Array.isArray(b.comentarios) ? b.comentarios : []).slice(0, 5000).map((c: any, i: number) => ({
        tempoSeg: Math.max(0, Math.min(86400, Math.round(Number(c?.tempoSeg) || 0))),
        nome: String(c?.nome || ('Participante ' + (i + 1))).trim().slice(0, 80),
        texto: String(c?.texto || '').trim().slice(0, 400)
      })).filter((c: any) => c.texto);
      db.prepare(`INSERT INTO webinario_sim(slug,ativo,cfg_json,coment_json,atualizado_em) VALUES(?,?,?,?,?)
        ON CONFLICT(slug) DO UPDATE SET ativo=excluded.ativo,cfg_json=excluded.cfg_json,coment_json=excluded.coment_json,atualizado_em=excluded.atualizado_em`)
        .run(wb.slug, b.ativo ? 1 : 0, JSON.stringify(cfg), JSON.stringify(coment), agora());
      res.json({ ok: true, total: coment.length });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  // [sim] IA gera comentários a partir do tema do webinário (não salva; o app salva depois)
  app.post('/webinarios/:slug/simulacao/ia', async (req: any, res: any) => {
    try {
      const wb = wbGet(String(req.params.slug)); if (!wb) return res.status(404).json({ ok: false });
      if (!LLM_API_KEY) return res.json({ ok: false, erro: 'IA sem chave (LLM_API_KEY) no motor.' });
      const b = req.body || {};
      const qtd = Math.max(5, Math.min(80, Math.round(Number(b.qtd) || 25)));
      const tema = String(b.tema || wb.nome || 'aula de música gospel').slice(0, 240);
      const dur = Math.max(60, Math.min(14400, Math.round(Number(b.duracaoSeg) || 1500)));
      const sys = 'Você gera comentários de chat de um webinário AO VIVO de uma escola de música gospel (Melodias e Riffs), em português do Brasil. Os comentários são de ALUNOS/participantes: curtos (3 a 12 palavras), naturais e variados (saudações, elogios, dúvidas, cidades do Brasil, empolgação, emojis ocasionais). Não repita nomes. Responda SOMENTE um array JSON valido, sem texto fora dele, no formato [{"nome":"Primeiro Nome","texto":"...","tempoSeg": N}], com ' + qtd + ' itens, com tempoSeg distribuidos de 0 a ' + dur + ' em ordem crescente.';
      const r = await _chamarLLM([{ role: 'system', content: sys }, { role: 'user', content: 'Tema da aula: ' + tema }], String(b.modelo || ''), 0.9, []);
      let txt = String(r?.content || '').trim();
      const ini = txt.indexOf('['); const fim = txt.lastIndexOf(']');
      if (ini >= 0 && fim > ini) txt = txt.slice(ini, fim + 1);
      let arr: any[] = []; try { arr = JSON.parse(txt); } catch { arr = []; }
      const coment = (Array.isArray(arr) ? arr : []).slice(0, qtd).map((c: any, i: number) => ({
        tempoSeg: Math.max(0, Math.round(Number(c?.tempoSeg) || Math.round(i * dur / qtd))),
        nome: String(c?.nome || ('Participante ' + (i + 1))).trim().slice(0, 80),
        texto: String(c?.texto || '').trim().slice(0, 400)
      })).filter((c: any) => c.texto);
      if (!coment.length) return res.json({ ok: false, erro: 'A IA não retornou comentários válidos. Tente de novo.' });
      res.json({ ok: true, comentarios: coment });
    } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
  });
  // vendas REAIS recentes (pros popups) — da tabela transacoes, só primeiro nome
  app.get('/w-api/:slug/vendas', (req: any, res: any) => {
    try {
      const wb = wbGet(String(req.params.slug)); if (!wb || !wb.popups) return res.json({ ok: true, vendas: [] });
      const desde = new Date(Date.now() - 3 * 3600 * 1000).toISOString();
      const cond = wb.projeto ? `AND t.projeto=?` : '';
      const args: any[] = [desde]; if (wb.projeto) args.push(String(wb.projeto));
      const rows = db.prepare(`SELECT t.produto, t.criado_em, c.nome nome FROM transacoes t LEFT JOIN contatos c ON c.id=t.contato_id
        WHERE t.status='pago' AND t.criado_em>=? ${cond} ORDER BY t.criado_em DESC LIMIT 12`).all(...args) as any[];
      const vendas = rows.map((r) => ({ nome: String(r.nome || 'Alguém').trim().split(/\s+/)[0], produto: String(r.produto || '').slice(0, 60) }));
      res.json({ ok: true, vendas });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // ---------- a SALA (HTML público) ----------
  app.get('/w/:slug', (req: any, res: any) => {
    const wb = wbGet(String(req.params.slug));
    if (!wb || !wb.ativo) { res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><body style="font-family:sans-serif;background:#0f1117;color:#eee;display:grid;place-items:center;height:100vh"><div>Webinário não encontrado.</div></body>'); return; }
    const cfg = wbConfigPublico(wb);
    const cfgJson = JSON.stringify(cfg).replace(/</g, '\\u003c');
    res.type('html').send(wbRoomHtml(cfgJson, String(wb.slug), String(wb.nome || 'Webinário')));
  });

  // JS do cliente da sala — SEM regex com barras e SEM `${`/backticks (pra não conflitar com o template literal)
  const WB_ROOM_JS = `(function(){
  var cfg=CFG, slug=SLUG;
  var sessao='s-'+Math.random().toString(36).slice(2)+Date.now().toString(36);
  var lead=''; try{ lead=new URLSearchParams(location.search).get('lead')||''; }catch(e){}
  var vid=document.getElementById('vid'); var videoEl=null;
  function ytId(u){ u=String(u||''); var keys=['v=','youtu.be/','embed/','live/','shorts/']; for(var i=0;i<keys.length;i++){ var k=keys[i]; var p=u.indexOf(k); if(p>=0){ var rest=u.slice(p+k.length); var id=''; for(var j=0;j<rest.length;j++){ var ch=rest[j]; if((ch>='A'&&ch<='Z')||(ch>='a'&&ch<='z')||(ch>='0'&&ch<='9')||ch==='_'||ch==='-'){ id+=ch; } else { break; } } if(id.length>=6){ return id; } } } return ''; }
  if(cfg.modo===1 && cfg.video_url){
    videoEl=document.createElement('video'); videoEl.src=cfg.video_url; videoEl.controls=true; videoEl.playsInline=true; videoEl.autoplay=true;
    videoEl.controlsList='nodownload noplaybackrate noremoteplayback'; videoEl.setAttribute('controlsList','nodownload noplaybackrate noremoteplayback');
    videoEl.disablePictureInPicture=true; videoEl.setAttribute('disablePictureInPicture',''); videoEl.disableRemotePlayback=true;
    videoEl.oncontextmenu=function(){return false;};
    vid.appendChild(videoEl);
  } else {
    var yid=ytId(cfg.youtube_url);
    if(yid){
      var ifr=document.createElement('iframe'); ifr.src='https://www.youtube.com/embed/'+yid+'?autoplay=1&rel=0&modestbranding=1&playsinline=1';
      ifr.allow='autoplay; encrypted-media; picture-in-picture'; ifr.allowFullscreen=true; vid.appendChild(ifr);
      if(cfg.modo===2 && cfg.mascara){ var mk=document.createElement('div'); mk.className='mask'; vid.appendChild(mk); }
    } else { vid.innerHTML='<div style="position:absolute;inset:0;display:grid;place-items:center;color:#9aa3b2">Configure o vídeo do webinário.</div>'; }
  }
  if(cfg.autoridade){ var au=document.getElementById('auth'); if(au){ au.style.display='block'; } }
  var t0=Date.now();
  function elapsed(){ return Math.floor((Date.now()-t0)/1000); }
  function pct(){ if(videoEl && videoEl.duration){ return Math.min(100, Math.floor(videoEl.currentTime/videoEl.duration*100)); } return Math.min(100, Math.floor(elapsed()/36)); }
  // [sim] contador simulado (por webinario) — some se cfg.sim for null
  var simOn=!!cfg.sim; var simV=simOn?(Number(cfg.sim.vInicial)||Number(cfg.sim.vMin)||0):0; var realOnline=0;
  function atualizaOnline(){ if(!cfg.contador){return;} var o=document.getElementById('online'); if(!o){return;} o.textContent=(simOn?Math.max(realOnline,simV):realOnline); }
  if(simOn){ var vmin=Number(cfg.sim.vMin)||0, vmax=Number(cfg.sim.vMax)||Math.max(simV,vmin), varx=Math.max(0,Number(cfg.sim.variacao)||2), itv=Math.max(2,Number(cfg.sim.intervalo)||6);
    setInterval(function(){ var dd=Math.round((Math.random()*2-1)*varx); simV=Math.max(vmin,Math.min(vmax,simV+dd)); atualizaOnline(); }, itv*1000); atualizaOnline(); }
  function ping(){
    fetch('/w-api/'+slug+'/ping',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sessao:sessao,lead:lead,pct:pct()})})
      .then(function(r){return r.json();}).then(function(d){ if(!d||!d.ok){return;}
        realOnline=Number(d.online)||0; atualizaOnline();
        if(cfg.acumulado){ var ab=document.getElementById('acbox'); var ac=document.getElementById('acum'); if(ab){ab.style.display='inline-flex';} if(ac){ac.textContent=(simOn?Math.max(Number(d.acumulado)||0,Math.round(simV*1.8)):d.acumulado);} }
      }).catch(function(){});
  }
  ping(); setInterval(ping, 10000);
  var chat=document.getElementById('chat');
  var simComs=(simOn && cfg.sim.comentarios)?cfg.sim.comentarios:[];
  var ultServer=[];
  function _mkmsg(m){ var d=document.createElement('div'); d.className='msg'+(m.origem==='host'?' host':m.origem==='ia'?' ia':''); var b=document.createElement('b'); b.textContent=(m.nome||'Visitante')+': '; var s=document.createElement('span'); s.className='t'; s.textContent=m.texto||''; d.appendChild(b); d.appendChild(s); return d; }
  function desenhar(){ if(!chat){return;} var lista=[]; var i;
    for(i=0;i<ultServer.length;i++){ var m=ultServer[i]; var ts=Date.parse(m.criado_em); if(isNaN(ts)){ts=t0+i;} lista.push({nome:m.nome,texto:m.texto,origem:m.origem,_ts:ts}); }
    var e=elapsed();
    for(i=0;i<simComs.length;i++){ var c=simComs[i]; if(e>=(Number(c.t)||0)){ lista.push({nome:c.nome,texto:c.texto,origem:'sim',_ts:t0+(Number(c.t)||0)*1000+i}); } }
    lista.sort(function(a,b){ return a._ts-b._ts; });
    var perto=(chat.scrollTop+chat.clientHeight)>=(chat.scrollHeight-48);
    chat.innerHTML=''; for(i=0;i<lista.length;i++){ chat.appendChild(_mkmsg(lista[i])); }
    if(perto){ chat.scrollTop=chat.scrollHeight; }
  }
  function renderChat(ms){ ultServer=ms||[]; desenhar(); }
  function loadChat(){ fetch('/w-api/'+slug+'/chat').then(function(r){return r.json();}).then(function(d){ if(d&&d.ok){renderChat(d.mensagens||[]);} }).catch(function(){}); }
  if(cfg.chat_real){ loadChat(); setInterval(loadChat, 3000); } else { var cbx=document.getElementById('cbox'); if(cbx){cbx.style.display='none';} }
  if(simOn){ desenhar(); setInterval(desenhar, 1500); }
  var ci=document.getElementById('ci'), cbtn=document.getElementById('cb');
  function askName(){ var n=''; try{ n=localStorage.getItem('wb_nome'); }catch(e){} if(!n){ n=prompt('Seu nome pro chat:')||'Visitante'; try{localStorage.setItem('wb_nome',n);}catch(e){} } return n; }
  function send(){ var tx=(ci.value||'').trim(); if(!tx){return;} ci.value='';
    fetch('/w-api/'+slug+'/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({nome:askName(),texto:tx,sessao:sessao})}).then(function(){ setTimeout(loadChat,400); }).catch(function(){}); }
  if(cbtn){cbtn.onclick=send;} if(ci){ci.addEventListener('keydown',function(e){ if(e.key==='Enter'){send();} });}
  var vIdx=0;
  function showPop(txt){ var p=document.createElement('div'); p.className='pop'; p.textContent=txt; document.body.appendChild(p); setTimeout(function(){ if(p&&p.parentNode){p.parentNode.removeChild(p);} }, 7000); }
  function loadVendas(){ if(!cfg.popups){return;} fetch('/w-api/'+slug+'/vendas').then(function(r){return r.json();}).then(function(d){ if(!d||!d.ok||!d.vendas||!d.vendas.length){return;} var v=d.vendas[vIdx%d.vendas.length]; vIdx++; showPop(v.nome+' acabou de garantir'+(v.produto?(': '+v.produto):'')+' ✅'); }).catch(function(){}); }
  if(cfg.popups){ setTimeout(loadVendas, 15000); setInterval(loadVendas, 30000); }
  var ofertas=(cfg.ofertas||[]).map(function(o){ o._shown=false; return o; });
  function hideReabrir(){ var rb=document.getElementById('reofebtn'); if(rb){ rb.style.display='none'; } }
  function showReabrir(o){ var rb=document.getElementById('reofebtn'); if(!rb){ rb=document.createElement('button'); rb.id='reofebtn'; rb.className='reofe'; document.body.appendChild(rb); } rb.textContent='🎁 Ver oferta'; rb.onclick=function(){ mostrarOferta(o); }; rb.style.display='inline-block'; }
  function mostrarOferta(o){ var tipo=o.tipo||'banner'; hideReabrir();
    if(tipo==='popup'){ var m=document.createElement('div'); m.className='modal'; var b=document.createElement('div'); b.className='b';
      var x=document.createElement('span'); x.textContent='✕'; x.style.position='absolute'; x.style.top='10px'; x.style.right='14px'; x.style.cursor='pointer'; x.onclick=function(){ if(m.parentNode){m.parentNode.removeChild(m);} showReabrir(o); };
      var h=document.createElement('div'); h.style.fontSize='17px'; h.style.fontWeight='700'; h.textContent=o.titulo||'Oferta especial';
      var p=document.createElement('div'); p.style.color='#9aa3b2'; p.style.margin='8px 0 14px'; p.textContent=o.conteudo||'';
      var a=document.createElement('a'); a.href=o.url||'#'; a.target='_blank'; a.textContent=o.botao||'Quero garantir'; a.style.display='inline-block'; a.style.background='#7c6cf0'; a.style.color='#fff'; a.style.fontWeight='700'; a.style.textDecoration='none'; a.style.padding='11px 22px'; a.style.borderRadius='999px';
      b.appendChild(x); b.appendChild(h); b.appendChild(p); b.appendChild(a); m.appendChild(b); document.body.appendChild(m);
    } else { var off=document.getElementById('offer'); if(!off){return;} var ban=document.createElement('div'); ban.className='banner'; ban.style.position='relative';
      var x2=document.createElement('span'); x2.className='x'; x2.textContent='✕'; x2.onclick=function(){ if(ban.parentNode){ban.parentNode.removeChild(ban);} showReabrir(o); };
      var h2=document.createElement('div'); h2.style.fontSize='16px'; h2.style.fontWeight='700'; h2.style.color='#fff'; h2.textContent=o.titulo||'Oferta';
      var p2=document.createElement('div'); p2.style.color='#eee'; p2.style.fontSize='13px'; p2.textContent=o.conteudo||'';
      ban.appendChild(x2); ban.appendChild(h2); ban.appendChild(p2);
      if(o.url){ var a2=document.createElement('a'); a2.href=o.url; a2.target='_blank'; a2.textContent=o.botao||'Garantir agora'; ban.appendChild(a2); } off.appendChild(ban); } }
  function checkOfertas(){ var e=elapsed(); ofertas.forEach(function(o){ if(o._shown){return;} if(e < (Number(o.momento)||0)){return;} o._shown=true; mostrarOferta(o); }); }
  setInterval(checkOfertas, 1000);
})();`;

  // gera o HTML da sala (client JS sem template-literal pra não conflitar com o template do server)
  function wbRoomHtml(cfgJson: string, slug: string, nome: string): string {
    const nomeEsc = String(nome).replace(/</g, '&lt;');
    return '<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">'
      + '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">'
      + '<title>' + nomeEsc + '</title><style>'
      + ':root{--bg:#0f1117;--card:#171a22;--bd:#262a36;--fg:#f2f4f8;--mut:#9aa3b2;--ac:#7c6cf0;--ok:#3fb27f}'
      + '*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}'
      + '.wrap{max-width:1180px;margin:0 auto;padding:14px;display:grid;grid-template-columns:1fr 340px;gap:14px}'
      + '@media(max-width:860px){html,body{height:100%;overflow:hidden}'
      + '.wrap{height:100vh;height:100dvh;max-width:none;padding:8px;gap:8px;display:flex;flex-direction:column;min-height:0}'
      + '.wrap>div:first-child{display:flex;flex-direction:column;min-height:0;flex:0 0 auto}'
      + '.vid{aspect-ratio:16/9}'
      + '.side{flex:1 1 auto;height:auto;min-height:0}'
      + '.offer{margin:8px 0 0}.offer:empty{display:none}.banner{padding:12px}}'
      + '.vid{position:relative;background:#000;border-radius:14px;overflow:hidden;aspect-ratio:16/9}'
      + '.vid iframe,.vid video{position:absolute;inset:0;width:100%;height:100%;border:0}'
      + '.mask{position:absolute;inset:0;z-index:5}'
      + '.top{display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap}'
      + '.badge{display:inline-flex;align-items:center;gap:6px;background:var(--card);border:1px solid var(--bd);border-radius:999px;padding:5px 11px;font-size:12.5px;color:var(--mut)}'
      + '.dot{width:8px;height:8px;border-radius:50%;background:var(--ok);display:inline-block;animation:pulse 1.6s infinite}'
      + '@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}'
      + '.side{background:var(--card);border:1px solid var(--bd);border-radius:14px;display:flex;flex-direction:column;height:72vh;min-height:420px}'
      + '.side h3{margin:0;padding:12px 14px;border-bottom:1px solid var(--bd);font-size:14px}'
      + '.chat{flex:1;overflow-y:auto;padding:10px 12px;display:flex;flex-direction:column;gap:9px}'
      + '.msg{font-size:13px;line-height:1.4}.msg b{color:#b3a7ff}.msg.host b{color:#ffcf6b}.msg.ia b{color:#7fd6a8}'
      + '.msg .t{color:var(--fg)}'
      + '.cbox{display:flex;gap:6px;padding:10px;border-top:1px solid var(--bd)}'
      + '.cbox input{flex:1;min-width:0;background:var(--bg);border:1px solid var(--bd);border-radius:9px;color:var(--fg);padding:9px 10px;font-size:13px}'
      + '.cbox button{background:var(--ac);border:0;color:#fff;border-radius:9px;padding:9px 13px;font-weight:600;cursor:pointer}'
      + '.pop{position:fixed;left:14px;bottom:14px;background:var(--card);border:1px solid var(--bd);border-left:3px solid var(--ok);border-radius:11px;padding:11px 14px;font-size:13px;box-shadow:0 10px 30px rgba(0,0,0,.4);z-index:40;max-width:300px}'
      + '.reofe{position:fixed;right:14px;bottom:14px;z-index:46;background:var(--ac);color:#fff;border:0;border-radius:999px;padding:12px 18px;font-weight:700;font-size:14px;box-shadow:0 10px 30px rgba(124,108,240,.5);cursor:pointer;animation:pulse 1.8s infinite}'
      + '@media(max-width:860px){.reofe{bottom:64px;right:10px;padding:10px 15px;font-size:13px}}'
      + '.offer{margin-top:12px}.banner{background:linear-gradient(135deg,#4a3fb0,#7e6cf0);border-radius:12px;padding:16px;text-align:center}'
      + '.banner a{display:inline-block;margin-top:8px;background:#fff;color:#2a2550;font-weight:700;text-decoration:none;padding:10px 20px;border-radius:999px}'
      + '.banner .x{position:absolute;top:8px;right:12px;color:#fff;cursor:pointer;font-weight:700}'
      + '.modal{position:fixed;inset:0;background:rgba(0,0,0,.6);display:grid;place-items:center;z-index:50;padding:16px}'
      + '.modal .b{background:var(--card);border:1px solid var(--bd);border-radius:16px;padding:22px;max-width:420px;text-align:center;position:relative}'
      + '.auth{font-size:12px;color:var(--mut);margin-top:10px;text-align:center}'
      // [sala-ajuste] celular DEITADO (paisagem): chat SEMPRE do lado, vídeo preenche a altura
      + '@media(orientation:landscape) and (max-height:600px){'
      + '.wrap{flex-direction:row}'
      + '.wrap>div:first-child{flex:1 1 auto}'
      + '.vid{aspect-ratio:auto;flex:1;min-height:0}'
      + '.side{flex:0 0 300px;max-width:46%;height:auto;min-height:0}'
      + '.top{margin-bottom:6px}.auth{display:none}}'
      + '</style></head><body>'
      + '<div class="wrap"><div><div class="top">'
      + '<div class="badge"><span class="dot"></span><span id="online">—</span> assistindo agora</div>'
      + '<div class="badge" id="acbox" style="display:none">👥 <span id="acum">—</span> já assistiram</div>'
      + '<div class="badge" style="font-weight:600;color:var(--fg)">' + nomeEsc + '</div></div>'
      + '<div class="vid" id="vid"></div>'
      + '<div class="offer" id="offer"></div>'
      + '<div class="auth" id="auth" style="display:none">+3.200 músicos já passaram pela escola · depoimentos reais</div>'
      + '</div><div class="side"><h3>💬 Chat ao vivo</h3><div class="chat" id="chat"></div>'
      + '<div class="cbox" id="cbox"><input id="ci" maxlength="400" placeholder="Escreva no chat..."><button id="cb">Enviar</button></div></div></div>'
      + '<script>var CFG=' + cfgJson + ';var SLUG=' + JSON.stringify(slug) + ';</script>'
      + '<script>' + WB_ROOM_JS + '</script></body></html>';
  }

  console.log('[webinario-sala] carregado: /webinarios (config) + /w/:slug (sala) + /w-api/:slug/* (chat/contador/vendas reais)');
}

// ============================================================================
// [webinario-posaula] PARTE 5 — pós-aula: % assistido -> TAG -> dispara as
// automações que já existem (reusa aplicarTag + rodarAutomacoes). + relatório.
// O link da sala por lead usa ?lead=<contatoId> (aí o % fica por pessoa).
// Aditivo. Não roda sozinho — o app chama POST /webinarios/:slug/processar.
// ============================================================================
{
  try { db.exec(`ALTER TABLE webinario_presenca ADD COLUMN processado INTEGER DEFAULT 0`); } catch { /* já existe */ }
  db.exec(`CREATE TABLE IF NOT EXISTS webinario_regras (
    id TEXT PRIMARY KEY, slug TEXT, faixa INTEGER, tag TEXT, criado_em TEXT
  );`);
  const wbPGet = (slug: string): any => db.prepare(`SELECT * FROM webinarios WHERE slug=? OR id=?`).get(String(slug), String(slug)) as any;

  app.get('/webinarios/:slug/regras', (req: any, res: any) => {
    try { const rows = db.prepare(`SELECT id,faixa,tag FROM webinario_regras WHERE slug=? ORDER BY faixa ASC`).all(String(req.params.slug)) as any[]; res.json({ ok: true, regras: rows }); }
    catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.post('/webinarios/:slug/regras', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const regras = Array.isArray((req.body || {}).regras) ? req.body.regras : [];
      db.prepare(`DELETE FROM webinario_regras WHERE slug=?`).run(slug);
      const ins = db.prepare(`INSERT INTO webinario_regras(id,slug,faixa,tag,criado_em) VALUES(?,?,?,?,?)`);
      for (const r of regras) { const faixa = Math.max(0, Math.min(100, Number(r.faixa) || 0)); const tag = String(r.tag || '').trim(); if (tag) ins.run(randomUUID(), slug, faixa, tag, agora()); }
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // processa o pós-aula: aplica a tag da MAIOR faixa que cada lead alcançou + dispara automações
  app.post('/webinarios/:slug/processar', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const wb = wbPGet(slug);
      if (!wb) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
      const projeto = String(wb.projeto || '');
      const regras = db.prepare(`SELECT faixa,tag FROM webinario_regras WHERE slug=? ORDER BY faixa DESC`).all(slug) as any[];
      if (!regras.length) return res.json({ ok: true, processados: 0, aviso: 'sem regras' });
      const soNovos = !((req.body || {}).reprocessar);
      const pres = db.prepare(`SELECT sessao,lead,pct,processado FROM webinario_presenca WHERE slug=? AND lead IS NOT NULL AND lead<>''`).all(slug) as any[];
      // consolida por lead (maior pct)
      const porLead: Record<string, number> = {};
      for (const p of pres) { if (soNovos && p.processado) continue; const l = String(p.lead); porLead[l] = Math.max(porLead[l] || 0, Number(p.pct) || 0); }
      let processados = 0; const porTag: Record<string, number> = {};
      for (const lead of Object.keys(porLead)) {
        const pct = porLead[lead];
        const regra = regras.find((r) => pct >= Number(r.faixa)); // regras DESC: pega a maior faixa atingida
        if (!regra) continue;
        try {
          if (projeto) { aplicarTag(projeto, lead, String(regra.tag)); rodarAutomacoes(projeto, 'tag_adicionada', { contatoId: lead, tag: String(regra.tag), origem: 'webinario' }).catch(() => { /* */ }); }
          porTag[regra.tag] = (porTag[regra.tag] || 0) + 1; processados++;
        } catch { /* ignora lead inválido */ }
      }
      db.prepare(`UPDATE webinario_presenca SET processado=1 WHERE slug=?`).run(slug);
      res.json({ ok: true, processados, por_tag: porTag, sem_projeto: !projeto });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  // relatório: quantos entraram, distribuição por faixa de %, e vendas no período
  app.get('/webinarios/:slug/relatorio', (req: any, res: any) => {
    try {
      const slug = String(req.params.slug); const wb = wbPGet(slug);
      if (!wb) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
      const total = (db.prepare(`SELECT COUNT(*) n FROM webinario_presenca WHERE slug=?`).get(slug) as any).n;
      const comLead = (db.prepare(`SELECT COUNT(*) n FROM webinario_presenca WHERE slug=? AND lead IS NOT NULL AND lead<>''`).get(slug) as any).n;
      const faixa = (min: number) => (db.prepare(`SELECT COUNT(*) n FROM webinario_presenca WHERE slug=? AND pct>=?`).get(slug, min) as any).n;
      const pctMedio = (db.prepare(`SELECT AVG(pct) m FROM webinario_presenca WHERE slug=?`).get(slug) as any).m || 0;
      let vendas = 0;
      try {
        const desde = String(wb.criado_em || new Date(Date.now() - 24 * 3600 * 1000).toISOString());
        const cond = wb.projeto ? `AND projeto=?` : ''; const args: any[] = [desde]; if (wb.projeto) args.push(String(wb.projeto));
        vendas = (db.prepare(`SELECT COUNT(*) n FROM transacoes WHERE status='pago' AND criado_em>=? ${cond}`).get(...args) as any).n;
      } catch { vendas = 0; }
      res.json({ ok: true, relatorio: { total_sessoes: total, com_lead: comLead, pct_medio: Math.round(pctMedio), faixa_10: faixa(10), faixa_20: faixa(20), faixa_50: faixa(50), faixa_100: faixa(100), vendas_periodo: vendas } });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });

  console.log('[webinario-posaula] carregado: /webinarios/:slug/regras|processar|relatorio (% -> tag -> automações)');
}

// ============================================================================
// [midia-ia] PARTE 6 — Transcrição (Whisper) + Legendas (SRT) + Cortes automáticos.
// Aditivo. Reusa ffmpeg (já instalado), MEDIA_DIR, _chamarLLM, pbBaixarMidia e a
// API compatível OpenAI (LLM_BASE_URL /audio/transcriptions). Tudo async (job id).
// ============================================================================
{
  db.exec(`
    CREATE TABLE IF NOT EXISTS transcricao_jobs (
      id TEXT PRIMARY KEY, fonte_ref TEXT, status TEXT DEFAULT 'processando',
      srt_url TEXT, texto TEXT, segments_json TEXT, erro TEXT, criado_em TEXT, atualizado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS cortes_jobs (
      id TEXT PRIMARY KEY, fonte_ref TEXT, status TEXT DEFAULT 'processando',
      resultado_json TEXT DEFAULT '[]', erro TEXT, criado_em TEXT, atualizado_em TEXT
    );
  `);
  const mdBase = (): string => String(MEDIA_PUBLIC_BASE || PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  function ffmpegRun(args: string[]): Promise<number> {
    return new Promise((resolve) => { const c = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'ignore'] }); c.on('exit', (code: number) => resolve(code || 0)); c.on('error', () => resolve(1)); });
  }
  function segParaTempo(s: number, virg: string): string {
    const ms = Math.floor((s - Math.floor(s)) * 1000); const tot = Math.floor(s);
    const hh = String(Math.floor(tot / 3600)).padStart(2, '0'); const mm = String(Math.floor((tot % 3600) / 60)).padStart(2, '0'); const ss = String(tot % 60).padStart(2, '0');
    return `${hh}:${mm}:${ss}${virg}${String(ms).padStart(3, '0')}`;
  }
  function segmentsParaSrt(segs: any[]): string {
    return segs.map((s, i) => `${i + 1}\n${segParaTempo(Number(s.start) || 0, ',')} --> ${segParaTempo(Number(s.end) || 0, ',')}\n${String(s.text || '').trim()}\n`).join('\n');
  }
  async function midiaTranscrever(input: string, idioma?: string): Promise<{ srt: string; texto: string; segments: any[] }> {
    if (!LLM_API_KEY) throw new Error('sem LLM_API_KEY no motor (configure a chave da OpenAI no .env)');
    const audio = `${MEDIA_DIR}/aud-${randomUUID().slice(0, 8)}.mp3`;
    const code = await ffmpegRun(['-hide_banner', '-loglevel', 'error', '-y', '-i', input, '-vn', '-ac', '1', '-ar', '16000', '-b:a', '32k', audio]);
    if (code !== 0) throw new Error('falha ao extrair o áudio (ffmpeg)');
    try {
      const buf = fs.readFileSync(audio);
      const fd = new FormData();
      fd.append('file', new Blob([buf]), 'audio.mp3');
      fd.append('model', process.env.WHISPER_MODEL || 'whisper-1');
      fd.append('response_format', 'verbose_json');
      if (idioma) fd.append('language', idioma);
      const r = await fetch(`${LLM_BASE_URL}/audio/transcriptions`, { method: 'POST', headers: { Authorization: `Bearer ${LLM_API_KEY}` }, body: fd as any });
      const txt = await r.text(); if (!r.ok) throw new Error(`whisper ${r.status}: ${txt.slice(0, 200)}`);
      const data: any = JSON.parse(txt);
      const segments = (data.segments || []).map((s: any) => ({ start: Number(s.start) || 0, end: Number(s.end) || 0, text: String(s.text || '').trim() }));
      return { srt: segmentsParaSrt(segments), texto: String(data.text || ''), segments };
    } finally { try { fs.unlinkSync(audio); } catch { /* */ } }
  }

  // --------- transcrição / legenda ---------
  app.post('/midia/transcrever', (req: any, res: any) => {
    try {
      const b = req.body || {}; if (!b.fonte_ref) return res.status(400).json({ ok: false, erro: 'fonte_ref_obrigatorio' });
      const id = randomUUID();
      db.prepare(`INSERT INTO transcricao_jobs(id,fonte_ref,status,criado_em,atualizado_em) VALUES(?,?,?,?,?)`).run(id, String(b.fonte_ref), 'processando', agora(), agora());
      (async () => {
        try {
          const input = String(b.fonte_tipo || 'onedrive') === 'url' ? String(b.fonte_ref) : pbBaixarMidia(String(b.fonte_ref)).file;
          const r = await midiaTranscrever(input, b.idioma ? String(b.idioma) : 'pt');
          const nome = `legenda-${id.slice(0, 8)}.srt`; fs.writeFileSync(`${MEDIA_DIR}/${nome}`, r.srt);
          db.prepare(`UPDATE transcricao_jobs SET status='pronto',srt_url=?,texto=?,segments_json=?,atualizado_em=? WHERE id=?`).run(`${mdBase()}/media/${nome}`, r.texto.slice(0, 100000), JSON.stringify(r.segments).slice(0, 500000), agora(), id);
        } catch (e: any) { db.prepare(`UPDATE transcricao_jobs SET status='erro',erro=?,atualizado_em=? WHERE id=?`).run(String(e?.message || e).slice(0, 400), agora(), id); }
      })();
      res.json({ ok: true, id });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.get('/midia/transcrever/:id', (req: any, res: any) => {
    const row = db.prepare(`SELECT id,fonte_ref,status,srt_url,erro,criado_em FROM transcricao_jobs WHERE id=?`).get(String(req.params.id)) as any;
    if (!row) return res.status(404).json({ ok: false, erro: 'nao_encontrado' }); res.json({ ok: true, job: row });
  });

  // --------- cortes automáticos (transcreve -> IA escolhe trechos -> ffmpeg corta) ---------
  app.post('/cortes/gerar', (req: any, res: any) => {
    try {
      const b = req.body || {}; if (!b.fonte_ref) return res.status(400).json({ ok: false, erro: 'fonte_ref_obrigatorio' });
      const n = Math.max(1, Math.min(10, Number(b.n) || 4));
      const id = randomUUID();
      db.prepare(`INSERT INTO cortes_jobs(id,fonte_ref,status,criado_em,atualizado_em) VALUES(?,?,?,?,?)`).run(id, String(b.fonte_ref), 'processando', agora(), agora());
      (async () => {
        try {
          const input = String(b.fonte_tipo || 'onedrive') === 'url' ? String(b.fonte_ref) : pbBaixarMidia(String(b.fonte_ref)).file;
          const tr = await midiaTranscrever(input, 'pt');
          if (!tr.segments.length) throw new Error('transcrição vazia (vídeo sem fala?)');
          // resume os segmentos pra IA (tempo + texto)
          const linhas = tr.segments.map((s: any) => `[${Math.floor(s.start)}s] ${s.text}`).join('\n').slice(0, 12000);
          const sys = 'Você edita cortes de vídeo. Recebe a transcrição com os tempos (em segundos) de uma aula/live. Escolha os ' + n + ' trechos mais interessantes e autoexplicativos para virarem cortes curtos (entre 20 e 60 segundos cada), que façam sentido sozinhos. Responda SOMENTE um JSON: {"cortes":[{"inicio":<segundos>,"fim":<segundos>,"titulo":"frase curta"}]} sem texto fora do JSON.';
          const r = await _chamarLLM([{ role: 'system', content: sys }, { role: 'user', content: linhas }], '', 0.4, []);
          let cortes: any[] = [];
          try { const m = String(r?.content || '').match(/\{[\s\S]*\}/); cortes = m ? (JSON.parse(m[0]).cortes || []) : []; } catch { cortes = []; }
          if (!cortes.length) throw new Error('a IA não retornou cortes');
          const out: any[] = [];
          for (let i = 0; i < cortes.length && i < n; i++) {
            const c = cortes[i]; const ini = Math.max(0, Number(c.inicio) || 0); let fim = Number(c.fim) || (ini + 40);
            if (fim <= ini) fim = ini + 40; if (fim - ini > 90) fim = ini + 90;
            const nome = `corte-${id.slice(0, 6)}-${i + 1}.mp4`; const outPath = `${MEDIA_DIR}/${nome}`;
            const code = await ffmpegRun(['-hide_banner', '-loglevel', 'error', '-y', '-ss', String(ini), '-to', String(fim), '-i', input, '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '21', '-c:a', 'aac', '-movflags', '+faststart', outPath]);
            if (code === 0) out.push({ titulo: String(c.titulo || ('Corte ' + (i + 1))).slice(0, 120), inicio: ini, fim, url: `${mdBase()}/media/${nome}` });
            db.prepare(`UPDATE cortes_jobs SET resultado_json=?,atualizado_em=? WHERE id=?`).run(JSON.stringify(out), agora(), id);
          }
          db.prepare(`UPDATE cortes_jobs SET status='pronto',resultado_json=?,atualizado_em=? WHERE id=?`).run(JSON.stringify(out), agora(), id);
        } catch (e: any) { db.prepare(`UPDATE cortes_jobs SET status='erro',erro=?,atualizado_em=? WHERE id=?`).run(String(e?.message || e).slice(0, 400), agora(), id); }
      })();
      res.json({ ok: true, id });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
  app.get('/cortes/:id', (req: any, res: any) => {
    const row = db.prepare(`SELECT * FROM cortes_jobs WHERE id=?`).get(String(req.params.id)) as any;
    if (!row) return res.status(404).json({ ok: false, erro: 'nao_encontrado' }); res.json({ ok: true, job: row });
  });
  app.get('/cortes', (_req: any, res: any) => { res.json({ ok: true, jobs: db.prepare(`SELECT id,fonte_ref,status,resultado_json,criado_em FROM cortes_jobs ORDER BY criado_em DESC LIMIT 50`).all() }); });

  console.log('[midia-ia] carregado: /midia/transcrever (legenda SRT) + /cortes/gerar (cortes automáticos via Whisper+IA)');
}

// ============== [telegram-recebedor] motor recebe posts do Telegram -> OneDrive (independente do PC) ==============
// DESLIGADO por padrao (pub_kv telegram_receiver_ativo != '1'). Quando ligado, o motor faz long-polling
// no robo do Telegram e, pra cada midia, pergunta instrumento + tipo e sobe DIRETO pro OneDrive.
// [v2] "Postar em TODOS os instrumentos" + TEXTO QUEIMADO na imagem (igual ao gancho do video).
// [v3] Suporte ao SERVIDOR DE ARQUIVOS LOCAL (telegram-bot-api) pra baixar ate 2GB. Ligado so quando
//      pub_kv telegram_local_api_base aponta pro servidor local (ex.: http://telegram-bot-api:8081).
//      Por padrao usa a API publica (limite ~20MB). NAO muda nada ate configurar.
const TGR_API = 'https://api.telegram.org';
function _tgrApiBase(): string {
  const b = (pubFlag('telegram_local_api_base', '') || process.env.TELEGRAM_LOCAL_API_BASE || '').trim();
  return b || TGR_API;
}
function _tgrToken(): string { try { return decifra(pubFlag('telegram_bot_token_enc', '')); } catch { return ''; } }
async function _tgrCall(metodo: string, corpo: any): Promise<any> {
  const token = _tgrToken(); if (!token) return null;
  try {
    const r = await fetch(`${_tgrApiBase()}/bot${token}/${metodo}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(corpo) });
    return await r.json();
  } catch { return null; }
}
async function _tgrSend(chatId: any, texto: string, teclado?: any): Promise<void> {
  const corpo: any = { chat_id: chatId, text: texto, parse_mode: 'HTML' };
  if (teclado) corpo.reply_markup = { inline_keyboard: teclado };
  await _tgrCall('sendMessage', corpo);
}
async function _tgrAck(cbId: string, texto?: string): Promise<void> { await _tgrCall('answerCallbackQuery', { callback_query_id: cbId, text: texto || '' }); }
const TGR_INSTR: Array<{ k: string; nome: string; pasta: string }> = [
  { k: 'tec', nome: '🎹 Teclado', pasta: 'Melodias e Riffs Teclado' },
  { k: 'bax', nome: '🎸 Baixo', pasta: 'Melodias e Riffs Baixo' },
  { k: 'vio', nome: '🎸 Violão', pasta: 'Melodias e Riffs Violão' },
  { k: 'har', nome: '🎵 Harpa / Comunidade', pasta: 'Comunidade da Harpa Cristã' },
];
const TGR_TIPO: Array<{ k: string; nome: string; pasta: string }> = [
  { k: 'reels', nome: '🎬 Reels', pasta: 'Vertical' },
  { k: 'trial', nome: '🧪 Reels de teste', pasta: 'Reels-de-teste' },
  { k: 'story', nome: '📸 Stories', pasta: 'Stories' },
  { k: 'feed', nome: '📺 Feed / Horizontal', pasta: 'Horizontal' },
  { k: 'carr', nome: '🖼️ Carrossel', pasta: 'Carrossel' },
];
const _tgrPend = new Map<string, { fileId: string; nome: string; instr?: string; tipoK?: string; ehImagem?: boolean; aguardandoTexto?: boolean; texto?: string; ts: number }>();
async function _tgrBaixar(fileId: string): Promise<{ buf?: Buffer; localPath?: string; nome: string } | null> {
  const token = _tgrToken(); if (!token) return null;
  const j = await _tgrCall('getFile', { file_id: fileId });
  const fp = j && j.result && j.result.file_path; if (!fp) return null;
  const nome = (String(fp).split('/').pop() || 'arquivo');
  const base = _tgrApiBase();
  const ehLocal = base !== TGR_API;
  // modo servidor local: file_path e um caminho ABSOLUTO no disco (volume compartilhado) — ate 2GB, sem baixar por HTTP
  if (ehLocal && (String(fp).startsWith('/') || /^[A-Za-z]:[\\/]/.test(String(fp)))) {
    try { if (fs.existsSync(fp)) return { localPath: fp, nome }; } catch { /* */ }
    return null;
  }
  try {
    const r = await fetch(`${base}/file/bot${token}/${fp}`);
    if (!r.ok) return null;
    const ab = await r.arrayBuffer();
    return { buf: Buffer.from(ab), nome };
  } catch { return null; }
}
// [texto-queimado] escreve o texto NA imagem (ffmpeg drawtext) — igual ao gancho do video.
const TGR_FONT = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf';
function _tgrQueimarTextoImg(imgPath: string, texto: string): Promise<string> {
  return new Promise((resolve) => {
    try {
      if (!fs.existsSync(TGR_FONT)) { resolve(''); return; }
      const palavras = String(texto).split(/\s+/).filter(Boolean);
      const linhas: string[] = []; let cur = '';
      for (const p of palavras) { if ((cur + ' ' + p).trim().length > 24) { if (cur) linhas.push(cur); cur = p; } else cur = (cur + ' ' + p).trim(); }
      if (cur) linhas.push(cur);
      const txtFile = MEDIA_DIR + '/tgtxt_' + Date.now() + '.txt';
      fs.writeFileSync(txtFile, linhas.join('\n'), 'utf8');
      const out = MEDIA_DIR + '/tgtxtout_' + Date.now() + '.png';
      const vf = "drawtext=fontfile='" + TGR_FONT + "':textfile='" + txtFile + "':text_align=center:fontcolor=white:fontsize=w*0.055:line_spacing=12:box=1:boxcolor=black@0.58:boxborderw=24:x=(w-text_w)/2:y=h-text_h-(h*0.08)";
      const c = spawn('ffmpeg', ['-y', '-i', imgPath, '-vf', vf, '-frames:v', '1', out], { stdio: ['ignore', 'ignore', 'ignore'] });
      const to = setTimeout(() => { try { c.kill('SIGKILL'); } catch { /* */ } resolve(''); }, 30000);
      c.on('exit', (code: number) => { clearTimeout(to); try { fs.unlinkSync(txtFile); } catch { /* */ } resolve(code === 0 && fs.existsSync(out) ? out : ''); });
      c.on('error', () => { clearTimeout(to); resolve(''); });
    } catch { resolve(''); }
  });
}
// baixa (buffer OU arquivo local do servidor 2GB), queima texto se imagem+texto, e sobe pro OneDrive
async function _tgrFinalizar(chatId: any, pend: any): Promise<void> {
  const tipo = TGR_TIPO.find((x) => x.k === pend.tipoK);
  const todos = pend.instr === 'todos';
  const instrs = todos ? TGR_INSTR : TGR_INSTR.filter((x) => x.k === pend.instr);
  if (!instrs.length || !tipo) { await _tgrSend(chatId, 'Opção inválida — manda o arquivo de novo.'); _tgrPend.delete(String(chatId)); return; }
  const alvoNome = todos ? 'TODOS os instrumentos' : instrs[0].nome;
  await _tgrSend(chatId, `⏳ Baixando e subindo pro OneDrive em <b>${alvoNome} › ${tipo.nome}</b>…`);
  const baixado = await _tgrBaixar(pend.fileId);
  if (!baixado) { await _tgrSend(chatId, '❌ Não consegui baixar do Telegram. Se for vídeo grande (acima de ~20MB) e o servidor de 2GB da nuvem não estiver ligado, suba pelo caminho normal do PC.'); _tgrPend.delete(String(chatId)); return; }
  let nome = pend.nome; if (!/\.[a-z0-9]{2,5}$/i.test(nome)) { const ext = (baixado.nome.split('.').pop() || 'mp4'); nome = nome + '.' + ext; }
  let src = ''; let tmpNosso = ''; let tmpTexto = '';
  if (baixado.localPath) { src = baixado.localPath; }
  else {
    tmpNosso = MEDIA_DIR + '/tgup_' + Date.now() + '_' + nome.replace(/[^A-Za-z0-9._-]+/g, '_');
    try { fs.writeFileSync(tmpNosso, baixado.buf as Buffer); src = tmpNosso; } catch { await _tgrSend(chatId, '❌ Falha ao salvar o arquivo temporário no servidor.'); _tgrPend.delete(String(chatId)); return; }
  }
  let comTexto = false;
  if (pend.ehImagem && pend.texto && String(pend.texto).trim()) {
    try {
      const saida = await _tgrQueimarTextoImg(src, String(pend.texto).trim());
      if (saida) { tmpTexto = saida; src = saida; comTexto = true; if (!/\.png$/i.test(nome)) nome = nome.replace(/\.[^.]+$/, '') + '.png'; }
      else await _tgrSend(chatId, '⚠️ Não consegui escrever o texto na imagem — vou subir sem o texto.');
    } catch { await _tgrSend(chatId, '⚠️ Não consegui escrever o texto na imagem — vou subir sem o texto.'); }
  }
  const oks: string[] = []; const errs: string[] = [];
  for (const inst of instrs) {
    const rel = `${inst.pasta}/${tipo.pasta}/${nome}`;
    try { _rclone(['copyto', src, _odRemotePath(rel)], { timeoutMs: 600000 }); oks.push(rel); }
    catch (e: any) { errs.push(inst.nome + ': ' + String((e && e.message) || e).slice(0, 80)); }
  }
  try { if (tmpNosso) fs.unlinkSync(tmpNosso); } catch { /* */ }
  try { if (tmpTexto) fs.unlinkSync(tmpTexto); } catch { /* */ }
  try { if (baixado.localPath) fs.unlinkSync(baixado.localPath); } catch { /* */ }
  _tgrPend.delete(String(chatId));
  if (oks.length) await _tgrSend(chatId, `✅ Subido direto pro OneDrive${comTexto ? ' (com o texto na imagem)' : ''}:\n` + oks.map((r) => '• ' + r).join('\n'));
  if (errs.length) await _tgrSend(chatId, `⚠️ Não subiu em: ` + errs.join('; '));
}
async function _tgrProcessar(upd: any): Promise<void> {
  const m = upd.message;
  if (m) {
    const chatId = m.chat && m.chat.id;
    const vid = m.video; const doc = m.document; const anim = m.animation;
    const pho = Array.isArray(m.photo) && m.photo.length ? m.photo[m.photo.length - 1] : null;
    const media = vid || doc || anim || pho;
    const pendAtual = _tgrPend.get(String(chatId));
    if (!media && pendAtual && pendAtual.aguardandoTexto && m.text && !String(m.text).startsWith('/')) {
      pendAtual.texto = String(m.text).trim(); pendAtual.aguardandoTexto = false; _tgrPend.set(String(chatId), pendAtual);
      await _tgrFinalizar(chatId, pendAtual); return;
    }
    if (!media) {
      if (m.text === '/start') await _tgrSend(chatId, '✅ Recebedor da nuvem ligado. Me manda o <b>vídeo</b> ou <b>imagem</b> que eu subo direto pro sistema, sem depender do PC.');
      else if (m.text && !String(m.text).startsWith('/')) await _tgrSend(chatId, 'Me manda um <b>vídeo</b> ou <b>imagem</b> que eu pergunto instrumento/tipo e já subo pro sistema. 👍');
      return;
    }
    const ehImagem = !!pho || (!!doc && /image\//i.test(String((doc as any).mime_type || '')));
    const nomeOrig = (doc && (doc as any).file_name) || (vid && (vid as any).file_name) || (m.caption ? String(m.caption).slice(0, 60).replace(/[\\/:*?"<>|\n\r]+/g, ' ').trim() : '') || ('tg_' + Date.now());
    _tgrPend.set(String(chatId), { fileId: media.file_id, nome: nomeOrig, ts: Date.now(), ehImagem });
    const teclado = TGR_INSTR.map((x) => [{ text: x.nome, callback_data: 'tgr:i:' + x.k }]);
    teclado.push([{ text: '🌐 Postar em TODOS os instrumentos', callback_data: 'tgr:i:todos' }]);
    await _tgrSend(chatId, `📥 Recebi <b>${nomeOrig}</b>.\n\nPra qual <b>instrumento</b>?`, teclado);
    return;
  }
  const cb = upd.callback_query;
  if (cb) {
    const chatId = cb.message && cb.message.chat && cb.message.chat.id;
    const data = String(cb.data || '');
    const pend = _tgrPend.get(String(chatId));
    if (!pend) { await _tgrAck(cb.id, 'Esse envio expirou — manda o arquivo de novo.'); return; }
    if (data.indexOf('tgr:i:') === 0) {
      pend.instr = data.slice(6); _tgrPend.set(String(chatId), pend);
      const teclado = TGR_TIPO.map((x) => [{ text: x.nome, callback_data: 'tgr:t:' + x.k }]);
      await _tgrAck(cb.id); await _tgrSend(chatId, 'E é qual <b>tipo</b> de post?', teclado); return;
    }
    if (data.indexOf('tgr:t:') === 0) {
      pend.tipoK = data.slice(6); _tgrPend.set(String(chatId), pend);
      if (pend.ehImagem) {
        pend.aguardandoTexto = true; _tgrPend.set(String(chatId), pend);
        await _tgrAck(cb.id);
        await _tgrSend(chatId, 'Quer escrever um <b>texto na imagem</b> (sai escrito na foto, igual ao vídeo)?\n\nMande o texto agora, ou toque em <b>Pular</b>.', [[{ text: '⏭️ Pular (sem texto)', callback_data: 'tgr:skip' }]]);
        return;
      }
      await _tgrAck(cb.id, 'Subindo…');
      await _tgrFinalizar(chatId, pend);
      return;
    }
    if (data === 'tgr:skip') {
      pend.texto = ''; pend.aguardandoTexto = false; _tgrPend.set(String(chatId), pend);
      await _tgrAck(cb.id, 'Subindo…');
      await _tgrFinalizar(chatId, pend);
      return;
    }
    await _tgrAck(cb.id);
  }
}
let _tgrOffset = 0; try { _tgrOffset = parseInt(pubFlag('telegram_recv_offset', '0'), 10) || 0; } catch { _tgrOffset = 0; }
let _tgrRodando = false;
async function tickTgReceiver(): Promise<void> {
  if (pubFlag('telegram_receiver_ativo', '0') !== '1') return;
  if (_tgrRodando) return; _tgrRodando = true;
  try {
    const token = _tgrToken(); if (!token) return;
    const r = await fetch(`${_tgrApiBase()}/bot${token}/getUpdates?offset=${_tgrOffset + 1}&timeout=20&allowed_updates=${encodeURIComponent('["message","callback_query"]')}`);
    const j: any = await r.json();
    if (j && j.ok && Array.isArray(j.result)) {
      for (const upd of j.result) {
        if (upd.update_id <= _tgrOffset) continue;
        _tgrOffset = Math.max(_tgrOffset, upd.update_id);
        try { setPubFlag('telegram_recv_offset', String(_tgrOffset)); } catch { /* */ }
        try { await _tgrProcessar(upd); } catch { /* */ }
      }
    }
  } catch { /* */ } finally { _tgrRodando = false; }
}
setInterval(() => { tickTgReceiver().catch(() => { }); }, 3000);
app.post('/pub/telegram-receiver/config', (req, res) => {
  try { const b = req.body || {}; if (b.ativo != null) setPubFlag('telegram_receiver_ativo', b.ativo ? '1' : '0'); res.json({ ok: true, ativo: pubFlag('telegram_receiver_ativo', '0') === '1' }); }
  catch (e: any) { res.json({ ok: false, erro: String((e && e.message) || e) }); }
});
// liga/desliga o SERVIDOR DE 2GB (bot "muda de casa"). Ao ligar, faz logOut no Telegram cloud pra o bot migrar pro servidor local.
app.post('/pub/telegram-receiver/local-api', async (req, res) => {
  try {
    const b = req.body || {};
    const token = _tgrToken(); if (!token) return res.json({ ok: false, erro: 'sem token do Telegram configurado' });
    if (b.ativo) {
      let logout = 'nao-tentado';
      try { const r = await fetch(`${TGR_API}/bot${token}/logOut`, { method: 'POST' }); const jj: any = await r.json(); logout = JSON.stringify(jj); } catch (e: any) { logout = 'erro: ' + String((e && e.message) || e); }
      setPubFlag('telegram_local_api_base', 'http://telegram-bot-api:8081');
      return res.json({ ok: true, ativo: true, base: 'http://telegram-bot-api:8081', logout });
    }
    setPubFlag('telegram_local_api_base', '');
    res.json({ ok: true, ativo: false, base: '' });
  } catch (e: any) { res.json({ ok: false, erro: String((e && e.message) || e) }); }
});
app.get('/pub/telegram-receiver/status', (_req, res) => {
  res.json({ ok: true, ativo: pubFlag('telegram_receiver_ativo', '0') === '1', tem_token: !!pubFlag('telegram_bot_token_enc', ''), offset: _tgrOffset, local_api: _tgrApiBase() !== TGR_API, base: _tgrApiBase() });
});

// [+tg-diagnostico] testa AO VIVO o que falta pra postagem pelo Telegram funcionar (pra usar na hora do login/deploy)
app.get('/pub/telegram-receiver/diagnostico', async (_req, res) => {
  const token = _tgrToken();
  const base = _tgrApiBase();
  const usandoLocal = base !== TGR_API;
  const d: any = {
    tem_token: !!token,
    receiver_ativo: pubFlag('telegram_receiver_ativo', '0') === '1',
    base,
    usando_servidor_2gb: usandoLocal,
    offset: _tgrOffset,
  };
  if (token) {
    // getMe na base atual (se local, confirma que o servidor de 2GB responde e o bot migrou)
    try {
      const r = await fetch(`${base}/bot${token}/getMe`);
      const j: any = await r.json().catch(() => ({}));
      d.getMe = j && j.ok ? { ok: true, username: j.result && j.result.username } : { ok: false, status: r.status, erro: (j && j.description) || 'falhou' };
      d.base_responde = r.ok;
    } catch (e: any) { d.getMe = { ok: false, erro: String((e && e.message) || e) }; d.base_responde = false; }
    if (usandoLocal) {
      // na nuvem o bot deve estar DESLOGADO (logOut) — se ainda responder ok, ele não migrou pro servidor local
      try { const rc = await fetch(`${TGR_API}/bot${token}/getMe`); const jc: any = await rc.json().catch(() => ({})); d.migrou_pro_local = !(jc && jc.ok); d.nuvem_getMe_ok = !!(jc && jc.ok); }
      catch { d.migrou_pro_local = true; }
    }
  }
  // dica automática do que resolver
  const dicas: string[] = [];
  if (!d.tem_token) dicas.push('Falta o token do robô do Telegram (configure em /pub/telegram-config).');
  if (d.tem_token && !d.receiver_ativo) dicas.push('O recebedor está DESLIGADO — ligue (ativo:true) pra ele ouvir o Telegram.');
  if (d.tem_token && usandoLocal && d.base_responde === false) dicas.push('O servidor de 2GB (telegram-bot-api) não respondeu — suba com: docker compose --profile tgbig up -d (precisa de TELEGRAM_API_ID/TELEGRAM_API_HASH no .env).');
  if (d.tem_token && usandoLocal && d.nuvem_getMe_ok) dicas.push('O bot ainda está na nuvem do Telegram — o logOut não pegou; desligue e ligue o servidor de 2GB de novo pra forçar a migração.');
  if (d.tem_token && !usandoLocal) dicas.push('Sem servidor de 2GB: vídeos acima de ~20MB não baixam. Ligue o 2GB (/pub/telegram-receiver/local-api ativo:true) pra subir até 2GB.');
  d.dicas = dicas;
  res.json({ ok: true, diagnostico: d });
});



// ===================================================================================
// LOTE A — ENDPOINTS NOVOS DO ROADMAP GRANDE (ADITIVO, inserido antes do app.listen).
// Reaproveita: db, app, agora(), randomUUID. Tudo em tabelas NOVAS (não toca no que existe).
// Frentes: #6 disponibilidade, #9 VSL, #13 convites, #4/#10 academy, #14 testes/captacao/diagnostico,
//          #2 provedor de IA, #7 áudio (fila), #8 editor (fila), #5 presets.
// Workers reais (Demucs/editor/LLM) e dedup/CRM em massa ficam pra lotes seguintes (com Ezequias).
// ===================================================================================
const _ndOk = (res: any, dados: any) => res.json({ ok: true, dados });
const _ndErr = (res: any, e: any) => res.status(500).json({ ok: false, erro: String(e?.message || e) });
const _ndJson = (v: any, fb: any) => { try { return v ? JSON.parse(v) : fb; } catch { return fb; } };

db.exec(`CREATE TABLE IF NOT EXISTS disponibilidade_janelas (
  id TEXT PRIMARY KEY, projeto TEXT, dia_semana INTEGER, hora TEXT, tipo TEXT, vagas INTEGER, ativo INTEGER DEFAULT 1, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS vsl_videos (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, fonte TEXT, ref TEXT, oferta_segundos INTEGER, barra TEXT,
  dominios TEXT, marcadagua INTEGER DEFAULT 1, plays INTEGER DEFAULT 0, retencao_media REAL, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS vsl_eventos (
  id TEXT PRIMARY KEY, vsl_id TEXT, contato_id TEXT, sessao TEXT, tipo TEXT, posicao_seg INTEGER, pct INTEGER, utm TEXT, ts TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS convites (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, produto TEXT, tipo TEXT, destino TEXT, webinarioId TEXT,
  imagem TEXT, videoVslId TEXT, cor TEXT, ia TEXT, pos TEXT, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS convite_respostas (
  id TEXT PRIMARY KEY, convite_id TEXT, etapa TEXT, dados TEXT, agendou INTEGER DEFAULT 0, compareceu INTEGER DEFAULT 0, ts TEXT
)`);
// [#13 B] colunas add-only: tag configurável do agendado + texto da mensagem de confirmação
try { db.exec(`ALTER TABLE convites ADD COLUMN tag TEXT`); } catch { /* coluna já existe */ }
try { db.exec(`ALTER TABLE convites ADD COLUMN msg_confirmacao TEXT`); } catch { /* coluna já existe */ }
db.exec(`CREATE TABLE IF NOT EXISTS academy_cursos (
  id TEXT PRIMARY KEY, nome TEXT, descricao TEXT, produto TEXT, publicado INTEGER DEFAULT 0, modulos INTEGER DEFAULT 0, aulas INTEGER DEFAULT 0, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_modulos ( id TEXT PRIMARY KEY, curso_id TEXT, nome TEXT, ordem INTEGER )`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_aulas ( id TEXT PRIMARY KEY, modulo_id TEXT, nome TEXT, video_fonte TEXT, video_ref TEXT, ordem INTEGER )`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_matriculas (
  id TEXT PRIMARY KEY, contato_id TEXT, nome TEXT, email TEXT, curso TEXT, origem TEXT, ativo INTEGER DEFAULT 1, ultimo_acesso TEXT, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_ofertas ( id TEXT PRIMARY KEY, nome TEXT, tipo TEXT, valor REAL )`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_vendas ( id TEXT PRIMARY KEY, aluno TEXT, produto TEXT, valor REAL, status TEXT, data TEXT )`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_config (
  id INTEGER PRIMARY KEY, dominio TEXT, certificado INTEGER DEFAULT 1, termo TEXT, exigir_termo INTEGER DEFAULT 1,
  yt_mascarado INTEGER DEFAULT 1, trava_dominio INTEGER DEFAULT 1, marcadagua INTEGER DEFAULT 1
)`);
db.exec(`CREATE TABLE IF NOT EXISTS academy_atendimento (
  id TEXT PRIMARY KEY, aluno TEXT, email TEXT, canal TEXT, assunto TEXT, ultima TEXT, nao_lidas INTEGER DEFAULT 0, mensagens TEXT, ts TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS paginas_testes (
  id TEXT PRIMARY KEY, projeto TEXT, nome TEXT, pagina TEXT, testar TEXT, metrica TEXT, status TEXT,
  significancia REAL, variantes TEXT, criado_em TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS capi_alertas ( projeto TEXT PRIMARY KEY, push INTEGER DEFAULT 1, email INTEGER DEFAULT 1, telegram INTEGER DEFAULT 1 )`);
db.exec(`CREATE TABLE IF NOT EXISTS agente_provedor ( agente TEXT PRIMARY KEY, base_url TEXT, modelo TEXT, chave_ref TEXT )`);
db.exec(`CREATE TABLE IF NOT EXISTS audio_jobs (
  id TEXT PRIMARY KEY, nome TEXT, tamanho INTEGER, status TEXT, progresso INTEGER DEFAULT 0, stems TEXT, tom TEXT, bpm INTEGER, cifra TEXT, criado_em TEXT
)`);

// ---------- #6 Disponibilidade ----------
app.get('/disponibilidade', (req: any, res: any) => {
  try {
    const projeto = String(req.query.projeto || '');
    const janelas = db.prepare(`SELECT * FROM disponibilidade_janelas WHERE (?='' OR projeto=?) ORDER BY dia_semana, hora`).all(projeto, projeto);
    _ndOk(res, { janelas });
  } catch (e) { _ndErr(res, e); }
});
app.post('/disponibilidade', (req: any, res: any) => {
  try {
    const b = req.body || {};
    if (b.id) {
      db.prepare(`UPDATE disponibilidade_janelas SET projeto=?, dia_semana=?, hora=?, tipo=?, vagas=?, ativo=? WHERE id=?`)
        .run(b.projeto, b.dia_semana, b.hora, b.tipo, b.vagas, b.ativo ? 1 : 0, b.id);
      return _ndOk(res, { id: b.id });
    }
    const id = randomUUID();
    db.prepare(`INSERT INTO disponibilidade_janelas (id,projeto,dia_semana,hora,tipo,vagas,ativo,criado_em) VALUES (?,?,?,?,?,?,?,?)`)
      .run(id, b.projeto, b.dia_semana, b.hora, b.tipo, b.vagas, b.ativo ? 1 : 0, agora());
    _ndOk(res, { id });
  } catch (e) { _ndErr(res, e); }
});
app.post('/disponibilidade/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM disponibilidade_janelas WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});

// ---------- #9 Players / VSL ----------
// [#9 VSL] colunas do CTA (oferta "os dois": botão no player + libera conteúdo da página). Guarded (add-only).
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN cta_texto TEXT`); } catch { /* coluna já existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN cta_link TEXT`); } catch { /* coluna já existe */ }
// [#9 VSL camada1] experiência do player (VTurb completo)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN formato TEXT`); } catch { /* existe */ }      // 16:9 | 9:16 | 1:1
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN cor TEXT`); } catch { /* existe */ }           // cor da barra/botão (hex)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN inicio TEXT`); } catch { /* existe */ }        // click | mudo_som | preroll
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN pausa_titulo TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN pausa_texto TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN pausa_botao TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN poster TEXT`); } catch { /* existe */ }        // URL da capa
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN resume INTEGER`); } catch { /* existe */ }     // continuar de onde parou (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN protecao INTEGER`); } catch { /* existe */ }   // anti-download básico (0/1)
// [#9 VSL camada2] tarja + escassez + CTA inteligente + tag/fluxo por marco
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_texto TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_pos TEXT`); } catch { /* existe */ }     // cima | baixo
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_fonte TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_bg TEXT`); } catch { /* existe */ }       // cor de fundo
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_cor TEXT`); } catch { /* existe */ }      // cor do texto
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_largura TEXT`); } catch { /* existe */ }  // ex.: 100% ou 600px
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN tarja_passando INTEGER`); } catch { /* existe */ } // letreiro rolando (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN escassez_segundos INTEGER`); } catch { /* existe */ } // contador após a oferta (0=off)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN escassez_texto TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN cta_utm INTEGER`); } catch { /* existe */ }     // passa UTM+contato pro link do CTA (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN marco_tag TEXT`); } catch { /* existe */ }      // tag aplicada no marco (precisa ?c= do contato)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN marco_evento TEXT`); } catch { /* existe */ }   // oferta | fim | play
// [#9 VSL camada3] prova social + gate de e-mail/WhatsApp + teste A/B
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN prova_on INTEGER`); } catch { /* existe */ }       // liga pop-ups de prova social (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN prova_texto TEXT`); } catch { /* existe */ }       // 1 mensagem por linha
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN prova_intervalo INTEGER`); } catch { /* existe */ } // seg entre pop-ups (default 25)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_on INTEGER`); } catch { /* existe */ }         // liga o gate de contato (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_momento INTEGER`); } catch { /* existe */ }    // seg do vídeo p/ pedir o contato (0 = antes de começar)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_titulo TEXT`); } catch { /* existe */ }
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_campos TEXT`); } catch { /* existe */ }        // email | whatsapp | ambos
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_obrigatorio INTEGER`); } catch { /* existe */ } // trava o vídeo até preencher (0/1)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN gate_tag TEXT`); } catch { /* existe */ }           // tag aplicada no lead capturado (opcional)
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN ab_par TEXT`); } catch { /* existe */ }             // id do vídeo B (variante) do teste A/B
try { db.exec(`ALTER TABLE vsl_videos ADD COLUMN ab_peso INTEGER`); } catch { /* existe */ }         // % do tráfego pra ESTE vídeo (A); resto vai pro B (default 50)
db.exec(`CREATE TABLE IF NOT EXISTS vsl_leads (id TEXT PRIMARY KEY, vsl_id TEXT, projeto TEXT, email TEXT, whatsapp TEXT, sessao TEXT, contato_id TEXT, utm TEXT, ts TEXT)`);
app.get('/vsl/videos', (req: any, res: any) => {
  try {
    const projeto = String(req.query.projeto || '');
    const videos = db.prepare(`SELECT * FROM vsl_videos WHERE (?='' OR projeto=?) ORDER BY criado_em DESC`).all(projeto, projeto);
    _ndOk(res, { videos });
  } catch (e) { _ndErr(res, e); }
});
app.post('/vsl/video', (req: any, res: any) => {
  try {
    const b = req.body || {}; const id = b.id || randomUUID();
    db.prepare(`INSERT OR REPLACE INTO vsl_videos (id,projeto,nome,fonte,ref,oferta_segundos,barra,dominios,marcadagua,cta_texto,cta_link,formato,cor,inicio,pausa_titulo,pausa_texto,pausa_botao,poster,resume,protecao,tarja_texto,tarja_pos,tarja_fonte,tarja_bg,tarja_cor,tarja_largura,tarja_passando,escassez_segundos,escassez_texto,cta_utm,marco_tag,marco_evento,prova_on,prova_texto,prova_intervalo,gate_on,gate_momento,gate_titulo,gate_campos,gate_obrigatorio,gate_tag,ab_par,ab_peso,plays,retencao_media,criado_em)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT plays FROM vsl_videos WHERE id=?),0),(SELECT retencao_media FROM vsl_videos WHERE id=?),COALESCE((SELECT criado_em FROM vsl_videos WHERE id=?),?))`)
      .run(id, b.projeto, b.nome, b.fonte, b.ref, b.oferta_segundos, b.barra, b.dominios, b.marcadagua ? 1 : 0,
        (b.cta_texto != null ? String(b.cta_texto) : null), (b.cta_link != null ? String(b.cta_link) : null),
        (b.formato != null ? String(b.formato) : null), (b.cor != null ? String(b.cor) : null), (b.inicio != null ? String(b.inicio) : null),
        (b.pausa_titulo != null ? String(b.pausa_titulo) : null), (b.pausa_texto != null ? String(b.pausa_texto) : null), (b.pausa_botao != null ? String(b.pausa_botao) : null),
        (b.poster != null ? String(b.poster) : null), (b.resume ? 1 : 0), (b.protecao ? 1 : 0),
        (b.tarja_texto != null ? String(b.tarja_texto) : null), (b.tarja_pos != null ? String(b.tarja_pos) : null),
        (b.tarja_fonte != null ? String(b.tarja_fonte) : null), (b.tarja_bg != null ? String(b.tarja_bg) : null),
        (b.tarja_cor != null ? String(b.tarja_cor) : null), (b.tarja_largura != null ? String(b.tarja_largura) : null),
        (b.tarja_passando ? 1 : 0), (b.escassez_segundos != null ? (Number(b.escassez_segundos) || 0) : null),
        (b.escassez_texto != null ? String(b.escassez_texto) : null), (b.cta_utm ? 1 : 0),
        (b.marco_tag != null ? String(b.marco_tag) : null), (b.marco_evento != null ? String(b.marco_evento) : null),
        (b.prova_on ? 1 : 0), (b.prova_texto != null ? String(b.prova_texto) : null), (b.prova_intervalo != null ? (Number(b.prova_intervalo) || 0) : null),
        (b.gate_on ? 1 : 0), (b.gate_momento != null ? (Number(b.gate_momento) || 0) : null), (b.gate_titulo != null ? String(b.gate_titulo) : null),
        (b.gate_campos != null ? String(b.gate_campos) : null), (b.gate_obrigatorio ? 1 : 0), (b.gate_tag != null ? String(b.gate_tag) : null),
        (b.ab_par != null ? String(b.ab_par) : null), (b.ab_peso != null ? (Number(b.ab_peso) || 0) : null),
        id, id, id, agora());
    // [#9 VSL upload] pré-baixa o vídeo próprio do OneDrive em 2º plano (igual às Lives), pro player já
    // ter a mídia pronta na 1ª visita. Só pra fonte 'storage' com ref RELATIVO (não http). Não trava o salvar.
    try {
      const ref = b.ref != null ? String(b.ref).trim() : '';
      if (String(b.fonte || '') === 'storage' && ref && !/^https?:\/\//i.test(ref)) {
        _odBaixarGrande(ref).then(() => { /* pronto */ }).catch(() => { /* tenta de novo no 1º acesso */ });
      }
    } catch { /* */ }
    _ndOk(res, { id });
  } catch (e) { _ndErr(res, e); }
});
// [#9 VSL upload] pré-aquecer (baixar agora) o vídeo próprio sob demanda — botão opcional no app.
app.post('/vsl/media/preaquecer', async (req: any, res: any) => {
  try {
    const b = req.body || {};
    const v = db.prepare(`SELECT fonte, ref FROM vsl_videos WHERE id=?`).get(b.id) as any;
    const ref = v && v.ref ? String(v.ref).trim() : '';
    if (!ref) return _ndErr(res, new Error('sem_video'));
    if (/^https?:\/\//i.test(ref)) return _ndOk(res, { pronto: true, tipo: 'url' });
    const dest = _odDestPath(ref);
    try { if (fs.existsSync(dest) && fs.statSync(dest).size > 0) return _ndOk(res, { pronto: true, tipo: 'cache' }); } catch { /* */ }
    _odBaixarGrande(ref).then(() => { /* */ }).catch(() => { /* */ });
    _ndOk(res, { pronto: false, baixando: true });
  } catch (e) { _ndErr(res, e); }
});
app.post('/vsl/video/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM vsl_videos WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});
app.get('/vsl/retencao/:id', (req: any, res: any) => {
  try {
    const rows = db.prepare(`SELECT pct FROM vsl_eventos WHERE vsl_id=? AND pct IS NOT NULL`).all(req.params.id) as any[];
    const curva: number[] = [];
    for (let faixa = 0; faixa < 20; faixa++) {
      const lo = faixa * 5, hi = lo + 5;
      const n = rows.filter((r) => Number(r.pct) >= lo).length;
      curva.push(rows.length ? Math.round((n / rows.length) * 100) : 0);
    }
    _ndOk(res, { curva: rows.length ? curva : [] });
  } catch (e) { _ndErr(res, e); }
});
app.post('/vsl/evento', (req: any, res: any) => {
  try {
    const b = req.body || {};
    db.prepare(`INSERT INTO vsl_eventos (id,vsl_id,contato_id,sessao,tipo,posicao_seg,pct,utm,ts) VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(randomUUID(), b.vsl_id, b.contato_id || null, b.sessao || null, b.tipo, b.posicao_seg || null, b.pct || null, b.utm || null, agora());
    // [#9 VSL] conta 1 play quando o vídeo começa; atualiza a retenção média (média do maior pct por sessão).
    try {
      if (b.tipo === 'play' && b.vsl_id) db.prepare(`UPDATE vsl_videos SET plays=COALESCE(plays,0)+1 WHERE id=?`).run(b.vsl_id);
      if ((b.tipo === 'fim' || b.tipo === 'progress') && b.vsl_id) {
        const r = db.prepare(`SELECT AVG(m) a FROM (SELECT MAX(pct) m FROM vsl_eventos WHERE vsl_id=? AND pct IS NOT NULL GROUP BY sessao)`).get(b.vsl_id) as any;
        if (r && r.a != null) db.prepare(`UPDATE vsl_videos SET retencao_media=? WHERE id=?`).run(Math.round(Number(r.a)), b.vsl_id);
      }
    } catch { /* métricas best-effort */ }
    // [#9 VSL camada2] tag + fluxo por MARCO: no evento configurado (padrão 'oferta'), se veio o contato (?c=),
    // aplica a tag e dispara as automações (rodarAutomacoes tag_adicionada reusa grupo/webinário/sequência).
    try {
      if (b.vsl_id && b.contato_id) {
        const vv = db.prepare(`SELECT projeto, marco_tag, marco_evento FROM vsl_videos WHERE id=?`).get(b.vsl_id) as any;
        const mev = (vv && vv.marco_evento) ? String(vv.marco_evento) : 'oferta';
        if (vv && vv.marco_tag && String(b.tipo) === mev) {
          aplicarTag(String(vv.projeto || ''), String(b.contato_id), String(vv.marco_tag));
          rodarAutomacoes(String(vv.projeto || ''), 'tag_adicionada', { contatoId: String(b.contato_id), tag: String(vv.marco_tag) }).catch(() => {});
        }
      }
    } catch { /* marco best-effort */ }
    // [#9 VSL camada2 batch B] cada marco vira evento pro Meta (CAPI) + Google (GA4) — remarketing/otimização.
    // play->ViewContent, oferta->AddToCart, cta_click->InitiateCheckout. Inerte até vsl_capi_ativo=1.
    try {
      const mapaEv: Record<string, string> = { play: 'ViewContent', oferta: 'AddToCart', cta_click: 'InitiateCheckout' };
      const evName = mapaEv[String(b.tipo)];
      if (evName && b.vsl_id && vslTrackHook && pubFlag('vsl_capi_ativo', '0') === '1') {
        const vv2 = db.prepare(`SELECT projeto FROM vsl_videos WHERE id=?`).get(b.vsl_id) as any;
        if (vv2 && vv2.projeto) {
          let em: string | null = null, tel: string | null = null;
          if (b.contato_id) { try { const ct = db.prepare(`SELECT email, telefone FROM contatos WHERE id=?`).get(String(b.contato_id)) as any; if (ct) { em = ct.email || null; tel = ct.telefone || null; } } catch { /* */ } }
          vslTrackHook(String(vv2.projeto), evName, { email: em, telefone: tel, pagina: null }).catch(() => { /* */ });
        }
      }
    } catch { /* capi marco best-effort */ }
    _ndOk(res, { registrado: true });
  } catch (e) { _ndErr(res, e); }
});
// [#9 VSL camada2 batch B] liga/desliga o envio dos marcos do player pro Meta/Google (default OFF).
app.get('/vsl/capi/status', (_req: any, res: any) => { try { _ndOk(res, { ativo: pubFlag('vsl_capi_ativo', '0') === '1' }); } catch (e) { _ndErr(res, e); } });
app.post('/vsl/capi/flag', (req: any, res: any) => { try { setPubFlag('vsl_capi_ativo', (req.body || {}).ativo ? '1' : '0'); _ndOk(res, { ativo: pubFlag('vsl_capi_ativo', '0') === '1' }); } catch (e) { _ndErr(res, e); } });

// [#9 VSL camada3] GATE: o player manda o e-mail/WhatsApp capturado. Grava em vsl_leads e, se tiver WhatsApp,
// cria/acha o contato no projeto do vídeo e aplica a tag (gate_tag ou 'lead-vsl') + dispara automações.
app.post('/vsl/lead', (req: any, res: any) => {
  try {
    const b = req.body || {};
    if (!b.vsl_id || (!b.email && !b.whatsapp)) return res.json({ ok: false, erro: 'faltou_vsl_id_ou_contato' });
    const vv = db.prepare(`SELECT projeto, gate_tag FROM vsl_videos WHERE id=?`).get(b.vsl_id) as any;
    const projeto = vv && vv.projeto ? String(vv.projeto) : '';
    const email = b.email != null ? String(b.email).trim().slice(0, 160) : '';
    const fone = b.whatsapp != null ? String(b.whatsapp).replace(/\D/g, '').slice(0, 15) : '';
    let contatoId: string | null = null;
    if (fone && projeto) {
      try {
        contatoId = upsertContato(projeto, fone, undefined, email || undefined, 'vsl');
        const tag = (vv && vv.gate_tag && String(vv.gate_tag).trim()) ? String(vv.gate_tag).trim() : 'lead-vsl';
        aplicarTag(projeto, contatoId, tag);
        rodarAutomacoes(projeto, 'tag_adicionada', { contatoId, tag, telefone: fone }).catch(() => { /* */ });
      } catch { /* captura best-effort: nunca quebra o player */ }
    }
    db.prepare(`INSERT INTO vsl_leads (id,vsl_id,projeto,email,whatsapp,sessao,contato_id,utm,ts) VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(randomUUID(), String(b.vsl_id), projeto, email || null, fone || null, b.sessao || null, contatoId, b.utm || null, agora());
    res.json({ ok: true, contato_id: contatoId });
  } catch (e: any) { res.json({ ok: false, erro: String(e?.message || e) }); }
});

// [#9 VSL camada2] RELATÓRIO: plays, % que chega na oferta, cliques no CTA, CTR, tempo médio, por UTM.
app.get('/vsl/relatorio/:id', (req: any, res: any) => {
  try {
    const id = req.params.id;
    const plays = (db.prepare(`SELECT COUNT(*) n FROM vsl_eventos WHERE vsl_id=? AND tipo='play'`).get(id) as any).n || 0;
    const chegouOferta = (db.prepare(`SELECT COUNT(DISTINCT sessao) n FROM vsl_eventos WHERE vsl_id=? AND tipo='oferta'`).get(id) as any).n || 0;
    const cliques = (db.prepare(`SELECT COUNT(*) n FROM vsl_eventos WHERE vsl_id=? AND tipo='cta_click'`).get(id) as any).n || 0;
    const tm = (db.prepare(`SELECT AVG(m) a FROM (SELECT MAX(posicao_seg) m FROM vsl_eventos WHERE vsl_id=? AND posicao_seg IS NOT NULL GROUP BY sessao)`).get(id) as any).a;
    const leads = (db.prepare(`SELECT COUNT(*) n FROM vsl_leads WHERE vsl_id=?`).get(id) as any).n || 0; // [#9 VSL camada3] gate
    const porUtm = db.prepare(`SELECT COALESCE(NULLIF(utm,''),'(sem utm)') utm, COUNT(DISTINCT sessao) sessoes,
        SUM(CASE WHEN tipo='play' THEN 1 ELSE 0 END) plays, SUM(CASE WHEN tipo='oferta' THEN 1 ELSE 0 END) ofertas,
        SUM(CASE WHEN tipo='cta_click' THEN 1 ELSE 0 END) cliques
      FROM vsl_eventos WHERE vsl_id=? GROUP BY COALESCE(NULLIF(utm,''),'(sem utm)') ORDER BY sessoes DESC LIMIT 30`).all(id);
    _ndOk(res, { plays, chegouOferta, cliques, leads, ctr: plays ? Math.round(cliques / plays * 100) : 0, taxaOferta: plays ? Math.round(chegouOferta / plays * 100) : 0, tempoMedioSeg: tm != null ? Math.round(Number(tm)) : 0, porUtm });
  } catch (e) { _ndErr(res, e); }
});

// [#9 VSL camada1] JS do player VTurb completo (estático; roda no navegador). SEM backtick e SEM ${} aqui dentro.
const PLAYER_VSL_JS = `
(function(){
  var CFG=window.CFG||{};
  var sent={}, sess=null, counted=false, modoReal=false, somAtivo=false, ended=false;
  try{ sess=sessionStorage.getItem('vslsess_'+CFG.id); }catch(e){}
  if(!sess){ sess=Date.now().toString(36)+Math.random().toString(36).slice(2,8); try{sessionStorage.setItem('vslsess_'+CFG.id,sess);}catch(e){} }
  var qs=new URLSearchParams(location.search);
  var contato=qs.get('c')||qs.get('contato')||'';
  var utm=location.search?location.search.replace(/^[?]/,''):'';
  function hostOf(u){ try{return new URL(u).hostname.toLowerCase();}catch(e){return '';} }
  if(CFG.dominios&&CFG.dominios.length){ var h=hostOf(document.referrer||''); if(h){ var ok=false; for(var i=0;i<CFG.dominios.length;i++){ var d=CFG.dominios[i]; if(d&&(h===d||h.indexOf('.'+d)>=0||h===('www.'+d)))ok=true; } if(!ok){ var b=document.getElementById('blk'); if(b)b.style.display='flex'; return; } } }
  function ev(tipo,pct,pos){ try{ var body=JSON.stringify({vsl_id:CFG.id,sessao:sess,contato_id:contato||null,tipo:tipo,pct:(pct!=null?pct:null),posicao_seg:(pos!=null?Math.round(pos):null),utm:utm}); if(navigator.sendBeacon){ navigator.sendBeacon('/vsl/evento', new Blob([body],{type:'application/json'})); } else { fetch('/vsl/evento',{method:'POST',headers:{'Content-Type':'application/json'},body:body,keepalive:true}); } }catch(e){} }
  var dur=0,cur=0;
  var barf=document.getElementById('barf'),bar=document.getElementById('bar'),big=document.getElementById('big'),wm=document.getElementById('wm'),cta=document.getElementById('cta'),poster=document.getElementById('poster'),sound=document.getElementById('sound'),pausaBox=document.getElementById('pausa');
  var escBox=document.getElementById('escassez'),escTime=document.getElementById('escTime');
  var gEl=document.getElementById('gate'); var gateDone=false, gateShownInline=false, provaTimer=null;
  var ctaA=cta?cta.querySelector('a'):null;
  if(ctaA){ if(CFG.ctaUtm){ var ex=location.search?location.search.replace(/^[?]/,''):''; if(ex){ ctaA.href += (ctaA.href.indexOf('?')>=0?'&':'?')+ex; } } ctaA.addEventListener('click',function(){ ev('cta_click',null,cur); }); }
  var escIniciado=false;
  function startEscassez(){ if(escIniciado||!CFG.escassez||CFG.escassez<=0||!escBox)return; escIniciado=true; escBox.style.display='flex'; var left=CFG.escassez; function tk(){ var mm=Math.floor(left/60),ss=left%60; if(escTime)escTime.textContent=mm+':'+(ss<10?'0':'')+ss; if(left<=0)return; left--; setTimeout(tk,1000); } tk(); }
  function startProva(){ if(!CFG.provaOn||!CFG.provaMsgs||!CFG.provaMsgs.length)return; var box=document.getElementById('prova'); if(!box||provaTimer)return;
    function mostra(){ var m=CFG.provaMsgs[Math.floor(Math.random()*CFG.provaMsgs.length)]; var d=document.createElement('div'); d.className='pv'; d.textContent=m; box.innerHTML=''; box.appendChild(d); box.style.display='block'; setTimeout(function(){ box.style.display='none'; },5000); }
    setTimeout(mostra,4000); provaTimer=setInterval(mostra, Math.max(8,CFG.provaInt||25)*1000); }
  function mostrarGate(){ if(!CFG.gateOn||gateDone||!gEl)return; try{ api.pause(); }catch(e){} gEl.style.display='flex'; }
  function fecharGate(){ gateDone=true; if(gEl)gEl.style.display='none'; }
  function enviarGate(){ var ie=document.getElementById('gEmail'),iz=document.getElementById('gZap'),er=document.getElementById('gErr');
    var em=ie?(ie.value||'').trim():''; var zap=iz?(iz.value||'').replace(/\\D/g,''):''; if(er)er.textContent='';
    if(!em&&!zap){ if(er)er.textContent='Preencha pra continuar.'; return; }
    if(em&&em.indexOf('@')<1){ if(er)er.textContent='E-mail inválido.'; return; }
    if(!em&&zap&&zap.length<10){ if(er)er.textContent='WhatsApp com DDD, por favor.'; return; }
    try{ var body=JSON.stringify({vsl_id:CFG.id,email:em||null,whatsapp:zap||null,sessao:sess,contato_id:contato||null,utm:utm}); fetch('/vsl/lead',{method:'POST',headers:{'Content-Type':'application/json'},body:body,keepalive:true}); }catch(e){}
    fecharGate(); if(!modoReal){ usuarioClicou(); } else { try{ api.play(); }catch(e){} } }
  function displayPct(x){ if(CFG.barra==='inteligente'){ return Math.round((1-Math.pow(1-x,1.8))*100); } return Math.round(x*100); }
  function onTick(){
    if(!modoReal) return;
    if(dur>0){ var x=Math.min(1,cur/dur); if(barf)barf.style.width=displayPct(x)+'%';
      var rp=Math.round(x*100); for(var m=10;m<=90;m+=10){ if(rp>=m&&!sent['p'+m]){ sent['p'+m]=1; ev('progress',rp,cur); } }
      try{ if(CFG.resume&&CFG.inicio==='click') localStorage.setItem('vslpos_'+CFG.id,String(Math.floor(cur))); }catch(e){}
    }
    if(CFG.oferta>0&&cur>=CFG.oferta&&!sent.oferta){ sent.oferta=1; if(cta)cta.style.display='flex'; startEscassez(); try{parent.postMessage({type:'vsl-oferta',id:CFG.id},'*');}catch(e){} ev('oferta',null,cur); }
    if(CFG.gateOn&&CFG.gateMomento>0&&!gateDone&&!gateShownInline&&cur>=CFG.gateMomento){ gateShownInline=true; mostrarGate(); }
  }
  function realStart(){ if(counted)return; counted=true; if(CFG.marcadagua&&wm)wm.textContent=(contato||'Melodias e Riffs'); ev('play',0,0); startProva();
    if(CFG.resume&&CFG.inicio==='click'){ try{ var sp=parseFloat(localStorage.getItem('vslpos_'+CFG.id)||'0'); if(sp>3) setTimeout(function(){ api.seek(sp); },400); }catch(e){} } }
  function onPlaying(){ if(poster)poster.style.display='none'; if(big)big.style.display='none'; if(pausaBox)pausaBox.style.display='none'; }
  function onPause(){ if(ended)return; if(CFG.pausaOn&&pausaBox){ pausaBox.style.display='flex'; } else if(big){ big.style.display='flex'; } }
  function onEnd(){ if(ended)return; ended=true; ev('fim',100,dur); try{ localStorage.removeItem('vslpos_'+CFG.id); }catch(e){} if(big)big.style.display='none'; }
  if(CFG.marcadagua&&wm){ wm.style.left='8%'; wm.style.top='8%'; setInterval(function(){ wm.style.left=(8+Math.random()*78)+'%'; wm.style.top=(8+Math.random()*78)+'%'; },4000); }
  if(CFG.protecao){ document.addEventListener('contextmenu',function(e){e.preventDefault();}); document.addEventListener('selectstart',function(e){e.preventDefault();}); }
  var api={ play:function(){}, pause:function(){}, mute:function(){}, unmute:function(){}, seek:function(){}, paused:function(){return true;} };
  function seekTo(p){ api.seek(p); }
  if(CFG.barra==='real'&&bar){ bar.style.height='10px'; bar.style.cursor='pointer'; bar.addEventListener('click',function(e){ if(dur>0){ var r=bar.getBoundingClientRect(); seekTo((e.clientX-r.left)/r.width*dur); } }); }
  if(CFG.fonte==='storage'){
    var elp=document.getElementById('ply'); var v=document.createElement('video'); v.src=CFG.ref; v.setAttribute('playsinline',''); v.playsInline=true; v.preload='auto'; elp.appendChild(v);
    v.addEventListener('loadedmetadata',function(){ dur=v.duration||0; });
    v.addEventListener('timeupdate',function(){ cur=v.currentTime; onTick(); });
    v.addEventListener('play',onPlaying); v.addEventListener('pause',onPause); v.addEventListener('ended',onEnd);
    api={ play:function(){var p=v.play();if(p&&p.catch)p.catch(function(){});}, pause:function(){v.pause();}, mute:function(){v.muted=true;}, unmute:function(){v.muted=false;}, seek:function(p){try{v.currentTime=p;}catch(e){}}, paused:function(){return v.paused;} };
    iniciar();
  } else {
    window.onYouTubeIframeAPIReady=function(){ var yt=new YT.Player('ply',{ videoId:CFG.ref, playerVars:{controls:0,modestbranding:1,rel:0,disablekb:1,fs:0,playsinline:1,iv_load_policy:3}, events:{ onReady:function(){ dur=yt.getDuration()||0; api={ play:function(){yt.playVideo();}, pause:function(){yt.pauseVideo();}, mute:function(){yt.mute();}, unmute:function(){yt.unMute();}, seek:function(p){yt.seekTo(p,true);}, paused:function(){return yt.getPlayerState&&yt.getPlayerState()!==YT.PlayerState.PLAYING;} }; iniciar(); setInterval(function(){ if(yt.getCurrentTime){ cur=yt.getCurrentTime(); if(!dur)dur=yt.getDuration()||0; onTick(); } },400); }, onStateChange:function(e){ if(e.data===YT.PlayerState.PLAYING){ onPlaying(); } else if(e.data===YT.PlayerState.PAUSED){ onPause(); } else if(e.data===YT.PlayerState.ENDED){ onEnd(); } } } }); };
  }
  function mostrarSom(){ if(sound)sound.style.display='flex'; }
  function esconderSom(){ if(sound)sound.style.display='none'; }
  function iniciar(){
    if(CFG.inicio==='mudo_som'){ api.mute(); api.play(); mostrarSom(); modoReal=true; realStart(); if(poster)poster.style.display='none'; if(big)big.style.display='none'; }
    else if(CFG.inicio==='preroll'){ api.mute(); api.play(); mostrarSom(); if(poster)poster.style.display='none'; if(big)big.style.display='none'; }
    else { if(poster&&CFG.poster)poster.style.display='block'; if(big)big.style.display='flex'; }
    if(CFG.gateOn&&CFG.gateMomento<=0&&!gateDone){ mostrarGate(); }
  }
  function usuarioClicou(){
    esconderSom();
    if(CFG.inicio==='preroll'&&!modoReal){ modoReal=true; api.seek(0); api.unmute(); api.play(); realStart(); return; }
    if(CFG.inicio==='mudo_som'&&!somAtivo){ somAtivo=true; api.unmute(); return; }
    if(!modoReal){ modoReal=true; realStart(); }
    if(pausaBox)pausaBox.style.display='none';
    if(api.paused()){ api.play(); } else { api.pause(); }
  }
  var g=document.getElementById('guard'); if(g)g.addEventListener('click',usuarioClicou);
  if(sound)sound.addEventListener('click',usuarioClicou);
  var pb=document.getElementById('pausaBtn'); if(pb)pb.addEventListener('click',function(e){ e.stopPropagation(); if(pausaBox)pausaBox.style.display='none'; api.play(); });
  var gBtn=document.getElementById('gBtn'); if(gBtn)gBtn.addEventListener('click',function(e){ e.stopPropagation(); enviarGate(); });
  var gSkip=document.getElementById('gSkip'); if(gSkip)gSkip.addEventListener('click',function(e){ e.stopPropagation(); fecharGate(); if(!modoReal){ usuarioClicou(); } else { try{ api.play(); }catch(err){} } });
})();
`;

// [#9 VSL upload] RESOLVEDOR do vídeo próprio (fonte 'storage'): entrega a mídia pro <video> do player.
// ref http(s) -> redireciona direto; ref RELATIVO do OneDrive -> baixa 1x pro /data/media (cache, igual às
// Lives, via rclone, SEM marca d'água e SEM YouTube) e redireciona pra /media/<hash> (que aceita Range -> seek).
// A pré-baixa já é disparada ao salvar o vídeo, então quase sempre o arquivo já está pronto aqui.
app.get('/vsl/media/:id', async (req: any, res: any) => {
  try {
    const v = db.prepare(`SELECT fonte, ref FROM vsl_videos WHERE id=?`).get(req.params.id) as any;
    const ref = v && v.ref ? String(v.ref).trim() : '';
    if (!ref) return res.status(404).type('text/plain').send('sem video');
    if (/^https?:\/\//i.test(ref)) return res.redirect(302, ref);
    const dest = _odDestPath(ref);
    const nome = dest.split('/').pop();
    try { if (fs.existsSync(dest) && fs.statSync(dest).size > 0) return res.redirect(302, '/media/' + nome); } catch { /* */ }
    await _odBaixarGrande(ref); // dedupe + atômico (.part), sem timeout curto; cacheia em /data/media
    return res.redirect(302, '/media/' + nome);
  } catch (e: any) { return res.status(502).type('text/plain').send('indisponivel'); }
});

// [#9 VSL] EMBED: <script src="/v/:id.js"> injeta o player (iframe no formato certo) na página de vendas
// e, no minuto da oferta, LIBERA os elementos com data-vsl-oferta (que o dono deixa escondidos).
app.get('/v/:id.js', (req: any, res: any) => {
  const id = String(req.params.id || '');
  const base = PUBLIC_BASE_URL || '';
  const v = db.prepare(`SELECT formato, ab_par, ab_peso FROM vsl_videos WHERE id=?`).get(id) as any;
  const fmt = v && v.formato === '9:16' ? '9/16' : v && v.formato === '1:1' ? '1/1' : '16/9';
  const maxw = fmt === '9/16' ? '420px' : fmt === '1/1' ? '620px' : '820px';
  // [#9 VSL camada3] teste A/B: se tem variante B, o embed sorteia A ou B (fixo por visitante via localStorage).
  const abB = (v && v.ab_par) ? String(v.ab_par) : '';
  const abPeso = (v && v.ab_peso != null) ? Math.max(0, Math.min(100, Number(v.ab_peso))) : 50;
  res.type('application/javascript').send(
    '(function(){var A=' + JSON.stringify(id) + ';var B=' + JSON.stringify(abB) + ';var peso=' + JSON.stringify(abPeso) + ';var base=' + JSON.stringify(base) + ';'
    + 'var id=A;if(B){try{var k="vslab_"+A;var st=localStorage.getItem(k);if(st!=="A"&&st!=="B"){st=(Math.random()*100<peso)?"A":"B";localStorage.setItem(k,st);}id=(st==="B")?B:A;}catch(e){id=A;}}'
    + 'var s=document.currentScript;var q=location.search||"";'
    + 'var w=document.createElement("div");w.style.position="relative";w.style.width="100%";w.style.maxWidth=' + JSON.stringify(maxw) + ';w.style.margin="0 auto";w.style.aspectRatio=' + JSON.stringify(fmt) + ';w.style.background="#000";'
    + 'var f=document.createElement("iframe");f.src=base+"/v/"+id+q;f.style.position="absolute";f.style.inset="0";f.style.width="100%";f.style.height="100%";f.style.border="0";f.setAttribute("allow","autoplay; fullscreen; encrypted-media");f.setAttribute("allowfullscreen","");'
    + 'w.appendChild(f);if(s&&s.parentNode){s.parentNode.insertBefore(w,s);}else{document.body.appendChild(w);}'
    + 'window.addEventListener("message",function(e){if(e.data&&e.data.type==="vsl-oferta"&&e.data.id===id){var els=document.querySelectorAll("[data-vsl-oferta]");for(var i=0;i<els.length;i++){els[i].style.display="";}}});'
    + '})();'
  );
});

// [#9 VSL] PLAYER público estilo VTurb: YouTube mascarado ou vídeo próprio (storage, sem marca/sem YouTube);
// formatos 16:9/9:16/1:1; início click/mudo-com-som/PRÉ-ROLL (reinicia no clique); barra real ou inteligente
// (rampa de velocidade); cor; trava de domínio; marca d'água; capa; continuar de onde parou; proteção; overlay de pausa; CTA.
app.get('/v/:id', (req: any, res: any) => {
  const v = db.prepare(`SELECT * FROM vsl_videos WHERE id=?`).get(req.params.id) as any;
  if (!v) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Vídeo não encontrado</h1>');
  const corSafe = /^#[0-9a-fA-F]{3,8}$/.test(String(v.cor || '')) ? String(v.cor) : '#1db954';
  const _fonte = v.fonte || 'youtube';
  // [#9 VSL upload] vídeo próprio (storage): o player busca pelo resolvedor /vsl/media/:id
  // (OneDrive→URL pública /media, sem marca d'água/sem YouTube). YouTube segue com o ref cru.
  const _refPlayer = (_fonte === 'storage') ? ('/vsl/media/' + v.id) : (v.ref || '');
  const cfg = {
    id: v.id, fonte: _fonte, ref: _refPlayer,
    oferta: Number(v.oferta_segundos || 0), barra: v.barra || 'inteligente',
    dominios: String(v.dominios || '').split(',').map((s: string) => s.trim().toLowerCase()).filter(Boolean),
    marcadagua: v.marcadagua ? 1 : 0, formato: v.formato || '16:9',
    inicio: (v.inicio === 'mudo_som' || v.inicio === 'preroll') ? v.inicio : 'click',
    poster: v.poster || '', resume: v.resume ? 1 : 0, protecao: v.protecao ? 1 : 0,
    pausaOn: !!(v.pausa_titulo || v.pausa_texto),
    escassez: Number(v.escassez_segundos || 0), ctaUtm: v.cta_utm ? 1 : 0,
    // [#9 VSL camada3] prova social + gate de contato
    provaOn: v.prova_on ? 1 : 0,
    provaMsgs: String(v.prova_texto || '').split('\n').map((s: string) => s.trim()).filter(Boolean).slice(0, 40),
    provaInt: Math.max(8, Number(v.prova_intervalo || 25)),
    gateOn: v.gate_on ? 1 : 0, gateMomento: Number(v.gate_momento || 0),
    gateCampos: (v.gate_campos === 'email' || v.gate_campos === 'whatsapp') ? v.gate_campos : 'ambos',
    gateObrig: v.gate_obrigatorio ? 1 : 0,
  };
  const cfgJson = JSON.stringify(cfg);
  const temCta = v.cta_texto && v.cta_link;
  // [#9 VSL camada2] tarja (faixa) sanitizada + escassez
  const tBg = /^#[0-9a-fA-F]{3,8}$/.test(String(v.tarja_bg || '')) ? String(v.tarja_bg) : '#111318';
  const tCor = /^#[0-9a-fA-F]{3,8}$/.test(String(v.tarja_cor || '')) ? String(v.tarja_cor) : '#ffffff';
  const tFonte = /^[a-zA-Z0-9 ,'\-]{1,60}$/.test(String(v.tarja_fonte || '')) ? String(v.tarja_fonte) : 'system-ui, Segoe UI, Arial';
  const tLarg = /^[0-9]{1,4}(px|%)$/.test(String(v.tarja_largura || '')) ? String(v.tarja_largura) : '100%';
  const tPos = v.tarja_pos === 'cima' ? 'cima' : 'baixo';
  const temTarja = !!(v.tarja_texto && String(v.tarja_texto).trim());
  const tScroll = v.tarja_passando ? 1 : 0;
  const tarjaHtml = temTarja
    ? '<div id="tarja" class="' + (tScroll ? 'scroll' : '') + '" style="' + (tPos === 'cima' ? 'top:0' : 'bottom:0') + ';background:' + tBg + ';color:' + tCor + ';font-family:' + tFonte + ';width:' + tLarg + '"><span>' + esc(String(v.tarja_texto)) + '</span></div>'
    : '';
  const temEsc = Number(v.escassez_segundos || 0) > 0;
  const escTxt = v.escassez_texto ? String(v.escassez_texto) : 'Oferta por tempo limitado:';
  const escHtml = temEsc ? '<div id="escassez"><span>' + esc(escTxt) + '</span> <b id="escTime"></b></div>' : '';
  const pTit = v.pausa_titulo ? String(v.pausa_titulo) : 'Espera! 🙏';
  const pTxt = v.pausa_texto ? String(v.pausa_texto) : 'Continue assistindo pra não perder o conteúdo.';
  const pBtn = v.pausa_botao ? String(v.pausa_botao) : '▶ Continuar assistindo';
  // [#9 VSL camada3] prova social (container) + gate de contato (overlay com formulário)
  const provaHtml = (cfg.provaOn && cfg.provaMsgs.length) ? '<div id="prova"></div>' : '';
  const gTit = v.gate_titulo ? String(v.gate_titulo) : 'Pra continuar assistindo, deixe seu contato 👇';
  const gCampos = cfg.gateCampos;
  const gateHtml = cfg.gateOn
    ? '<div id="gate"><div class="gbox"><h3>' + esc(gTit) + '</h3><div class="gerr" id="gErr"></div>'
      + ((gCampos === 'email' || gCampos === 'ambos') ? '<input id="gEmail" type="email" placeholder="Seu melhor e-mail" />' : '')
      + ((gCampos === 'whatsapp' || gCampos === 'ambos') ? '<input id="gZap" type="tel" placeholder="Seu WhatsApp com DDD" />' : '')
      + '<button id="gBtn">Liberar o vídeo ▶</button>'
      + (cfg.gateObrig ? '' : '<div id="gSkip">pular</div>')
      + '</div></div>'
    : '';
  res.type('html').send(
    '<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(v.nome || 'Vídeo') + '</title>'
    + '<style>html,body{margin:0;height:100%;background:#000;overflow:hidden;font-family:system-ui,Segoe UI,Arial}'
    + '#wrap{position:relative;width:100%;height:100%;background:#000}#ply{position:absolute;inset:0}#ply iframe,#ply video{width:100%;height:100%;display:block;border:0;background:#000;object-fit:contain}'
    + '#poster{position:absolute;inset:0;z-index:4;background:#000 center/contain no-repeat;display:none}'
    + '#guard{position:absolute;inset:0;z-index:5;cursor:pointer}'
    + '#big{position:absolute;inset:0;display:none;align-items:center;justify-content:center;z-index:6;pointer-events:none}#big div{width:86px;height:86px;border-radius:50%;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center}#big svg{width:36px;height:36px;fill:' + corSafe + ';margin-left:4px}'
    + '#sound{position:absolute;inset:0;display:none;align-items:flex-end;justify-content:center;padding-bottom:14%;z-index:9;pointer-events:none}#sound div{pointer-events:auto;background:' + corSafe + ';color:#06230f;font-weight:800;font-size:15px;padding:12px 20px;border-radius:30px;box-shadow:0 4px 16px rgba(0,0,0,.5);animation:pz 1.3s infinite}'
    + '#bar{position:absolute;left:0;right:0;bottom:0;height:6px;background:rgba(255,255,255,.18);z-index:7}#barf{height:100%;width:0;background:' + corSafe + ';transition:width .3s linear}'
    + '#wm{position:absolute;left:8%;top:8%;z-index:8;color:rgba(255,255,255,.35);font-size:13px;font-weight:600;pointer-events:none;text-shadow:0 1px 2px rgba(0,0,0,.6);transition:all .8s linear}'
    + '#cta{position:absolute;left:0;right:0;bottom:26px;display:none;justify-content:center;z-index:9;padding:0 16px}#cta a{background:' + corSafe + ';color:#06230f;font-weight:800;font-size:17px;padding:14px 26px;border-radius:10px;text-decoration:none;box-shadow:0 6px 20px rgba(0,0,0,.45);animation:pz 1.4s infinite}@keyframes pz{0%,100%{transform:scale(1)}50%{transform:scale(1.06)}}'
    + '#pausa{position:absolute;inset:0;display:none;flex-direction:column;align-items:center;justify-content:center;z-index:12;background:rgba(0,0,0,.72);text-align:center;padding:24px}#pausa h3{color:#fff;font-size:22px;margin:0 0 10px}#pausa p{color:#dcdcdc;font-size:15px;margin:0 0 18px;max-width:420px}#pausa button{background:' + corSafe + ';color:#06230f;font-weight:800;font-size:16px;border:0;padding:13px 24px;border-radius:10px;cursor:pointer}'
    + '#blk{position:absolute;inset:0;display:none;align-items:center;justify-content:center;color:#bbb;background:#000;z-index:20;text-align:center;padding:20px;font-size:15px}'
    + '#tarja{position:absolute;left:50%;transform:translateX(-50%);z-index:10;padding:7px 14px;font-size:14px;font-weight:700;text-align:center;box-sizing:border-box;max-width:100%;overflow:hidden;white-space:nowrap}#tarja.scroll span{display:inline-block;padding-left:100%;animation:mq 14s linear infinite}@keyframes mq{from{transform:translateX(0)}to{transform:translateX(-100%)}}'
    + '#escassez{position:absolute;left:0;right:0;bottom:74px;display:none;justify-content:center;z-index:9;color:#fff;font-weight:700;font-size:14px;text-shadow:0 1px 3px rgba(0,0,0,.7)}#escassez b{margin-left:6px;color:' + corSafe + '}'
    + '#prova{position:absolute;left:14px;bottom:88px;z-index:11;max-width:78%;display:none}#prova .pv{background:rgba(20,22,28,.93);color:#fff;border-left:3px solid ' + corSafe + ';border-radius:8px;padding:8px 12px;font-size:12.5px;line-height:1.35;box-shadow:0 4px 14px rgba(0,0,0,.45);animation:pvin .4s ease}@keyframes pvin{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}'
    + '#gate{position:absolute;inset:0;display:none;align-items:center;justify-content:center;z-index:15;background:rgba(0,0,0,.82);padding:20px}#gate .gbox{background:#15171d;border-radius:14px;padding:22px;max-width:360px;width:100%;text-align:center}#gate h3{color:#fff;font-size:17px;margin:0 0 14px;line-height:1.3}#gate input{width:100%;box-sizing:border-box;margin-bottom:10px;padding:12px;border-radius:8px;border:1px solid #333;background:#0e1014;color:#fff;font-size:14px}#gate button{width:100%;background:' + corSafe + ';color:#06230f;font-weight:800;font-size:15px;border:0;padding:13px;border-radius:8px;cursor:pointer}#gate #gSkip{color:#8a8f98;font-size:12px;margin-top:10px;cursor:pointer;text-decoration:underline}#gate .gerr{color:#ff8a8a;font-size:12px;margin:-4px 0 8px;min-height:14px}</style></head><body>'
    + '<div id="wrap"><div id="ply"></div>'
    + (cfg.poster ? '<div id="poster" style="background-image:url(\'' + esc(String(cfg.poster)) + '\')"></div>' : '')
    + '<div id="guard"></div><div id="big"><div><svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></div></div>'
    + '<div id="sound"><div>🔊 Toque para ativar o som</div></div>'
    + (cfg.marcadagua ? '<div id="wm"></div>' : '')
    + '<div id="bar"><div id="barf"></div></div>'
    + tarjaHtml + escHtml
    + (temCta ? '<div id="cta"><a href="' + esc(String(v.cta_link)) + '" target="_blank" rel="noopener">' + esc(String(v.cta_texto)) + '</a></div>' : '')
    + '<div id="pausa"><h3>' + esc(pTit) + '</h3><p>' + esc(pTxt) + '</p><button id="pausaBtn">' + esc(pBtn) + '</button></div>'
    + provaHtml + gateHtml
    + '<div id="blk">Este vídeo não está disponível neste site.</div></div>'
    + '<script>var CFG=' + cfgJson + ';</script>'
    + (cfg.fonte === 'storage' ? '' : '<script src="https://www.youtube.com/iframe_api"></script>')
    + '<script>' + PLAYER_VSL_JS + '</script></body></html>'
  );
});

// ---------- #13 Convites (webinário usa a rota /webinarios que já existe) ----------
app.get('/convites', (req: any, res: any) => {
  try {
    const projeto = String(req.query.projeto || '');
    const rows = db.prepare(`SELECT * FROM convites WHERE (?='' OR projeto=?) ORDER BY criado_em DESC`).all(projeto, projeto) as any[];
    const convites = rows.map((c) => {
      const abriram = (db.prepare(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=?`).get(c.id) as any).n;
      const responderam = (db.prepare(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND dados IS NOT NULL`).get(c.id) as any).n;
      const agendaram = (db.prepare(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND agendou=1`).get(c.id) as any).n;
      const compareceram = (db.prepare(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND compareceu=1`).get(c.id) as any).n;
      const link = String(PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/c/' + c.id;
      return { ...c, link, ia: _ndJson(c.ia, {}), pos: _ndJson(c.pos, {}), stats: { abriram, responderam, agendaram, compareceram } };
    });
    _ndOk(res, { convites });
  } catch (e) { _ndErr(res, e); }
});
app.post('/convite', (req: any, res: any) => {
  try {
    const b = req.body || {}; const id = b.id || randomUUID();
    db.prepare(`INSERT OR REPLACE INTO convites (id,projeto,nome,produto,tipo,destino,webinarioId,imagem,videoVslId,cor,ia,pos,tag,msg_confirmacao,criado_em)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT criado_em FROM convites WHERE id=?),?))`)
      .run(id, b.projeto, b.nome, b.produto, b.tipo, b.destino, b.webinarioId || null, b.imagem || null, b.videoVslId || null, b.cor || null,
           JSON.stringify(b.ia || {}), JSON.stringify(b.pos || {}), (b.tag || null), (b.msg_confirmacao || null), id, agora());
    _ndOk(res, { id, link: String(PUBLIC_BASE_URL || '').replace(/\/+$/, '') + '/c/' + id });
  } catch (e) { _ndErr(res, e); }
});
app.post('/convite/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM convites WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});
app.get('/convite/:id/stats', (req: any, res: any) => {
  try {
    const id = req.params.id;
    const g = (sql: string) => (db.prepare(sql).get(id) as any).n;
    _ndOk(res, {
      abriram: g(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=?`),
      responderam: g(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND dados IS NOT NULL`),
      agendaram: g(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND agendou=1`),
      compareceram: g(`SELECT COUNT(*) n FROM convite_respostas WHERE convite_id=? AND compareceu=1`),
    });
  } catch (e) { _ndErr(res, e); }
});
// IA gera perguntas/copy — stub até ligar o LLM (chave no VPS)
app.post('/convite/ia-gerar', (_req: any, res: any) => { try { _ndOk(res, { ok: false, pendente: 'LLM não configurado' }); } catch (e) { _ndErr(res, e); } });

// [#13 convite] PAGINA PUBLICA DO CONVITE + agendamento. Espelha o fluxo publico do evento
// (/e/:slug + /e-api/.../inscrever). Link de teste = PUBLIC_BASE_URL + /c/:id — o Ezequias
// clica e abre no navegador, igual /w/ (webinario) e /v/ (VSL). SOMENTE ADITIVO.
const convGet = (id: string): any => db.prepare(`SELECT * FROM convites WHERE id=?`).get(String(id)) as any;
function convHorarios(c: any): { valor: string; rotulo: string }[] {
  const DIAS = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
  try {
    if (c.destino === 'webinario') return [{ valor: 'webinario:' + (c.webinarioId || ''), rotulo: 'Assistir agora (sala automática)' }];
    const js = db.prepare(`SELECT * FROM disponibilidade_janelas WHERE (?='' OR projeto=?) AND (ativo=1 OR ativo IS NULL) ORDER BY dia_semana, hora`).all(String(c.projeto || ''), String(c.projeto || '')) as any[];
    return js.slice(0, 8).map((j: any) => ({
      valor: 'janela:' + j.id,
      rotulo: (DIAS[j.dia_semana] ?? '?') + ' ' + (j.hora || '') + (j.tipo === 'grupo' && j.vagas ? ' · ' + j.vagas + ' vagas' : ''),
    }));
  } catch { return []; }
}
app.get('/c/:id', (req: any, res: any) => {
  const c = convGet(String(req.params.id));
  if (!c) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Convite nao encontrado</h1>');
  const cor = c.cor || '#22c55e';
  const ehQuiz = c.tipo === 'quiz';
  const horarios = convHorarios(c);
  const base = String(PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  const vsl = c.videoVslId ? ('<div style="margin:0 0 14px"><iframe src="' + base + '/v/' + esc(c.videoVslId) + '" style="width:100%;aspect-ratio:16/9;border:0;border-radius:10px" allow="autoplay; fullscreen" allowfullscreen></iframe></div>') : '';
  const img = (!c.videoVslId && c.imagem) ? ('<img src="' + esc(c.imagem) + '" alt="" style="width:100%;border-radius:10px;margin:0 0 14px">') : '';
  const quizBloco = ehQuiz
    ? '<label>Há quanto tempo você toca?</label><select name="nivel"><option value="">— escolha —</option><option>Nunca toquei</option><option>Iniciante</option><option>Já toco um pouco</option><option>Toco na igreja</option></select>'
    : '';
  const opcoes = horarios.length
    ? '<label>Escolha seu horário</label><select name="horario" required><option value="">— escolha —</option>' + horarios.map((h, i) => '<option value="' + esc(h.valor) + '"' + (i === 0 ? ' selected' : '') + '>' + esc(h.rotulo) + '</option>').join('') + '</select>'
    : '<p class="hint">' + (c.destino === 'aula_vivo' ? 'Os horários aparecem aqui quando houver janelas abertas na Disponibilidade (#6).' : 'Sala automática — você recebe o acesso ao confirmar.') + '</p>';
  res.type('html').send('<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(c.nome || 'Seu convite') + '</title>'
    + '<style>body{margin:0;font:16px/1.5 system-ui,Segoe UI,Arial;background:#0e1116;color:#e6e6e6;display:flex;justify-content:center;padding:28px 16px}.card{width:100%;max-width:520px;background:#161b22;border:1px solid #2a313c;border-radius:14px;overflow:hidden}.top{height:6px;background:' + esc(cor) + '}.in{padding:22px}h1{font-size:22px;margin:0 0 6px}.sub{color:#9aa3b2;margin:0 0 16px;font-size:14px}label{display:block;margin:14px 0 5px;font-weight:600;font-size:14px}input,select{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:9px;border:1px solid #2a313c;background:#0e1116;color:#e6e6e6;font-size:15px}.hint{color:#9aa3b2;font-size:13px;margin:10px 0}button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:9px;background:' + esc(cor) + ';color:#07210f;font-weight:700;font-size:16px;cursor:pointer}#ok{display:none;margin-top:16px;padding:12px;border-radius:9px;background:#0f2e1b;border:1px solid #1db95455}</style></head><body><div class="card"><div class="top"></div><div class="in">'
    + img + vsl
    + '<h1>Você ganhou uma aula! 🎉</h1><p class="sub">' + (ehQuiz ? 'Responda rapidinho pra gente preparar sua aula e garanta seu horário:' : 'Confirme seus dados pra garantir sua vaga:') + '</p>'
    + '<form id="frm">' + quizBloco
    + '<label>Nome</label><input name="nome" required>'
    + '<label>WhatsApp</label><input name="telefone" type="tel" required>'
    + '<label>E-mail</label><input name="email" type="email">'
    + opcoes
    + '<button type="submit">Confirmar minha vaga</button></form><div id="ok">✅ Vaga confirmada! Você vai receber a confirmação no WhatsApp/e-mail.</div></div></div>'
    + '<script>document.getElementById("frm").onsubmit=async(e)=>{e.preventDefault();const d={};new FormData(e.target).forEach((v,k)=>d[k]=v);const r=await fetch("/c-api/' + c.id + '/agendar",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(d)});if(r.ok){e.target.style.display="none";document.getElementById("ok").style.display="block";}};</script></body></html>');
});
app.post('/c-api/:id/agendar', (req: any, res: any) => {
  try {
    const c = convGet(String(req.params.id)); if (!c) return res.status(404).json({ ok: false });
    const dados = (req.body && typeof req.body === 'object') ? req.body : {};
    const tel = String(dados.telefone || dados.whatsapp || dados.celular || '').replace(/\D/g, '');
    const nome = dados.nome || '';
    const email = dados.email || '';
    const agendou = dados.horario ? 1 : 0;
    let contatoId: string | null = null;
    if (tel && tel.length >= 10) {
      contatoId = upsertContato(String(c.projeto || ''), tel, nome, email, 'convite');
      const tag = String(c.tag || '').trim() || ('convite-agendado' + (c.projeto ? '-' + c.projeto : ''));
      if (contatoId) aplicarTag(String(c.projeto || ''), contatoId, tag);
      if (contatoId) rodarAutomacoes(String(c.projeto || ''), 'tag_adicionada', { contatoId, tag, telefone: tel, nome }).catch(() => {});
    }
    // [#13 B] confirmação direta conforme o convite pediu (pos.whatsapp envia na hora; pos.email enfileira)
    let pos: any = {}; try { pos = JSON.parse(c.pos || '{}'); } catch { pos = {}; }
    const primeiroNome = String(nome || '').trim().split(/\s+/)[0] || '';
    const msgConf = String(c.msg_confirmacao || '').trim()
      || ('Oi ' + (primeiroNome || 'tudo bem') + '! ✅ Sua vaga na aula está confirmada. Em breve te envio os detalhes por aqui. Qualquer dúvida, é só responder. 🎹');
    if (pos.whatsapp && tel && tel.length >= 10) {
      enviarTexto(String(c.projeto || '') as Projeto, tel, msgConf).catch(() => {});
    }
    if (pos.email && email) {
      try {
        const html = '<div style="font:16px/1.5 system-ui,Arial;color:#111"><h2>Vaga confirmada! 🎉</h2><p>' + esc(msgConf) + '</p></div>';
        db.prepare(`INSERT INTO email_fila(id,projeto,para,assunto,html,status,criado_em) VALUES(?,?,?,?,?,?,?)`)
          .run(randomUUID(), String(c.projeto || ''), String(email), 'Sua vaga está confirmada 🎉', html, 'pendente', agora());
      } catch { /* fila best-effort (sai quando o SMTP do domínio estiver ligado) */ }
    }
    db.prepare(`INSERT INTO convite_respostas(id,convite_id,etapa,dados,agendou,compareceu,ts) VALUES(?,?,?,?,?,?,?)`)
      .run(randomUUID(), c.id, 'agendou', JSON.stringify(dados), agendou, 0, agora());
    res.json({ ok: true });
  } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
});

// ---------- #4/#10 Academy ----------
app.get('/academy/resumo', (_req: any, res: any) => {
  try {
    const alunos = (db.prepare(`SELECT COUNT(*) n FROM academy_matriculas`).get() as any).n;
    const ativos7d = (db.prepare(`SELECT COUNT(*) n FROM academy_matriculas WHERE ultimo_acesso >= date('now','-7 day')`).get() as any).n;
    const novos_mes = (db.prepare(`SELECT COUNT(*) n FROM academy_matriculas WHERE criado_em >= date('now','start of month')`).get() as any).n;
    _ndOk(res, { alunos, ativos7d, novos_mes, acessos_hoje: 0, certificados: 0 });
  } catch (e) { _ndErr(res, e); }
});
app.get('/academy/cursos', (_req: any, res: any) => {
  try { _ndOk(res, { cursos: db.prepare(`SELECT * FROM academy_cursos ORDER BY criado_em DESC`).all() }); } catch (e) { _ndErr(res, e); }
});
app.post('/academy/curso', (req: any, res: any) => {
  try {
    const b = req.body || {};
    if (b.id) {
      if (b.publicado !== undefined) db.prepare(`UPDATE academy_cursos SET publicado=? WHERE id=?`).run(b.publicado ? 1 : 0, b.id);
      if (b.nome !== undefined) db.prepare(`UPDATE academy_cursos SET nome=?, descricao=?, produto=? WHERE id=?`).run(b.nome, b.descricao || '', b.produto || '', b.id);
      return _ndOk(res, { id: b.id });
    }
    const id = randomUUID();
    db.prepare(`INSERT INTO academy_cursos (id,nome,descricao,produto,publicado,criado_em) VALUES (?,?,?,?,0,?)`).run(id, b.nome, b.descricao || '', b.produto || '', agora());
    _ndOk(res, { id });
  } catch (e) { _ndErr(res, e); }
});
app.post('/academy/curso/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM academy_cursos WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});
app.get('/academy/alunos', (req: any, res: any) => {
  try {
    const busca = '%' + String(req.query.busca || '') + '%';
    const curso = String(req.query.curso || '');
    const alunos = db.prepare(`SELECT * FROM academy_matriculas WHERE (nome LIKE ? OR email LIKE ?) AND (?='' OR curso=?) ORDER BY criado_em DESC`).all(busca, busca, curso, curso);
    const cursos = (db.prepare(`SELECT DISTINCT curso FROM academy_matriculas WHERE curso IS NOT NULL AND curso<>''`).all() as any[]).map((r) => r.curso);
    _ndOk(res, { alunos, cursos });
  } catch (e) { _ndErr(res, e); }
});
app.post('/academy/aluno/acesso', (req: any, res: any) => {
  try { const b = req.body || {}; db.prepare(`UPDATE academy_matriculas SET ativo=? WHERE id=?`).run(b.liberar ? 1 : 0, b.id); _ndOk(res, { ok: true }); } catch (e) { _ndErr(res, e); }
});
app.get('/academy/vendas', (_req: any, res: any) => {
  try {
    const vendas = db.prepare(`SELECT * FROM academy_vendas ORDER BY data DESC`).all();
    const resumo = db.prepare(`SELECT COUNT(*) total_vendas, COALESCE(SUM(valor),0) faturamento, SUM(CASE WHEN status='reembolso' THEN 1 ELSE 0 END) reembolsos FROM academy_vendas`).get();
    _ndOk(res, { vendas, resumo });
  } catch (e) { _ndErr(res, e); }
});
app.get('/academy/ofertas', (_req: any, res: any) => {
  try { _ndOk(res, { ofertas: db.prepare(`SELECT * FROM academy_ofertas`).all() }); } catch (e) { _ndErr(res, e); }
});
app.post('/academy/ofertas', (req: any, res: any) => {
  try { const b = req.body || {}; const id = randomUUID(); db.prepare(`INSERT INTO academy_ofertas (id,nome,tipo,valor) VALUES (?,?,?,?)`).run(id, b.nome, b.tipo, Number(b.valor) || 0); _ndOk(res, { id }); } catch (e) { _ndErr(res, e); }
});
app.post('/academy/oferta/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM academy_ofertas WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});
app.get('/academy/config', (_req: any, res: any) => {
  try { const row = db.prepare(`SELECT * FROM academy_config WHERE id=1`).get() || {}; _ndOk(res, row); } catch (e) { _ndErr(res, e); }
});
app.post('/academy/config', (req: any, res: any) => {
  try {
    const b = req.body || {};
    db.prepare(`INSERT OR REPLACE INTO academy_config (id,dominio,certificado,termo,exigir_termo,yt_mascarado,trava_dominio,marcadagua) VALUES (1,?,?,?,?,?,?,?)`)
      .run(b.dominio || '', b.certificado ? 1 : 0, b.termo || '', b.exigir_termo ? 1 : 0, b.yt_mascarado ? 1 : 0, b.trava_dominio ? 1 : 0, b.marcadagua ? 1 : 0);
    _ndOk(res, { salvo: true });
  } catch (e) { _ndErr(res, e); }
});
app.get('/academy/relatorios', (_req: any, res: any) => {
  try {
    const ativos = (db.prepare(`SELECT COUNT(*) n FROM academy_matriculas WHERE ativo=1`).get() as any).n;
    _ndOk(res, { conclusao_media: 0, engajamento: 0, certificados: 0, ativos, cursos: [] });
  } catch (e) { _ndErr(res, e); }
});
app.get('/academy/atendimento/conversas', (_req: any, res: any) => {
  try { _ndOk(res, db.prepare(`SELECT id,aluno,email,canal,assunto,ultima,nao_lidas FROM academy_atendimento ORDER BY ts DESC`).all()); } catch (e) { _ndErr(res, e); }
});
app.get('/academy/atendimento/conversa/:id', (req: any, res: any) => {
  try { const row = db.prepare(`SELECT * FROM academy_atendimento WHERE id=?`).get(req.params.id) as any; if (row) row.mensagens = _ndJson(row.mensagens, []); _ndOk(res, row || {}); } catch (e) { _ndErr(res, e); }
});
app.post('/academy/atendimento/responder', (req: any, res: any) => {
  try {
    const b = req.body || {}; const row = db.prepare(`SELECT mensagens FROM academy_atendimento WHERE id=?`).get(b.conversaId) as any;
    const msgs = _ndJson(row && row.mensagens, []); msgs.push({ de: 'voce', texto: b.texto, ts: agora() });
    db.prepare(`UPDATE academy_atendimento SET mensagens=?, ultima=?, nao_lidas=0 WHERE id=?`).run(JSON.stringify(msgs), b.texto, b.conversaId);
    _ndOk(res, { enviado: true });
  } catch (e) { _ndErr(res, e); }
});

// ---------- #14 Testes A/B/C de páginas ----------
app.get('/paginas/testes', (req: any, res: any) => {
  try {
    const projeto = String(req.query.projeto || '');
    const rows = db.prepare(`SELECT * FROM paginas_testes WHERE (?='' OR projeto=?) ORDER BY criado_em DESC`).all(projeto, projeto) as any[];
    const testes = rows.map((t) => ({ ...t, testar: _ndJson(t.testar, []), variantes: _ndJson(t.variantes, []) }));
    _ndOk(res, { testes });
  } catch (e) { _ndErr(res, e); }
});
app.post('/paginas/teste', (req: any, res: any) => {
  try {
    const b = req.body || {}; const id = randomUUID();
    db.prepare(`INSERT INTO paginas_testes (id,projeto,nome,pagina,testar,metrica,status,significancia,variantes,criado_em) VALUES (?,?,?,?,?,?,?,?,?,?)`)
      .run(id, b.projeto, b.nome, b.pagina || '', JSON.stringify(b.testar || []), b.metrica || '', 'rodando', 0, JSON.stringify(b.variantes || []), agora());
    _ndOk(res, { id });
  } catch (e) { _ndErr(res, e); }
});
app.post('/paginas/teste/promover', (req: any, res: any) => {
  try {
    const b = req.body || {}; const row = db.prepare(`SELECT variantes FROM paginas_testes WHERE id=?`).get(b.id) as any;
    const vs = _ndJson(row && row.variantes, []).map((v: any) => ({ ...v, vencedora: v.nome === b.variante }));
    db.prepare(`UPDATE paginas_testes SET variantes=?, status='concluido' WHERE id=?`).run(JSON.stringify(vs), b.id);
    _ndOk(res, { promovido: b.variante });
  } catch (e) { _ndErr(res, e); }
});
app.post('/paginas/teste/remover', (req: any, res: any) => {
  try { db.prepare(`DELETE FROM paginas_testes WHERE id=?`).run((req.body || {}).id); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
});

// ---------- #14 Captação (leitura best-effort; zeros até ligar as fontes) ----------
app.get('/paginas/captacao', (_req: any, res: any) => {
  try { _ndOk(res, { resumo: { visitas: 0, leads: 0, agendou: 0, comprou: 0 }, paginas: [], origens: [] }); } catch (e) { _ndErr(res, e); }
});

// ---------- #14 Diagnóstico de conexões + alertas ----------
app.get('/capi/diagnostico', (req: any, res: any) => {
  try {
    const projeto = String(req.query.projeto || '');
    let eventos: any = { recebidos: 0, enviados: 0, erro: 0, por_tipo: [] };
    try {
      const r = db.prepare(`SELECT COUNT(*) n FROM capi_eventos`).get() as any;
      eventos.recebidos = r ? r.n : 0; eventos.enviados = eventos.recebidos;
    } catch { /* tabela capi_eventos pode não existir ainda */ }
    const alertas = db.prepare(`SELECT push,email,telegram FROM capi_alertas WHERE projeto=?`).get(projeto) || { push: 1, email: 1, telegram: 1 };
    _ndOk(res, { conexoes: [], eventos, emq_por_produto: [], alertas });
  } catch (e) { _ndErr(res, e); }
});
app.post('/capi/alertas', (req: any, res: any) => {
  try {
    const b = req.body || {};
    db.prepare(`INSERT OR REPLACE INTO capi_alertas (projeto,push,email,telegram) VALUES (?,?,?,?)`).run(b.projeto || '', b.push ? 1 : 0, b.email ? 1 : 0, b.telegram ? 1 : 0);
    _ndOk(res, { salvo: true });
  } catch (e) { _ndErr(res, e); }
});

// ---------- #2 Provedor de IA por agente ----------
app.get('/agente/provedor', (req: any, res: any) => {
  try { const row = db.prepare(`SELECT * FROM agente_provedor WHERE agente=?`).get(String(req.query.agente || '')) || {}; _ndOk(res, row); } catch (e) { _ndErr(res, e); }
});
app.post('/agente/provedor', (req: any, res: any) => {
  try { const b = req.body || {}; db.prepare(`INSERT OR REPLACE INTO agente_provedor (agente,base_url,modelo,chave_ref) VALUES (?,?,?,?)`).run(b.agente || '', b.base_url || '', b.modelo || '', b.chave_ref || ''); _ndOk(res, { salvo: true }); } catch (e) { _ndErr(res, e); }
});
app.post('/agente/provedor/testar', (_req: any, res: any) => { try { _ndOk(res, { ok: false, pendente: 'cole a chave no .env do VPS e ligue o teste real depois' }); } catch (e) { _ndErr(res, e); } });

// ---------- #5 Presets de tráfego (telas já têm defaults) ----------
app.get('/trafego/presets', (_req: any, res: any) => { try { _ndOk(res, { presets: [] }); } catch (e) { _ndErr(res, e); } });
app.get('/trafego/recomendacoes', (_req: any, res: any) => { try { _ndOk(res, { recomendacoes: [] }); } catch (e) { _ndErr(res, e); } });
app.post('/trafego/preset/aplicar', (_req: any, res: any) => { try { _ndOk(res, { aplicado: true }); } catch (e) { _ndErr(res, e); } });

// ---------- #7 Estúdio de Áudio (fila; worker Demucs nos 3 PCs liga depois) ----------
app.post('/audio/upload', (req: any, res: any) => {
  try { const b = req.body || {}; const id = randomUUID(); db.prepare(`INSERT INTO audio_jobs (id,nome,tamanho,status,progresso,criado_em) VALUES (?,?,?,?,0,?)`).run(id, b.nome || '', b.tamanho || 0, 'na fila', agora()); _ndOk(res, { jobId: id }); } catch (e) { _ndErr(res, e); }
});
const _audioAcao = (req: any, res: any) => {
  try { const b = req.body || {}; if (b.jobId) db.prepare(`UPDATE audio_jobs SET status='processando' WHERE id=?`).run(b.jobId); _ndOk(res, { status: 'na fila', jobId: b.jobId }); } catch (e) { _ndErr(res, e); }
};
app.post('/audio/stems', _audioAcao);
app.post('/audio/tom', _audioAcao);
app.post('/audio/velocidade', _audioAcao);
app.post('/audio/acordes', _audioAcao);
app.post('/audio/bpm', _audioAcao);
app.get('/audio/job/:id', (req: any, res: any) => {
  try { const row = db.prepare(`SELECT * FROM audio_jobs WHERE id=?`).get(req.params.id) as any; if (row) row.stems = _ndJson(row.stems, []); _ndOk(res, row || { status: 'desconhecido' }); } catch (e) { _ndErr(res, e); }
});

// ---------- #8 Editor novo (fila; workers ligam depois) ----------
const _editorAcao = (_req: any, res: any) => { try { _ndOk(res, { status: 'na fila' }); } catch (e) { _ndErr(res, e); } };
app.post('/editor/legendas', _editorAcao);
app.post('/editor/corte-silencio', _editorAcao);
app.post('/editor/corte-vicios', _editorAcao);
app.post('/editor/cortes-live', _editorAcao);
app.post('/editor/reframe', _editorAcao);
app.post('/editor/tela-dividida', _editorAcao);
app.post('/editor/prompt', _editorAcao);
app.post('/editor/limpar-audio', _editorAcao);
// ============================================================================
// #13 EVENTOS MULTI-AULA — agrupa N aulas/webinarios num evento (ex.: lancamento de 3 dias).
// Aditivo. Reusa a sala de webinario real (/w/:slug) + upsertContato/aplicarTag/rodarAutomacoes + _ndOk.
// Config (app, x-api-key): /eventos /evento /evento/aula /evento/:id/inscricoes
// Publico: /e/:slug (pagina do evento com a programacao) + /e-api/:slug/inscrever (inscricao unica -> tag -> entra no fluxo)
// ============================================================================
{
  db.exec(`
    CREATE TABLE IF NOT EXISTS webinar_eventos (
      id TEXT PRIMARY KEY, slug TEXT UNIQUE, projeto TEXT, nome TEXT, descricao TEXT,
      tag_inscrito TEXT, redirecionar TEXT, ativo INTEGER DEFAULT 1, criado_em TEXT, atualizado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS webinar_evento_aulas (
      id TEXT PRIMARY KEY, evento_id TEXT, ordem INTEGER DEFAULT 0, titulo TEXT,
      webinario_slug TEXT, data_hora TEXT, duracao_min INTEGER DEFAULT 60,
      tag_presenca TEXT, ativo INTEGER DEFAULT 1, criado_em TEXT
    );
    CREATE TABLE IF NOT EXISTS webinar_evento_inscricoes (
      id TEXT PRIMARY KEY, evento_id TEXT, contato_id TEXT, nome TEXT, email TEXT, telefone TEXT, origem TEXT, criado_em TEXT
    );
  `);

  const evSlug = (s: string): string => String(s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 48) || ('e-' + randomUUID().slice(0, 6));
  const evBase = (): string => String(PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  const evGet = (slugOrId: string): any => db.prepare(`SELECT * FROM webinar_eventos WHERE slug=? OR id=?`).get(String(slugOrId), String(slugOrId)) as any;
  const evAulas = (eventoId: string): any[] => db.prepare(`SELECT * FROM webinar_evento_aulas WHERE evento_id=? AND ativo=1 ORDER BY ordem ASC, data_hora ASC`).all(String(eventoId)) as any[];

  // -------- config (app, x-api-key) --------
  app.get('/eventos', (req: any, res: any) => {
    try {
      const projeto = String(req.query.projeto || '');
      const rows = (projeto
        ? db.prepare(`SELECT * FROM webinar_eventos WHERE projeto=? ORDER BY criado_em DESC LIMIT 100`).all(projeto)
        : db.prepare(`SELECT * FROM webinar_eventos ORDER BY criado_em DESC LIMIT 100`).all()) as any[];
      for (const ev of rows) {
        ev.aulas = (db.prepare(`SELECT COUNT(*) n FROM webinar_evento_aulas WHERE evento_id=? AND ativo=1`).get(ev.id) as any).n;
        ev.inscritos = (db.prepare(`SELECT COUNT(*) n FROM webinar_evento_inscricoes WHERE evento_id=?`).get(ev.id) as any).n;
        ev.link = evBase() + '/e/' + ev.slug;
      }
      _ndOk(res, { eventos: rows });
    } catch (e) { _ndErr(res, e); }
  });
  app.get('/evento/:id', (req: any, res: any) => {
    try {
      const ev = evGet(String(req.params.id)); if (!ev) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
      ev.aulasLista = evAulas(ev.id);
      ev.inscritos = (db.prepare(`SELECT COUNT(*) n FROM webinar_evento_inscricoes WHERE evento_id=?`).get(ev.id) as any).n;
      ev.link = evBase() + '/e/' + ev.slug;
      _ndOk(res, { evento: ev });
    } catch (e) { _ndErr(res, e); }
  });
  app.post('/evento', (req: any, res: any) => {
    try {
      const b = req.body || {};
      const nome = String(b.nome || 'Evento').trim();
      const existe = b.id ? evGet(String(b.id)) : null;
      const id = existe ? existe.id : (String(b.id || '').trim() || randomUUID());
      const slug = existe ? existe.slug : evSlug(nome + '-' + id.slice(0, 4));
      if (existe) {
        db.prepare(`UPDATE webinar_eventos SET projeto=?, nome=?, descricao=?, tag_inscrito=?, redirecionar=?, ativo=?, atualizado_em=? WHERE id=?`)
          .run(String(b.projeto || existe.projeto || ''), nome, String(b.descricao || ''), String(b.tag_inscrito || ''), String(b.redirecionar || ''), b.ativo === false ? 0 : 1, agora(), id);
      } else {
        db.prepare(`INSERT INTO webinar_eventos(id,slug,projeto,nome,descricao,tag_inscrito,redirecionar,ativo,criado_em,atualizado_em) VALUES(?,?,?,?,?,?,?,1,?,?)`)
          .run(id, slug, String(b.projeto || ''), nome, String(b.descricao || ''), String(b.tag_inscrito || ''), String(b.redirecionar || ''), agora(), agora());
      }
      _ndOk(res, { id, slug, link: evBase() + '/e/' + slug });
    } catch (e) { _ndErr(res, e); }
  });
  app.post('/evento/remover', (req: any, res: any) => {
    try {
      const id = String((req.body || {}).id || '');
      db.prepare(`DELETE FROM webinar_evento_aulas WHERE evento_id=?`).run(id);
      db.prepare(`DELETE FROM webinar_evento_inscricoes WHERE evento_id=?`).run(id);
      db.prepare(`DELETE FROM webinar_eventos WHERE id=?`).run(id);
      _ndOk(res, { removido: true });
    } catch (e) { _ndErr(res, e); }
  });
  app.post('/evento/aula', (req: any, res: any) => {
    try {
      const b = req.body || {};
      if (!b.evento_id) return res.status(400).json({ ok: false, erro: 'evento_id_obrigatorio' });
      const existe = b.id ? db.prepare(`SELECT * FROM webinar_evento_aulas WHERE id=?`).get(String(b.id)) as any : null;
      const id = existe ? existe.id : randomUUID();
      if (existe) {
        db.prepare(`UPDATE webinar_evento_aulas SET ordem=?, titulo=?, webinario_slug=?, data_hora=?, duracao_min=?, tag_presenca=?, ativo=? WHERE id=?`)
          .run(Number(b.ordem) || 0, String(b.titulo || ''), String(b.webinario_slug || ''), String(b.data_hora || ''), Number(b.duracao_min) || 60, String(b.tag_presenca || ''), b.ativo === false ? 0 : 1, id);
      } else {
        db.prepare(`INSERT INTO webinar_evento_aulas(id,evento_id,ordem,titulo,webinario_slug,data_hora,duracao_min,tag_presenca,ativo,criado_em) VALUES(?,?,?,?,?,?,?,?,1,?)`)
          .run(id, String(b.evento_id), Number(b.ordem) || 0, String(b.titulo || ''), String(b.webinario_slug || ''), String(b.data_hora || ''), Number(b.duracao_min) || 60, String(b.tag_presenca || ''), agora());
      }
      _ndOk(res, { id });
    } catch (e) { _ndErr(res, e); }
  });
  app.post('/evento/aula/remover', (req: any, res: any) => {
    try { db.prepare(`DELETE FROM webinar_evento_aulas WHERE id=?`).run(String((req.body || {}).id || '')); _ndOk(res, { removido: true }); } catch (e) { _ndErr(res, e); }
  });
  app.get('/evento/:id/inscricoes', (req: any, res: any) => {
    try {
      const ev = evGet(String(req.params.id)); if (!ev) return res.status(404).json({ ok: false, erro: 'nao_encontrado' });
      const rows = db.prepare(`SELECT * FROM webinar_evento_inscricoes WHERE evento_id=? ORDER BY criado_em DESC LIMIT 1000`).all(ev.id) as any[];
      _ndOk(res, { inscricoes: rows, total: rows.length });
    } catch (e) { _ndErr(res, e); }
  });

  // -------- publico: pagina do evento + inscricao --------
  app.get('/e/:slug', (req: any, res: any) => {
    const ev = evGet(String(req.params.slug));
    if (!ev || !ev.ativo) return res.status(404).type('html').send('<!doctype html><meta charset="utf-8"><h1>Evento nao encontrado</h1>');
    const aulas = evAulas(ev.id);
    const fmtData = (s: string): string => { if (!s) return 'em breve'; try { return new Date(s).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }); } catch { return String(s); } };
    const linhasAulas = aulas.map((a: any, i: number) => '<div class="aula"><div class="n">' + (i + 1) + '</div><div><div class="at">' + esc(a.titulo || ('Aula ' + (i + 1))) + '</div><div class="ad">' + esc(fmtData(a.data_hora)) + (a.duracao_min ? ' &middot; ' + a.duracao_min + 'min' : '') + '</div></div></div>').join('');
    res.type('html').send('<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(ev.nome) + '</title>'
      + '<style>body{margin:0;font:16px/1.5 system-ui,Segoe UI,Arial;background:#0e1116;color:#e6e6e6;display:flex;justify-content:center;padding:28px 16px}.card{width:100%;max-width:560px;background:#161b22;border:1px solid #2a313c;border-radius:14px;padding:24px}h1{font-size:24px;margin:0 0 6px}.desc{color:#9aa3b2;margin:0 0 18px}.aula{display:flex;gap:12px;align-items:center;padding:11px 12px;border:1px solid #2a313c;border-radius:10px;margin-bottom:9px;background:#0e1116}.aula .n{width:28px;height:28px;flex:none;border-radius:50%;background:#1db954;color:#07210f;font-weight:700;display:flex;align-items:center;justify-content:center}.at{font-weight:600}.ad{color:#9aa3b2;font-size:13px}label{display:block;margin:12px 0 5px;font-weight:600}input{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:9px;border:1px solid #2a313c;background:#0e1116;color:#e6e6e6;font-size:15px}button{margin-top:18px;width:100%;padding:13px;border:0;border-radius:9px;background:#1db954;color:#07210f;font-weight:700;font-size:16px;cursor:pointer}#ok{display:none;margin-top:16px;padding:12px;border-radius:9px;background:#0f2e1b;border:1px solid #1db95455}h3{margin:22px 0 10px;font-size:16px}</style></head><body><div class="card">'
      + '<h1>' + esc(ev.nome) + '</h1>' + (ev.descricao ? '<p class="desc">' + esc(ev.descricao) + '</p>' : '')
      + '<h3>Programacao (' + aulas.length + ' aula' + (aulas.length === 1 ? '' : 's') + ')</h3>' + (linhasAulas || '<p class="desc">Em breve.</p>')
      + '<h3>Garanta sua vaga</h3><form id="frm"><label>Nome</label><input name="nome" required><label>WhatsApp</label><input name="telefone" type="tel" required><label>E-mail</label><input name="email" type="email"><button type="submit">Quero participar</button></form><div id="ok">✅ Inscricao confirmada! Voce vai receber os links de cada aula.</div></div>'
      + '<script>document.getElementById("frm").onsubmit=async(e)=>{e.preventDefault();const d={};new FormData(e.target).forEach((v,k)=>d[k]=v);const r=await fetch("/e-api/' + ev.slug + '/inscrever",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(d)});if(r.ok){e.target.style.display="none";document.getElementById("ok").style.display="block";' + (ev.redirecionar ? 'setTimeout(()=>location.href=' + JSON.stringify(String(ev.redirecionar)) + ',1400);' : '') + '}};</script></body></html>');
  });
  app.post('/e-api/:slug/inscrever', (req: any, res: any) => {
    try {
      const ev = evGet(String(req.params.slug)); if (!ev || !ev.ativo) return res.status(404).json({ ok: false });
      const dados = (req.body && typeof req.body === 'object') ? req.body : {};
      const tel = String(dados.telefone || dados.phone || dados.celular || dados.whatsapp || '').replace(/\D/g, '');
      const nome = dados.nome || dados.name || '';
      const email = dados.email || '';
      let contatoId: string | null = null;
      if (tel && tel.length >= 10) {
        contatoId = upsertContato(ev.projeto, tel, nome, email, 'evento');
        if (ev.tag_inscrito) aplicarTag(ev.projeto, contatoId, String(ev.tag_inscrito));
        if (contatoId) rodarAutomacoes(ev.projeto, 'tag_adicionada', { contatoId, tag: String(ev.tag_inscrito || ''), telefone: tel, nome }).catch(() => {});
      }
      db.prepare(`INSERT INTO webinar_evento_inscricoes(id,evento_id,contato_id,nome,email,telefone,origem,criado_em) VALUES(?,?,?,?,?,?,?,?)`)
        .run(randomUUID(), ev.id, contatoId, String(nome), String(email), tel, 'pagina', agora());
      res.json({ ok: true, redirecionar: ev.redirecionar || '' });
    } catch (e: any) { res.status(500).json({ ok: false, erro: String(e?.message || e) }); }
  });
}
// ===================== FIM DO LOTE A =====================

app.listen(PORT, () => console.log(`[engine] ouvindo na porta ${PORT} — base ${PUBLIC_BASE_URL}`));
