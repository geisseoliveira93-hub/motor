/**
 * [+ig-live-nuvem 12/11 Ezequias] Instagram LIVE via navegador invisível NA NUVEM (motor/VPS).
 *
 * Espelha a fundação do tt-browser-nuvem (Playwright headless + proxy residencial de
 * casa via escolherProxy do yt-browser-nuvem + cofre de sessão por conta), mas com uma
 * diferença essencial: o TikTok POSTA e fecha; aqui o Instagram Live precisa do navegador
 * ABERTO o tempo todo pra sustentar a transmissão. Então:
 *   - iniciarLiveIgNuvem(): abre o Live Producer, dá "Transmitir ao vivo" e MANTÉM o
 *     browser/context/page vivo num mapa por conta (NÃO fecha).
 *   - pararLiveIgNuvem(): encerra a live (botão) e fecha o browser daquela conta.
 *   - statusLiveIgNuvem(): diz se está no ar.
 *
 * Quem empurra o VÍDEO (ffmpeg -> rtmps da chave persistente) continua sendo o motor
 * (livExecutar / ffmpegPushRtmp). Este módulo só cuida do "dar o ao vivo" no navegador.
 *
 * Sessão: /data/ig-sessions/<conta>.json  (gravada via POST /pub/instagram/sessao-nuvem,
 * igual ao TikTok). Proxy: o MESMO residencial de casa do YouTube/TikTok (OBRIGATÓRIO pra
 * não queimar o IP de datacenter — o Instagram bloqueia pesado).
 */

import * as fs from 'fs';

// Reusa o MESMO proxy residencial de casa do YouTube/TikTok (failover entre aparelhos).
import { escolherProxy } from './yt-browser-nuvem';

const IG_SESS_DIR = '/data/ig-sessions';
const SNAP_DIR = '/data/media/ig-snaps'; // servido em /media/ig-snaps/
const PROXY_FILE = '/data/yt-sessions/_proxies.json';
try { fs.mkdirSync(IG_SESS_DIR, { recursive: true }); } catch { /* ok */ }
try { fs.mkdirSync(SNAP_DIR, { recursive: true }); } catch { /* ok */ }

const USER_AGENT_DEFAULT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const TIMEOUT_NAV_MS = 60_000;
const TIMEOUT_GOLIVE_MS = 90_000;

export type MotivoFalhaIG =
  | 'sem_sessao'
  | 'sessao_expirada'
  | 'sem_proxy'
  | 'checkpoint'
  | 'ui_mudou'
  | 'timeout'
  | 'ja_rodando'
  | 'outro';

export interface ResultadoLiveIG {
  ok: boolean;
  erro?: string;
  motivoFalha?: MotivoFalhaIG;
  conta?: string;
  proxyUsado?: string | null;
  snap?: string | null;
  duracaoMs?: number;
}

// --- navegadores vivos por conta (NÃO fechar enquanto a live estiver no ar) ---
interface LiveVivo { browser: any; ctx: any; page: any; inicio: number; titulo: string; }
const LIVES_IG: Map<string, LiveVivo> = new Map();

function logI(msg: string, extra?: any): void { try { console.log('[ig-live-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* */ } }
function logW(msg: string, extra?: any): void { try { console.warn('[ig-live-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* */ } }

async function carregarChromium(): Promise<any> {
  const mod: any = await import('patchright').catch(() => import('playwright'));
  return mod.chromium;
}

function qtdProxiesConfigurados(): number {
  try {
    const d = JSON.parse(fs.readFileSync(PROXY_FILE, 'utf8'));
    const lista = Array.isArray(d) ? d : (d && Array.isArray(d.lista) ? d.lista : []);
    return lista.filter((p: any) => p && p.host && p.port).length;
  } catch { return 0; }
}

function lerSessao(conta: string): { storageState: any; userAgent: string } | null {
  try {
    const d = JSON.parse(fs.readFileSync(IG_SESS_DIR + '/' + conta + '.json', 'utf8'));
    if (!d || !d.storageState || typeof d.storageState !== 'object') return null;
    return { storageState: d.storageState, userAgent: d.userAgent || USER_AGENT_DEFAULT };
  } catch { return null; }
}

async function snap(page: any, nome: string): Promise<string | null> {
  try {
    const p = SNAP_DIR + '/' + nome + '-' + Date.now() + '.jpg';
    await page.screenshot({ path: p, type: 'jpeg', quality: 60 });
    return p;
  } catch { return null; }
}

export function statusLiveIgNuvem(conta: string): { vivo: boolean; inicio?: number; titulo?: string } {
  const v = LIVES_IG.get(String(conta || '').toLowerCase());
  if (!v) return { vivo: false };
  return { vivo: true, inicio: v.inicio, titulo: v.titulo };
}

export function listarLivesIgNuvem(): Array<{ conta: string; inicio: number; titulo: string }> {
  const out: Array<{ conta: string; inicio: number; titulo: string }> = [];
  for (const [conta, v] of LIVES_IG.entries()) out.push({ conta, inicio: v.inicio, titulo: v.titulo });
  return out;
}

/**
 * Abre o Instagram Live Producer com a sessão da conta (saindo pelo IP de casa), põe
 * título, avança, e clica "Transmitir ao vivo". MANTÉM o browser aberto (no mapa) pra
 * sustentar a live. O vídeo em si já está sendo empurrado pelo motor (ffmpeg) na chave
 * persistente — por isso o Instagram mostra "Conectado/Pronto" e deixa transmitir.
 */
export async function iniciarLiveIgNuvem(opts: { conta: string; titulo?: string; privacidade?: string }): Promise<ResultadoLiveIG> {
  const start = Date.now();
  const conta = String(opts.conta || '').trim().toLowerCase();
  const titulo = String(opts.titulo || 'Ao vivo').slice(0, 125);
  if (!conta) return { ok: false, erro: 'conta_obrigatoria', motivoFalha: 'outro' };
  if (LIVES_IG.has(conta)) return { ok: false, erro: 'Já existe uma live no ar nessa conta pelo navegador da nuvem.', motivoFalha: 'ja_rodando', conta };

  const sess = lerSessao(conta);
  if (!sess) return { ok: false, erro: 'Sessão do Instagram não salva pra essa conta. Faça o login único (export de sessão) antes.', motivoFalha: 'sem_sessao', conta };

  let browser: any = null; let ctx: any = null; let page: any = null; let proxyUsado: string | null = null;
  try {
    const launchOpts: any = { headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled'] };
    const temProxyLista = qtdProxiesConfigurados() > 0;
    const escolhido: any = temProxyLista ? await escolherProxy() : null;
    if (temProxyLista && (!escolhido || !escolhido.playwright)) {
      return { ok: false, erro: 'Nenhum aparelho de casa (proxy residencial) está online agora. Ligue um PC/celular da lista e tente de novo.', motivoFalha: 'sem_proxy', conta, duracaoMs: Date.now() - start };
    }
    if (escolhido && escolhido.playwright) {
      launchOpts.proxy = escolhido.playwright;
      proxyUsado = escolhido.node?.nome || escolhido.node?.host || 'casa';
      logI('usando proxy residencial', { proxy: proxyUsado });
    } else {
      logW('SEM proxy residencial — saindo pelo IP do servidor (Instagram provavelmente recusa)');
    }

    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({
      storageState: sess.storageState,
      userAgent: sess.userAgent || USER_AGENT_DEFAULT,
      viewport: { width: 1366, height: 820 },
      locale: 'pt-BR',
    });
    page = await ctx.newPage();
    page.setDefaultTimeout(TIMEOUT_NAV_MS);

    await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: TIMEOUT_NAV_MS });
    await page.waitForTimeout(3500);

    // sessão expirada? (foi parar na tela de login)
    const urlAgora = String(page.url() || '');
    if (/\/accounts\/login/i.test(urlAgora) || await page.locator('input[name="password"]').count().catch(() => 0)) {
      const s = await snap(page, 'login');
      await fecharTudo(browser);
      return { ok: false, erro: 'Sessão do Instagram expirou (caiu no login). Refaça o export da sessão pela conta.', motivoFalha: 'sessao_expirada', conta, proxyUsado, snap: s, duracaoMs: Date.now() - start };
    }
    // checkpoint/verificação?
    if (/challenge|checkpoint/i.test(urlAgora)) {
      const s = await snap(page, 'checkpoint');
      await fecharTudo(browser);
      return { ok: false, erro: 'Instagram pediu verificação (checkpoint). Confirme a conta manualmente e refaça o export da sessão.', motivoFalha: 'checkpoint', conta, proxyUsado, snap: s, duracaoMs: Date.now() - start };
    }

    // Abrir Criar -> Vídeo ao vivo
    const abriuCriar = await clicarPrimeiro(page, [
      'div[role="button"]:has-text("Criar")',
      'a:has-text("Criar")',
      'svg[aria-label="Novo post"]',
      'svg[aria-label="New post"]',
    ]);
    if (!abriuCriar) {
      const s = await snap(page, 'sem-criar');
      await fecharTudo(browser);
      return { ok: false, erro: 'Não achei o botão "Criar" no Instagram (UI mudou?).', motivoFalha: 'ui_mudou', conta, proxyUsado, snap: s, duracaoMs: Date.now() - start };
    }
    await page.waitForTimeout(1200);
    const abriuLive = await clicarPrimeiro(page, [
      'span:has-text("Vídeo ao vivo")',
      'div:has-text("Vídeo ao vivo")',
      'span:has-text("Live video")',
      'a[href*="/live/"]',
    ]);
    if (!abriuLive) {
      const s = await snap(page, 'sem-videoaovivo');
      await fecharTudo(browser);
      return { ok: false, erro: 'Não achei a opção "Vídeo ao vivo" no menu Criar.', motivoFalha: 'ui_mudou', conta, proxyUsado, snap: s, duracaoMs: Date.now() - start };
    }
    await page.waitForTimeout(2000);

    // Tela "Adicionar detalhes": título + Avançar
    try {
      const campoTitulo = page.locator('input[placeholder="Título"], input[placeholder="Title"], input[type="text"]').first();
      if (await campoTitulo.count().catch(() => 0)) { await campoTitulo.fill(titulo).catch(() => { /* */ }); }
    } catch { /* */ }
    await clicarPrimeiro(page, ['div[role="button"]:has-text("Avançar")', 'button:has-text("Avançar")', 'div[role="button"]:has-text("Next")', 'button:has-text("Next")']);
    await page.waitForTimeout(3000);

    // Tela da chave de stream: esperar a chave persistente ficar "Conectado/Pronto" (o ffmpeg já está empurrando)
    // e clicar em "Transmitir ao vivo".
    const inicioGoLive = Date.now();
    let transmitiu = false;
    while (Date.now() - inicioGoLive < TIMEOUT_GOLIVE_MS) {
      // o botão "Transmitir ao vivo" costuma estar no topo direito e habilita quando o stream conecta
      const okBtn = await clicarPrimeiro(page, [
        'div[role="button"]:has-text("Transmitir ao vivo")',
        'button:has-text("Transmitir ao vivo")',
        'div[role="button"]:has-text("Go live")',
        'button:has-text("Go live")',
      ], /*soMarcarSeHabilitado*/ true);
      if (okBtn) { transmitiu = true; break; }
      await page.waitForTimeout(3000);
    }
    const sFinal = await snap(page, transmitiu ? 'no-ar' : 'nao-transmitiu');
    if (!transmitiu) {
      await fecharTudo(browser);
      return { ok: false, erro: 'Cheguei no Live Producer mas o botão "Transmitir ao vivo" não habilitou a tempo (o stream do motor pode não ter conectado na chave persistente).', motivoFalha: 'timeout', conta, proxyUsado, snap: sFinal, duracaoMs: Date.now() - start };
    }

    // Deu o ao vivo — MANTÉM o browser aberto pra sustentar a live.
    LIVES_IG.set(conta, { browser, ctx, page, inicio: Date.now(), titulo });
    logI('LIVE NO AR (navegador da nuvem mantido aberto)', { conta, proxyUsado });
    return { ok: true, conta, proxyUsado, snap: sFinal, duracaoMs: Date.now() - start };
  } catch (e: any) {
    try { if (browser) await fecharTudo(browser); } catch { /* */ }
    return { ok: false, erro: String(e?.message || e), motivoFalha: 'outro', conta, proxyUsado, duracaoMs: Date.now() - start };
  }
}

/** Encerra a live (botão) e fecha o navegador da nuvem daquela conta. */
export async function pararLiveIgNuvem(conta: string): Promise<{ ok: boolean; erro?: string }> {
  const c = String(conta || '').trim().toLowerCase();
  const v = LIVES_IG.get(c);
  if (!v) return { ok: true }; // já não tem nada rodando
  try {
    try {
      await clicarPrimeiro(v.page, [
        'div[role="button"]:has-text("Encerrar vídeo ao vivo")',
        'button:has-text("Encerrar vídeo ao vivo")',
        'div[role="button"]:has-text("Encerrar")',
        'div[role="button"]:has-text("End live")',
      ]);
      await v.page.waitForTimeout(800);
      // confirmar o "Encerrar agora/End now" se aparecer
      await clicarPrimeiro(v.page, ['button:has-text("Encerrar agora")', 'button:has-text("End now")', 'button:has-text("Encerrar")']);
    } catch { /* */ }
    await fecharTudo(v.browser);
  } catch (e: any) {
    try { await fecharTudo(v.browser); } catch { /* */ }
  } finally {
    LIVES_IG.delete(c);
  }
  return { ok: true };
}

async function fecharTudo(browser: any): Promise<void> {
  try { await browser?.close?.(); } catch { /* */ }
}

/**
 * Clica no primeiro seletor que existir. Se soMarcarSeHabilitado=true, só considera
 * clicado quando o elemento está visível E habilitado (pro botão "Transmitir ao vivo"
 * que só habilita depois que o stream conecta).
 */
async function clicarPrimeiro(page: any, seletores: string[], soMarcarSeHabilitado = false): Promise<boolean> {
  for (const sel of seletores) {
    try {
      const loc = page.locator(sel).first();
      const n = await loc.count().catch(() => 0);
      if (!n) continue;
      if (soMarcarSeHabilitado) {
        const vis = await loc.isVisible().catch(() => false);
        const dis = await loc.isDisabled().catch(() => false);
        if (!vis || dis) continue;
      }
      await loc.click({ timeout: 6000 }).catch(() => { /* */ });
      return true;
    } catch { /* tenta o próximo */ }
  }
  return false;
}
