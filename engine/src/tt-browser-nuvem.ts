/**
 * [+tt-browser-nuvem 31/10] TikTok via navegador invisível NA NUVEM (motor/VPS).
 *
 * Porte fiel do tiktok-playwright.ts do PC (postarTikTok — 38 correções acumuladas
 * de UI do TikTok Studio), mas SEM nada de Electron: usa Playwright puro
 * (chromium.launch headless) e lê a sessão logada do cofre em
 * /data/tt-sessions/<projeto>.json (gravada pelo app via POST /pub/tiktok/sessao-nuvem).
 *
 * Reusa o MESMO proxy residencial de casa (aparelhos via Tailscale, com failover)
 * do yt-browser-nuvem — o TikTok bloqueia IP de datacenter ainda mais que o Google,
 * então o proxy é OBRIGATÓRIO aqui: se há lista configurada e nenhum aparelho online,
 * aborta com erro claro em vez de queimar o IP do VPS.
 *
 * Diferenças vs. PC (de propósito, por ser servidor headless sem Electron):
 *   - launchPersistentContext(perfil)  →  launch() + newContext({ storageState })
 *   - screenshots de diagnóstico via app.getPath  →  snap() opcional em /data/media/tt-snaps
 *   - fallback "agente IA" (Claude Vision do app)  →  removido (o fluxo tem caminho não-IA p/ tudo)
 *   - marcarFalha/marcarUsoOk (store local do app) →  removido (devolve o resultado; o publicador decide)
 *   - NÃO acrescenta marcador #appPW... na legenda (a legenda sai exatamente como o publicador montou)
 */

import * as fs from 'fs';
import * as path from 'path';
// Reusa o MESMO proxy residencial de casa do YouTube: a função de escolha com failover
// (escolherProxy) vem do yt-browser-nuvem; só a contagem da lista é lida localmente aqui
// (lerProxies é interna lá), do mesmo arquivo /data/yt-sessions/_proxies.json.
import { escolherProxy } from './yt-browser-nuvem';

const PROXY_FILE = '/data/yt-sessions/_proxies.json';
function qtdProxiesConfigurados(): number {
  try {
    const d = JSON.parse(fs.readFileSync(PROXY_FILE, 'utf8'));
    const lista = Array.isArray(d) ? d : (d && Array.isArray(d.lista) ? d.lista : []);
    return lista.filter((p: any) => p && p.host && p.port).length;
  } catch { return 0; }
}

async function carregarChromium(): Promise<any> {
  const mod: any = await import('patchright').catch(() => import('playwright'));
  return mod.chromium;
}

const USER_AGENT_DEFAULT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const TT_SESS_DIR = '/data/tt-sessions';
const SNAP_DIR = '/data/media/tt-snaps'; // servido em /media/tt-snaps/ (conferir prints por link)

const TIMEOUT_POST_MS = 10 * 60_000;
const TIMEOUT_INPUT_FILE_MS = 90_000;
const URL_STUDIO_UPLOAD_VIDEO = 'https://www.tiktok.com/tiktokstudio/upload';
const URL_STUDIO_UPLOAD_PHOTO = 'https://www.tiktok.com/tiktokstudio/upload/photo';

export type MotivoFalhaTT =
  | 'sem_sessao'
  | 'sessao_expirada'
  | 'captcha'
  | 'rate_limit'
  | 'timeout'
  | 'arquivo_inexistente'
  | 'sem_proxy'
  | 'ui_mudou'
  | 'outro';

export interface PublicarTtNuvemPayload {
  projeto: string;        // teclado | baixo | violao | comunidade
  arquivo: string | string[]; // caminho(s) absoluto(s) no servidor (vídeo = 1; carrossel = N imagens)
  descricao?: string;
  tipo?: 'video' | 'carrossel';
  privacy?: 'public' | 'self'; // 'self' = Somente você (pra testar sem aparecer pro público)
  snaps?: boolean;
}

export interface PublicarTtNuvemResposta {
  ok: boolean;
  postUrl?: string;
  erro?: string;
  motivoFalha?: MotivoFalhaTT;
  duracaoMs: number;
  screenshots?: string[];
  proxyUsado?: string | null;
}

function logI(msg: string, extra?: any): void {
  try { console.log('[tt-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* ok */ }
}
function logW(msg: string, extra?: any): void {
  try { console.warn('[tt-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* ok */ }
}

function delayHumano(min = 1000, max = 3000): Promise<void> {
  const ms = min + Math.floor(Math.random() * Math.max(0, max - min));
  return new Promise((r) => setTimeout(r, ms));
}

/** Lê a sessão do cofre (gravada pelo app via POST /pub/tiktok/sessao-nuvem). */
function lerSessao(projeto: string): { storageState: any; userAgent: string } | null {
  try {
    const f = TT_SESS_DIR + '/' + projeto + '.json';
    const d = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (!d || !d.storageState || typeof d.storageState !== 'object') return null;
    return { storageState: d.storageState, userAgent: d.userAgent || USER_AGENT_DEFAULT };
  } catch {
    return null;
  }
}

export async function publicarTiktokNuvem(payload: PublicarTtNuvemPayload): Promise<PublicarTtNuvemResposta> {
  const start = Date.now();
  const projeto = String(payload.projeto || '').trim().toLowerCase();
  const tipo = payload.tipo === 'carrossel' ? 'carrossel' : 'video';
  const arquivos = Array.isArray(payload.arquivo) ? payload.arquivo : [payload.arquivo];
  const descricao = (payload.descricao || '').trim();
  const privacyAlvo = payload.privacy === 'self' ? 'self' : 'public';
  const screenshots: string[] = [];
  const querSnaps = !!payload.snaps;

  logI('iniciando post via navegador invisível (nuvem)', { projeto, tipo, arquivos: arquivos.length });

  if (!projeto) {
    return { ok: false, erro: 'projeto obrigatório', motivoFalha: 'outro', duracaoMs: Date.now() - start };
  }
  for (const a of arquivos) {
    if (!a || !fs.existsSync(a)) {
      return { ok: false, erro: 'arquivo não encontrado no servidor: ' + a, motivoFalha: 'arquivo_inexistente', duracaoMs: Date.now() - start };
    }
  }
  const sess = lerSessao(projeto);
  if (!sess) {
    return { ok: false, erro: 'Sessão do TikTok não está no cofre da nuvem. Exporte a sessão pelo app (botão de exportar sessão TikTok p/ nuvem).', motivoFalha: 'sem_sessao', duracaoMs: Date.now() - start };
  }

  try { if (querSnaps) fs.mkdirSync(SNAP_DIR, { recursive: true }); } catch { /* ok */ }

  let browser: any = null;
  let ctx: any = null;
  let page: any = null;

  async function snap(etapa: string): Promise<void> {
    if (!querSnaps || !page) return;
    try {
      const p = path.join(SNAP_DIR, projeto + '-' + Date.now() + '-' + etapa + '.png');
      await page.screenshot({ path: p, fullPage: false });
      screenshots.push(p);
    } catch { /* ignore */ }
  }

  let proxyUsado: string | null = null;
  try {
    // Proxy residencial de casa (sai pelo IP de casa). TikTok recusa IP de datacenter —
    // se há lista e nenhum aparelho online, aborta sem arriscar o IP do VPS.
    const temLista = qtdProxiesConfigurados() > 0;
    const escolhido = await escolherProxy();
    if (temLista && !escolhido) {
      return { ok: false, erro: 'Nenhum aparelho de casa (proxy residencial) está online agora. Ligue um PC/celular da lista e tente de novo.', motivoFalha: 'sem_proxy', duracaoMs: Date.now() - start, screenshots };
    }
    const launchOpts: any = {
      headless: true,
      args: [
        '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
        '--no-default-browser-check', '--no-first-run', '--disable-extensions', '--disable-infobars'
      ]
    };
    if (escolhido) {
      launchOpts.proxy = escolhido.playwright;
      proxyUsado = escolhido.node.nome || escolhido.node.host;
      logI('usando proxy residencial', { proxy: proxyUsado });
    } else {
      logW('SEM proxy residencial configurado — saindo pelo IP do servidor (datacenter); o TikTok provavelmente vai recusar');
    }
    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({
      storageState: sess.storageState,
      userAgent: sess.userAgent || USER_AGENT_DEFAULT,
      viewport: { width: 1920, height: 1200 },
      locale: 'pt-BR',
      timezoneId: 'America/Sao_Paulo',
      deviceScaleFactor: 1,
      hasTouch: false,
      isMobile: false,
      colorScheme: 'light'
    });

    // Disfarce: navigator.webdriver=false + plugins/languages fake ANTES de qualquer script.
    await ctx.addInitScript(() => {
      const nav: any = (globalThis as any).navigator;
      try { Object.defineProperty(nav, 'webdriver', { get: () => false }); } catch { /* ok */ }
      try { Object.defineProperty(nav, 'plugins', { get: () => [1, 2, 3, 4, 5] }); } catch { /* ok */ }
      try { Object.defineProperty(nav, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] }); } catch { /* ok */ }
    });

    // Remove modais bloqueadores (Verificações automáticas, Novos recursos, joyride) assim que aparecem.
    await ctx.addInitScript(() => {
      const doc: any = (globalThis as any).document;
      const win: any = (globalThis as any).window;
      const padroesTitulo = [
        /verifica[çc][õo]es? autom[áa]tic/i, /automatic content check/i,
        /ativar verifica[çc]/i, /enable automatic verif/i,
        /novos recursos de edi[çc][ãa]o adicionados/i, /new editing features/i,
        /^entendi$/i, /^got it$/i
      ];
      function remover(): void {
        try {
          const dialogs = doc.querySelectorAll('[role="dialog"], [class*="modal" i], [class*="dialog" i]');
          for (const d of Array.from(dialogs) as any[]) {
            const txt = ((d as any).innerText || (d as any).textContent || '').trim().slice(0, 500);
            if (padroesTitulo.some((rx) => rx.test(txt))) { try { (d as any).remove(); } catch { /* ok */ } }
          }
          const fixed = doc.querySelectorAll('div, section');
          for (const el of Array.from(fixed) as any[]) {
            const cs = win.getComputedStyle(el as any);
            const zi = parseInt(cs.zIndex || '0', 10) || 0;
            if ((cs.position !== 'fixed' && cs.position !== 'absolute') || zi < 100) continue;
            const txt = ((el as any).innerText || (el as any).textContent || '').trim().slice(0, 500);
            if (txt.length < 10 || txt.length > 800) continue;
            if (padroesTitulo.some((rx) => rx.test(txt))) { try { (el as any).remove(); } catch { /* ok */ } }
          }
          const jr = doc.querySelectorAll('#react-joyride-portal, .react-joyride__overlay, [data-test-id="overlay"][class*="joyride"]');
          for (const j of Array.from(jr) as any[]) { try { (j as any).remove(); } catch { /* ok */ } }
        } catch { /* ok */ }
      }
      try { remover(); } catch { /* ok */ }
      try { win.__ttkFixInterval = win.setInterval(remover, 500); } catch { /* ok */ }
    });

    page = await ctx.newPage();
    page.setDefaultTimeout(TIMEOUT_POST_MS);

    // ETAPA 1: abrir o Studio (vídeo ou foto/carrossel)
    const urlAlvo = tipo === 'carrossel' ? URL_STUDIO_UPLOAD_PHOTO : URL_STUDIO_UPLOAD_VIDEO;
    await page.goto(urlAlvo, { timeout: 60_000, waitUntil: 'domcontentloaded' });
    await delayHumano(4000, 6000);
    await snap('01-studio');

    const urlAgora = page.url();
    if (urlAgora.includes('/login') || urlAgora.includes('/signup')) {
      return { ok: false, erro: 'Sessão TikTok expirou — redirecionou pra login. Exporte a sessão de novo pelo app.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    // Captcha
    const captcha = await page.$('[class*="captcha" i], [id*="captcha" i], [class*="verify" i]').catch(() => null);
    if (captcha) {
      return { ok: false, erro: 'TikTok pediu verificação (captcha). Pode ser o IP — confira o proxy de casa.', motivoFalha: 'captcha', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    // "Algo deu errado" — tenta 3x
    for (let t = 0; t < 3; t++) {
      const temErro = await page.evaluate(() => {
        const doc: any = (globalThis as any).document;
        const body = (doc?.body?.innerText || doc?.body?.textContent || '').toLowerCase();
        const temRetry = Array.from(doc?.querySelectorAll('button, [role="button"]') || []).some(
          (b: any) => /tentar novamente|try again/i.test(b.textContent || '')
        );
        return body.includes('algo deu errado') || body.includes('something went wrong') || temRetry;
      }).catch(() => false);
      if (!temErro) break;
      logW('"Algo deu errado" detectado', { tentativa: t + 1 });
      const clicou = await page.evaluate(() => {
        const doc: any = (globalThis as any).document;
        const alvo = (Array.from(doc.querySelectorAll('button, [role="button"]')) as any[])
          .find((b) => /tentar novamente|try again/i.test(b.textContent || ''));
        if (alvo) { alvo.click(); return true; }
        return false;
      }).catch(() => false);
      if (!clicou) await page.reload({ timeout: 30_000, waitUntil: 'domcontentloaded' }).catch(() => undefined);
      await delayHumano(3000, 5000);
      if (t === 2) {
        return { ok: false, erro: 'TikTok devolveu "Algo deu errado" 3x. Reconecte/exporte a sessão e aguarde ~15min.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots, proxyUsado };
      }
    }
    await snap('01b-pronto-pra-upload');

    // ETAPA 2: upload do(s) arquivo(s) — input direto → filechooser → iframe
    let inputFile: any = await page
      .waitForSelector('input[type="file"]', { timeout: TIMEOUT_INPUT_FILE_MS, state: 'attached' })
      .catch(() => null);
    if (!inputFile) {
      logW('input direto não achado, tentando filechooser via botão');
      try {
        const [fileChooser] = await Promise.all([
          page.waitForEvent('filechooser', { timeout: 15_000 }),
          page.click('button[data-e2e="select_video_button"], button:has-text("Selecionar vídeos"), button:has-text("Select videos")', { timeout: 5_000 })
        ]);
        await fileChooser.setFiles(arquivos);
        inputFile = 'filechooser';
      } catch {
        let inputEmIframe: any = null;
        for (const frame of page.frames()) {
          try {
            const inp = await frame.$('input[type="file"]');
            if (inp) { inputEmIframe = inp; break; }
          } catch { /* ignora */ }
        }
        if (inputEmIframe) {
          await inputEmIframe.setInputFiles(arquivos);
          inputFile = 'iframe';
        } else {
          const urlFinal = page.url();
          const bodyText: string = await page.evaluate(() => {
            const doc: any = (globalThis as any).document;
            return (doc?.body?.innerText || '').slice(0, 500);
          }).catch(() => '');
          await snap('02-input-nao-achado');
          const causa: MotivoFalhaTT =
            bodyText.toLowerCase().includes('login') || urlFinal.includes('login') ? 'sessao_expirada'
              : bodyText.toLowerCase().includes('captcha') || bodyText.toLowerCase().includes('verify') ? 'captcha'
                : 'ui_mudou';
          return { ok: false, erro: 'Input de upload não encontrado (url=' + urlFinal.slice(0, 80) + ')', motivoFalha: causa, duracaoMs: Date.now() - start, screenshots, proxyUsado };
        }
      }
    } else {
      await inputFile.setInputFiles(arquivos);
    }
    logI('arquivo(s) enviado(s), aguardando processamento');
    await snap('02-arquivo-anexado');

    // ETAPA 3: aguardar upload — botão Publicar habilitado por 6s consecutivos
    const tUploadInicio = Date.now();
    let uploadOk = false;
    let progressoOk = false;
    let btnOk = false;
    while (Date.now() - tUploadInicio < TIMEOUT_POST_MS - 60_000) {
      if (!progressoOk) {
        try {
          const prog = await page.evaluate(() => {
            const doc: any = (globalThis as any).document;
            const sels = ['[data-e2e="progress"]', '[class*="progress"][class*="bar"]', '[class*="upload"][class*="progress"]', '[role="progressbar"]', '[class*="Progress"]'];
            let percent = -1;
            for (const sel of sels) {
              const el = doc?.querySelector(sel);
              if (!el) continue;
              const aria = el.getAttribute?.('aria-valuenow');
              if (aria) percent = Math.max(percent, parseInt(aria, 10) || -1);
              const m = (el.textContent || '').match(/(\d{1,3})\s*%/);
              if (m) percent = Math.max(percent, parseInt(m[1], 10));
            }
            const body = doc?.body?.innerText || '';
            return { percent, uploadedOk: /uploaded|carregado|complete|concluid|100\s*%/i.test(body) };
          });
          if (prog.percent >= 100 || prog.uploadedOk) progressoOk = true;
        } catch { /* ok */ }
      }
      if (!btnOk) {
        const btn = await page.$('button[data-e2e="post_video_button"], button:has-text("Publicar"), button:has-text("Post"), button:has-text("Agendar"), button:has-text("Schedule")').catch(() => null);
        if (btn) {
          const disabled = await btn.evaluate((el: any) => el.disabled || el.getAttribute('aria-disabled') === 'true').catch(() => true);
          if (!disabled) btnOk = true;
        }
      }
      // Detector de erro visível (captcha real / rate limit / formato / login)
      try {
        const err = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          const body = (doc?.body?.innerText || '').slice(0, 5000);
          const captchaEl = doc.querySelector('[class*="captcha" i]:not([class*="preview" i]), [id*="captcha" i], [data-captcha], [class*="secsdk-captcha" i], [class*="verify-slider" i], [class*="captcha_container" i], iframe[src*="captcha" i]');
          if (captchaEl) return { motivo: 'captcha', trecho: 'elemento captcha no DOM' };
          const padroes: Array<{ rx: RegExp; motivo: string }> = [
            { rx: /try again later|tente novamente mais tarde|rate limit|too many requests|violat[ae]/i, motivo: 'rate_limit' },
            { rx: /file format|formato.*inv[aá]lid|invalid.*file|unsupported.*(format|codec)/i, motivo: 'outro' },
            { rx: /file size.*(exceed|too large)|tamanho.*excedid|arquivo.*muito grande/i, motivo: 'outro' },
            { rx: /video.*too long|dura[cç][aã]o.*excedid|duration.*exceed/i, motivo: 'outro' },
            { rx: /(please )?log ?in|sign ?in( to| again)|autentique.*novamente|fa[cç]a login/i, motivo: 'sessao_expirada' },
            { rx: /(n[aã]o sou (um )?rob[oô]|not a robot|verify.*you.*(human|robot)|arraste.*(completar|quebra|puzzle)|slide.*(complete|puzzle)|deslize.*(completar|para)|drag.*to.*verify)/i, motivo: 'captcha' }
          ];
          for (const p of padroes) { const m = body.match(p.rx); if (m) return { motivo: p.motivo, trecho: (m[0] || '').slice(0, 100) }; }
          return null;
        });
        if (err) {
          await snap('03-erro-upload');
          return { ok: false, erro: 'TikTok mostrou: ' + err.trecho, motivoFalha: (err.motivo as MotivoFalhaTT) || 'outro', duracaoMs: Date.now() - start, screenshots, proxyUsado };
        }
      } catch { /* ok */ }
      // Botão habilitado por 6s consecutivos = upload concluído
      if (btnOk) {
        await new Promise((r) => setTimeout(r, 6000));
        const aindaOk = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          const btn = doc.querySelector('button[data-e2e="post_video_button"]') as any;
          if (!btn) return false;
          return !btn.disabled && btn.getAttribute('aria-disabled') !== 'true';
        }).catch(() => false);
        if (aindaOk) { uploadOk = true; break; }
        btnOk = false;
      }
      await new Promise((r) => setTimeout(r, 2500));
    }
    if (!uploadOk) {
      await snap('03-timeout-upload');
      return { ok: false, erro: 'Timeout no upload (progressoOk=' + progressoOk + ' btnOk=' + btnOk + ')', motivoFalha: 'timeout', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }
    await delayHumano(1500, 2500);
    await snap('03-upload-ok');

    // ETAPA 4: descrição (contenteditable) — fecha o dropdown de hashtag depois
    if (descricao) {
      const editor = await page.$(
        '[data-e2e="caption-editor"] [contenteditable="true"], [contenteditable="true"][data-placeholder], textarea[data-e2e="video_caption"], [data-e2e="video-caption"] [contenteditable="true"], div[role="textbox"][contenteditable="true"], .public-DraftEditor-content[contenteditable="true"], [contenteditable="true"][aria-label*="descri" i], [contenteditable="true"][aria-label*="caption" i], [contenteditable="true"][aria-label*="titulo" i]'
      ).catch(() => null);
      if (editor) {
        await editor.click();
        await delayHumano(300, 700);
        await page.keyboard.press('Control+A').catch(() => undefined);
        await page.keyboard.press('Delete').catch(() => undefined);
        await delayHumano(200, 500);
        await page.keyboard.type(descricao, { delay: 15 });
        await delayHumano(400, 800);
        try {
          await page.keyboard.press('Escape');
          await delayHumano(200, 400);
          await page.evaluate(() => {
            const doc: any = (globalThis as any).document;
            const active: any = doc.activeElement;
            if (active && typeof active.blur === 'function') active.blur();
            const drops = doc.querySelectorAll('[class*="autocomplete" i], [class*="suggest" i], [class*="dropdown" i][class*="hashtag" i], [role="listbox"]');
            for (const d of Array.from(drops) as any[]) { try { (d as any).style.display = 'none'; } catch { /* ok */ } try { (d as any).remove(); } catch { /* ok */ } }
          }).catch(() => undefined);
          await delayHumano(300, 500);
        } catch { /* ok */ }
        await snap('04-descricao');
      } else {
        logW('editor de descrição não achado — seguindo sem legenda');
      }
    }
    await delayHumano(1000, 2000);

    // ETAPA 5: privacidade (só pro teste "Somente você"). Público é o padrão do Studio.
    if (privacyAlvo === 'self') {
      try {
        const marcou = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          // Abre o seletor "Quem pode ver este vídeo" e escolhe "Somente você".
          const triggers = Array.from(doc.querySelectorAll('[class*="select" i], [role="combobox"], [data-e2e*="privacy" i], [class*="Visibility" i]')) as any[];
          for (const tg of triggers) {
            const t = (tg.innerText || tg.textContent || '').toLowerCase();
            if (/p[úu]blico|public|quem pode ver|who can view|seguidores|amigos|somente voc[êe]|only you/.test(t)) { try { tg.click(); } catch { /* ok */ } break; }
          }
          return true;
        }).catch(() => false);
        if (marcou) {
          await delayHumano(600, 1000);
          await page.evaluate(() => {
            const doc: any = (globalThis as any).document;
            const opts = Array.from(doc.querySelectorAll('[role="option"], li, [class*="option" i], [class*="item" i]')) as any[];
            const alvo = opts.find((o) => /somente voc[êe]|only you|private|privado/i.test(o.textContent || ''));
            if (alvo) { try { alvo.click(); } catch { /* ok */ } }
          }).catch(() => undefined);
          await delayHumano(400, 800);
          await snap('05-privacidade-self');
        }
      } catch { /* best-effort — se não achou, o teste sai público mesmo */ }
    }

    // ETAPA 6: remover joyride/modais bloqueadores antes do Publicar
    await page.evaluate(() => {
      const doc: any = (globalThis as any).document;
      const win: any = (globalThis as any).window;
      for (let i = 0; i < 3; i++) {
        const portais = Array.from(doc.querySelectorAll('#react-joyride-portal, [data-test-id="overlay"][class*="joyride"], .react-joyride__overlay'));
        for (const el of portais) { try { (el as any).remove(); } catch { /* ok */ } }
        if (portais.length === 0) break;
      }
      // Remove containers de modais "Entendi"/"Got it" e backdrops escuros cobrindo a tela
      const btns: any[] = Array.from(doc.querySelectorAll('button, div[role="button"], span[role="button"]'));
      for (const btn of btns) {
        const txt = (btn.innerText || btn.textContent || '').trim().toLowerCase();
        if (!['entendi', 'got it', 'i got it', 'ok, entendi', 'ok, got it'].includes(txt)) continue;
        if (btn.closest('[data-e2e="post_video_button"]')) continue;
        let container: any = btn;
        for (let i = 0; i < 8 && container.parentElement; i++) {
          container = container.parentElement;
          const cs = win.getComputedStyle(container);
          const zi = parseInt(cs.zIndex || '0', 10) || 0;
          if ((cs.position === 'fixed' || cs.position === 'absolute') && zi >= 100) { try { container.remove(); } catch { /* ok */ } break; }
        }
      }
      const overlays: any[] = Array.from(doc.querySelectorAll('div, section'));
      for (const el of overlays) {
        const cs = win.getComputedStyle(el);
        const zi = parseInt(cs.zIndex || '0', 10) || 0;
        if (cs.position !== 'fixed' || zi < 100) continue;
        const rect = el.getBoundingClientRect();
        const cobre = rect.width > win.innerWidth * 0.5 && rect.height > win.innerHeight * 0.5;
        const bg = cs.backgroundColor || '';
        if (cobre && (/rgba?\([^)]*0?\.[3-9]/.test(bg) || bg === 'rgba(0, 0, 0, 0.5)')) { try { el.remove(); } catch { /* ok */ } }
      }
    }).catch(() => undefined);
    await delayHumano(300, 600);

    // ETAPA 7: achar o botão Publicar (7 seletores + varredura por texto)
    async function acharBotaoFinal(): Promise<any> {
      const seletores = [
        'button[data-e2e="post_video_button"]', 'button[data-e2e*="post_video"]',
        'button[data-e2e*="publish"]', 'button[data-e2e*="post_button"]',
        'button[data-e2e*="submit"]', 'div[data-e2e="post_video_button"]',
        'div[role="button"][data-e2e*="post"]'
      ];
      const achado = await Promise.race(seletores.map((s) => page.waitForSelector(s, { timeout: 12_000, state: 'visible' }).catch(() => null)));
      if (achado) return achado;
      try {
        const h = await page.evaluateHandle(() => {
          const doc: any = (globalThis as any).document;
          const rx = /^(publicar|postar|post|publish|agendar|schedule|programar)$/i;
          const viz = (Array.from(doc.querySelectorAll('button, div[role="button"], [class*="Button__root"]')) as any[]).filter((el) => {
            const t = (el.textContent || '').trim();
            if (!rx.test(t)) return false;
            if (el.disabled || el.getAttribute('aria-disabled') === 'true') return false;
            const r = el.getBoundingClientRect();
            if (r.width < 60 || r.height < 20) return false;
            const st = doc.defaultView.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none' || st.opacity === '0') return false;
            return true;
          });
          viz.sort((a, b) => b.getBoundingClientRect().bottom - a.getBoundingClientRect().bottom);
          return viz[0] || null;
        });
        const el = h.asElement();
        if (el) return el;
      } catch { /* ok */ }
      return null;
    }
    const btnFinal = await acharBotaoFinal();
    if (!btnFinal) {
      await snap('07-botao-nao-achado');
      const telaErro = await page.evaluate(() => {
        const doc: any = (globalThis as any).document;
        const t = ((doc?.body?.innerText || '') + ' ' + (doc?.body?.textContent || '')).toLowerCase();
        const btnRetry = Array.from(doc?.querySelectorAll('button, [role="button"]') || []).some((b: any) => /tentar novamente|try again/i.test(b.textContent || ''));
        return t.includes('algo deu errado') || t.includes('something went wrong') || btnRetry;
      }).catch(() => false);
      if (telaErro) {
        return { ok: false, erro: 'TikTok devolveu "Algo deu errado". Reconecte/exporte a sessão.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots, proxyUsado };
      }
      return { ok: false, erro: 'Botão publicar não encontrado', motivoFalha: 'ui_mudou', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    // ETAPA 8: confirmação por network (mais confiável que URL) — monitora resposta de sucesso
    const urlAntes = page.url();
    const networkSucessoPromise: Promise<boolean> = page
      .waitForResponse((resp: any) => {
        try {
          const u = resp.url();
          const match = /aweme\/(v[12]|post\/v1)\/(upload\/status|create\/aweme|post\/publish|creator|item\/create|aweme\/create_post)|\/api\/(creator|upload_video|tiktokstudio|video)\/(publish|create|save|post)|\/tiktok\/v1\/(upload\/status|creator\/publish|publish)|\/creator_center\/api\/.*\/publish|\/business\/publish/i.test(u);
          if (!match) return false;
          const st = resp.status();
          if (st >= 200 && st < 300) return true;
          if (st === 302) {
            const loc = (resp.headers()?.['location'] || resp.headers()?.['Location'] || '') as string;
            if (/\/tiktokstudio\/(content|manage)/i.test(loc)) return true;
          }
          return false;
        } catch { return false; }
      }, { timeout: 60_000 })
      .then(async (resp: any) => {
        try {
          const json = await resp.json().catch(() => null);
          if (json && (json.status_code === 0 || json.status === 'ok' || json.success === true || json.aweme_id || json.item_id)) return true;
          return false;
        } catch { return false; }
      })
      .catch(() => false);

    // ETAPA 9: clicar Publicar — sequência completa de PointerEvent + fallbacks
    await snap('08-antes-publicar');
    const clickInterno: boolean = await page.evaluate(() => {
      const doc: any = (globalThis as any).document;
      const jr = doc.querySelector('#react-joyride-portal'); if (jr) jr.remove();
      for (const o of Array.from(doc.querySelectorAll('.react-joyride__overlay, [data-test-id="overlay"]'))) { try { (o as any).remove(); } catch { /* ok */ } }
      const btn = doc.querySelector('button[data-e2e="post_video_button"]') as any;
      if (!btn) return false;
      try {
        const rect = btn.getBoundingClientRect();
        const cx = rect.left + rect.width / 2;
        const cy = rect.top + rect.height / 2;
        const win: any = (globalThis as any).window;
        const init: any = { bubbles: true, cancelable: true, composed: true, view: win, clientX: cx, clientY: cy, screenX: cx, screenY: cy, button: 0, buttons: 1, pointerType: 'mouse', pointerId: 1, isPrimary: true };
        btn.dispatchEvent(new win.PointerEvent('pointerover', init));
        btn.dispatchEvent(new win.PointerEvent('pointerenter', init));
        btn.dispatchEvent(new win.PointerEvent('pointerdown', init));
        btn.dispatchEvent(new win.MouseEvent('mousedown', init));
        btn.dispatchEvent(new win.PointerEvent('pointerup', { ...init, buttons: 0 }));
        btn.dispatchEvent(new win.MouseEvent('mouseup', { ...init, buttons: 0 }));
        btn.dispatchEvent(new win.MouseEvent('click', { ...init, buttons: 0, detail: 1 }));
        try { btn.click(); } catch { /* ok */ }
        return true;
      } catch { return false; }
    }).catch(() => false);
    if (!clickInterno) {
      await btnFinal.click({ force: true }).catch(() => undefined);
    }
    // Reforço: scrollIntoView + Playwright click
    await new Promise((r) => setTimeout(r, 800));
    try {
      await btnFinal.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => undefined);
      await new Promise((r) => setTimeout(r, 200));
      await btnFinal.click({ timeout: 5000, delay: 50, force: false });
    } catch { /* o click JS acima já pode ter resolvido */ }
    logI('botão Publicar clicado', { urlAntes, viaJs: clickInterno });
    await snap('09-publicar-clicado');

    // ETAPA 10: confirmação (network → URL → botão sumiu → toast → modal "Continuar publicando")
    await delayHumano(3000, 5000);
    let postUrl: string | undefined;
    let confirmadoPorNetwork = false;
    let confirmadoPorUrl = false;
    let confirmadoPorBotao = false;
    let confirmadoPorToast = false;

    const raceNet = await Promise.race([networkSucessoPromise, new Promise<boolean>((r) => setTimeout(() => r(false), 15_000))]).catch(() => false);
    if (raceNet) confirmadoPorNetwork = true;

    if (!confirmadoPorNetwork) {
      await page.waitForURL((url: any) => {
        const s = String(url);
        return s.includes('/tiktokstudio/content') || s.includes('/tiktokstudio/manage') || s.includes('/tiktokstudio/videos') || s.includes('/tiktokstudio/home') || s.includes('/upload/done') || s.includes('/success') || /[?&](state=published|success=1|posted=1)/.test(s) || s.includes('#success');
      }, { timeout: 45_000 }).catch(() => undefined);
      const urlDepois = page.url();
      if (urlDepois !== urlAntes && (urlDepois.includes('/tiktokstudio/content') || urlDepois.includes('/tiktokstudio/manage') || urlDepois.includes('/tiktokstudio/videos') || urlDepois.includes('/tiktokstudio/home') || urlDepois.includes('/upload/done') || urlDepois.includes('/success') || /[?&](state=published|success=1|posted=1)/.test(urlDepois) || urlDepois.includes('#success'))) {
        confirmadoPorUrl = true;
        postUrl = urlDepois;
      }
    }

    // Toast de sucesso / erro inequívoco
    if (!confirmadoPorUrl && !confirmadoPorNetwork) {
      const feedback = await page.evaluate(() => {
        const doc: any = (globalThis as any).document;
        const alertSel = ['[role="alert"]', '[role="status"]', '.toast', '[class*="toast" i]', '[class*="Toast" i]', '[class*="notification" i]', '[class*="snackbar" i]', '[data-e2e*="toast" i]', '[data-e2e*="notice" i]', '[data-e2e*="success" i]', '[data-e2e*="error" i]'].join(', ');
        const alertText = (Array.from(doc.querySelectorAll(alertSel)) as any[]).map((n) => (n.innerText || n.textContent || '').trim()).filter((t) => t.length > 0).join(' | ').slice(0, 2000);
        const body = (doc?.body?.innerText || '').slice(0, 4000);
        const rxSucesso = /post.*successful|publicad|posted|success|sucesso|enviado.*com sucesso|published/i;
        const rxErro = /(post (was )?(failed|rejected)|publica[cç][aã]o (falhou|foi rejeitada)|upload failed|falha no upload|video (was )?(rejected|violates)|video foi rejeitado|was not published|nao foi publicad|content violates our (community )?(guidelines|policy)|conte[uú]do viola (nossas )?(diretrizes|pol[ií]ticas)|blocked from posting|bloqueado de postar|account (has been )?(restricted|suspended)|conta (foi )?(restringida|suspensa))/i;
        const erro = alertText.match(rxErro) || body.match(rxErro);
        return { sucesso: rxSucesso.test(body), erroTrecho: erro ? erro[0] : null };
      }).catch(() => ({ sucesso: false, erroTrecho: null }));
      if (feedback.erroTrecho) {
        await snap('10-rejeitado');
        return { ok: false, erro: 'TikTok rejeitou: ' + feedback.erroTrecho, motivoFalha: 'outro', duracaoMs: Date.now() - start, screenshots, proxyUsado };
      }
      if (feedback.sucesso) confirmadoPorToast = true;
    }

    // URL mudou / botão sumiu-desabilitou-virou "Publicando..." (até 60s)
    if (!confirmadoPorUrl && !confirmadoPorToast && !confirmadoPorNetwork) {
      const t0 = Date.now();
      while (Date.now() - t0 < 60_000) {
        const urlAtual = page.url();
        if (urlAtual !== urlAntes && !urlAtual.includes('/tiktokstudio/upload')) { confirmadoPorUrl = true; postUrl = urlAtual; break; }
        const sit = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          const btn = doc.querySelector('button[data-e2e="post_video_button"]') as any;
          if (!btn) return { existe: false, disabled: false, loading: false, textoMudou: false };
          const txt = (btn.textContent || '').trim().toLowerCase();
          const loading = !!btn.querySelector('[class*="spinner" i], [class*="loading" i], [class*="Loading" i]') || btn.getAttribute('aria-busy') === 'true' || /publicando|publishing|posting|enviando|carregando/i.test(txt);
          const textoOriginal = /^(publicar|post|publish|agendar|schedule)$/i.test(txt);
          return { existe: true, disabled: !!btn.disabled || btn.getAttribute('aria-disabled') === 'true', loading, textoMudou: !textoOriginal && txt.length > 0 };
        }).catch(() => ({ existe: true, disabled: false, loading: false, textoMudou: false }));
        if (!sit.existe || sit.disabled || sit.loading || sit.textoMudou) { confirmadoPorBotao = true; break; }
        const fb = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          const body = (doc?.body?.innerText || '').slice(0, 4000);
          return { sucesso: /post.*successful|publicad|posted|success|sucesso|enviado.*com sucesso|published/i.test(body) };
        }).catch(() => ({ sucesso: false }));
        if (fb.sucesso) { confirmadoPorToast = true; break; }
        await new Promise((r) => setTimeout(r, 2000));
      }
    }

    // Modal "Continuar publicando?" (TikTok mostra quando detecta possível problema no vídeo)
    if (!confirmadoPorUrl && !confirmadoPorToast && !confirmadoPorBotao && !confirmadoPorNetwork) {
      try {
        const modal = await page.evaluate(() => {
          const doc: any = (globalThis as any).document;
          const win: any = doc.defaultView;
          const txt = ((doc?.body?.innerText || '') + ' ' + (doc?.body?.textContent || '')).toLowerCase();
          if (!(txt.includes('continuar publicando') || txt.includes('continue publishing'))) return { encontrado: false };
          const sels = ['[role="dialog"]', '[aria-modal="true"]', '[class*="Modal"]', '[class*="modal"]', '[class*="Dialog"]', '[class*="dialog"]', '[class*="Overlay"]', '[class*="overlay"]', '[class*="Popup"]', '[class*="popup"]'];
          let modalEl: any = null;
          for (const sel of sels) {
            for (const el of Array.from(doc.querySelectorAll(sel)) as any[]) {
              const t = (el.textContent || '').toLowerCase();
              if (t.includes('continuar publicando') || t.includes('continue publishing')) {
                const r = el.getBoundingClientRect();
                const st = win.getComputedStyle(el);
                if (r.width > 100 && r.height > 100 && st.visibility !== 'hidden' && st.display !== 'none') { modalEl = el; break; }
              }
            }
            if (modalEl) break;
          }
          if (!modalEl) return { encontrado: true, botaoAchado: false };
          const alvo = (Array.from(modalEl.querySelectorAll('button, [role="button"]')) as any[]).find((b) => {
            const t = (b.textContent || '').trim();
            if (!/^(publicar|publish|continuar|continue|confirmar|confirm|sim|yes)$/i.test(t)) return false;
            const r = b.getBoundingClientRect();
            return r.width >= 40 && r.height >= 20;
          });
          if (!alvo) return { encontrado: true, botaoAchado: false };
          const r = alvo.getBoundingClientRect();
          return { encontrado: true, botaoAchado: true, x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
        }).catch(() => ({ encontrado: false }));
        if ((modal as any).encontrado && (modal as any).botaoAchado) {
          await page.mouse.click((modal as any).x, (modal as any).y);
          await new Promise((r) => setTimeout(r, 8000));
          const urlPos = page.url();
          if (urlPos !== urlAntes && !urlPos.includes('/tiktokstudio/upload')) { confirmadoPorUrl = true; postUrl = urlPos; }
        }
      } catch { /* ok */ }
    }

    if (!confirmadoPorUrl && !confirmadoPorToast && !confirmadoPorBotao && !confirmadoPorNetwork) {
      await snap('10-sem-confirmacao');
      const urlAg = page.url();
      return { ok: false, erro: 'Publicação não confirmada em 60s. Confira em tiktok.com/tiktokstudio/manage (urlAgora=' + urlAg + ')', motivoFalha: 'outro', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    await snap('10-publicado');
    logI('post concluído', { postUrl, confirmadoPorNetwork, confirmadoPorUrl, confirmadoPorBotao, confirmadoPorToast });
    return { ok: true, postUrl, duracaoMs: Date.now() - start, screenshots, proxyUsado };
  } catch (e) {
    const msg = (e as Error).message;
    let motivo: MotivoFalhaTT = 'outro';
    if (/timeout/i.test(msg)) motivo = 'timeout';
    else if (/captcha|verify/i.test(msg)) motivo = 'captcha';
    else if (/rate.?limit|too many/i.test(msg)) motivo = 'rate_limit';
    logW('erro publicando', { projeto, erro: msg });
    try { await snap('erro-final'); } catch { /* ok */ }
    return { ok: false, erro: msg, motivoFalha: motivo, duracaoMs: Date.now() - start, screenshots, proxyUsado };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}
