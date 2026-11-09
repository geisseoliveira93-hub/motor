/**
 * [+yt-browser-nuvem 28/10] YouTube via navegador invisível NA NUVEM (motor/VPS).
 *
 * Porte fiel do youtube-playwright.ts do PC (navegador invisível que posta via
 * automação no YouTube Studio), mas SEM nada de Electron: usa Playwright puro
 * (chromium.launch headless) e lê a sessão logada do cofre em /data/yt-sessions/<projeto>.json
 * (gravada pelo app via POST /pub/youtube/sessao-nuvem).
 *
 * Mesmo fluxo validado do PC (estável em 2026):
 *   Studio → Criar → Enviar vídeos → input[type=file] → título/descrição via execCommand
 *   → capa (opcional) → "Não para crianças" → wizard (monetização + autocertificação)
 *   → Visibilidade PUBLIC → Publicar → aguardar verificação → confirmar "publicar mesmo assim"
 *   → capturar URL youtu.be / shorts.
 *
 * Diferenças vs. PC (de propósito, por ser servidor headless sem Electron):
 *   - delayHumano  = sleep com jitter (igual ideia, sem base compartilhada)
 *   - clicarNativo = page.evaluate(el.click())  (idêntico ao do PC)
 *   - snap         = screenshot opcional em /data/yt-sessions/_snaps (best-effort, nunca derruba)
 *   - sem tentarClicarComIA: cai direto no clique nativo/Playwright (plano do PC já tinha esse caminho)
 *
 * ⚠️ RISCO de IP de datacenter: o Google pode desafiar um login vindo de IP de VPS.
 * Se cair em accounts.google.com, devolvemos sessao_expirada (igual ao PC) — nesse caso
 * pode ser necessário um proxy residencial BR (mesma razão do TikTok).
 */

import * as fs from 'fs';
import * as net from 'net';
import * as path from 'path';

// Navegador DISFARÇADO: patchright (igual ao PC) com fallback pro playwright comum. Lazy import
// porque só é usado aqui — mantém o boot do engine leve e não quebra se um dos dois faltar.
async function carregarChromium(): Promise<any> {
  const mod: any = await import('patchright').catch(() => import('playwright'));
  return mod.chromium;
}

const USER_AGENT_DEFAULT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36';

const YT_SESS_DIR = '/data/yt-sessions';
const SNAP_DIR = '/data/media/yt-snaps'; // servido publicamente em /media/yt-snaps/ (pra conferir os prints por link)
const PROXY_FILE = YT_SESS_DIR + '/_proxies.json';

// [+proxy-residencial 28/10] Lista de proxies residenciais (os aparelhos de casa, via Tailscale),
// em ordem de prioridade. A nuvem testa um a um e usa o PRIMEIRO que estiver online (failover).
export interface ProxyNode { nome?: string; host: string; port: number; usuario?: string; senha?: string }

function lerProxies(): ProxyNode[] {
  try {
    const d = JSON.parse(fs.readFileSync(PROXY_FILE, 'utf8'));
    const lista = Array.isArray(d) ? d : (d && Array.isArray(d.lista) ? d.lista : []);
    return lista.filter((p: any) => p && p.host && p.port).map((p: any) => ({
      nome: p.nome || (p.host + ':' + p.port),
      host: String(p.host), port: Number(p.port),
      usuario: p.usuario || undefined, senha: p.senha || undefined
    }));
  } catch { return []; }
}

// Testa se um proxy está vivo E funcionando: abre CONNECT até www.google.com:443 por ele e
// espera o "200 Connection Established". Timeout curto. Resolve true/false (nunca lança).
function testarProxy(p: ProxyNode, timeoutMs = 6000): Promise<boolean> {
  return new Promise((resolve) => {
    let feito = false;
    const fim = (ok: boolean) => { if (!feito) { feito = true; try { sock.destroy(); } catch { /* ok */ } resolve(ok); } };
    const sock = net.connect(p.port, p.host);
    sock.setTimeout(timeoutMs);
    sock.on('timeout', () => fim(false));
    sock.on('error', () => fim(false));
    sock.on('connect', () => {
      let linha = 'CONNECT www.google.com:443 HTTP/1.1\r\nHost: www.google.com:443\r\n';
      if (p.usuario) linha += 'Proxy-Authorization: Basic ' + Buffer.from((p.usuario || '') + ':' + (p.senha || '')).toString('base64') + '\r\n';
      linha += '\r\n';
      sock.write(linha);
    });
    let buf = '';
    sock.on('data', (d) => {
      buf += d.toString('latin1');
      if (buf.indexOf('\r\n\r\n') !== -1) fim(/^HTTP\/1\.[01]\s+200/i.test(buf));
    });
  });
}

// Testa TODOS os proxies da lista (diagnóstico). Retorna [{nome,host,port,online}].
export async function statusProxies(): Promise<any[]> {
  const lista = lerProxies();
  const out: any[] = [];
  for (const p of lista) {
    const online = await testarProxy(p).catch(() => false);
    out.push({ nome: p.nome, host: p.host, port: p.port, temAuth: !!p.usuario, online });
  }
  return out;
}

// Escolhe o 1º proxy online da lista. Retorna {node, playwright} ou null (sem lista / nenhum online).
export async function escolherProxy(): Promise<{ node: ProxyNode; playwright: any } | null> {
  const lista = lerProxies();
  if (!lista.length) return null;
  for (const p of lista) {
    const ok = await testarProxy(p).catch(() => false);
    if (ok) {
      const auth = p.usuario ? { username: p.usuario, password: p.senha || '' } : {};
      return { node: p, playwright: { server: 'http://' + p.host + ':' + p.port, ...auth } };
    }
  }
  return null;
}

export interface PublicarNuvemPayload {
  projeto: string; // teclado | baixo | violao | comunidade
  arquivo: string; // caminho absoluto do vídeo no servidor
  titulo: string;
  descricao?: string;
  thumbnailPath?: string; // caminho absoluto da capa no servidor (opcional)
  privacy?: 'public' | 'private' | 'unlisted'; // default public
  snaps?: boolean; // se true, salva screenshots de diagnóstico
}

export interface PublicarNuvemResposta {
  ok: boolean;
  postUrl?: string;
  rascunho?: boolean; // [+publish-fix 02/11] true = publicou o upload mas NÃO confirmou a saída do rascunho
  erro?: string;
  motivoFalha?: 'sessao_expirada' | 'sem_sessao' | 'arquivo_inexistente' | 'ui_mudou' | 'outro';
  duracaoMs: number;
  screenshots?: string[];
  proxyUsado?: string | null;
}

function logI(msg: string, extra?: any): void {
  try { console.log('[yt-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* ok */ }
}
function logW(msg: string, extra?: any): void {
  try { console.warn('[yt-nuvem]', msg, extra ? JSON.stringify(extra) : ''); } catch { /* ok */ }
}

function delayHumano(min: number, max: number): Promise<void> {
  const ms = Math.floor(min + Math.random() * Math.max(0, max - min));
  return new Promise((r) => setTimeout(r, ms));
}

/** Lê a sessão do cofre (gravada pelo PC via POST /pub/youtube/sessao-nuvem). */
function lerSessao(projeto: string): { storageState: any; userAgent: string; channelId: string | null } | null {
  try {
    const f = YT_SESS_DIR + '/' + projeto + '.json';
    const d = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (!d || !d.storageState || typeof d.storageState !== 'object') return null;
    return {
      storageState: d.storageState,
      userAgent: d.userAgent || USER_AGENT_DEFAULT,
      channelId: d.channelId || null
    };
  } catch {
    return null;
  }
}

export async function publicarYoutubeNuvem(payload: PublicarNuvemPayload): Promise<PublicarNuvemResposta> {
  const start = Date.now();
  const projeto = String(payload.projeto || '').trim().toLowerCase();
  const arquivo = payload.arquivo;
  const titulo = payload.titulo || 'Video sem titulo';
  const descricao = payload.descricao || '';
  const thumbnailPath = payload.thumbnailPath || '';
  const privacyAlvo = (payload.privacy || 'public').toUpperCase(); // PUBLIC | PRIVATE | UNLISTED
  const screenshots: string[] = [];
  const querSnaps = !!payload.snaps;

  logI('iniciando publicacao via navegador invisível (nuvem)', { projeto, titulo, arquivo: String(arquivo).slice(-60) });

  if (!projeto) {
    return { ok: false, erro: 'projeto obrigatório', motivoFalha: 'outro', duracaoMs: Date.now() - start };
  }
  if (!arquivo || !fs.existsSync(arquivo)) {
    return { ok: false, erro: 'arquivo de vídeo não encontrado no servidor: ' + arquivo, motivoFalha: 'arquivo_inexistente', duracaoMs: Date.now() - start };
  }
  const sess = lerSessao(projeto);
  if (!sess) {
    return { ok: false, erro: 'Sessão do YouTube não está no cofre da nuvem. Exporte a sessão pelo app (botão de exportar sessão p/ nuvem).', motivoFalha: 'sem_sessao', duracaoMs: Date.now() - start };
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

  // Clique NATIVO via evaluate (el.click()). Atravessa o scrim que o YouTube põe
  // sobre o formulário enquanto um vídeo grande sobe (o clique do Playwright é
  // bloqueado, o nativo não). Idêntico ao do PC.
  async function clicarNativo(sel: string): Promise<boolean> {
    if (!page) return false;
    return await page
      .evaluate((s: string) => {
        const el = (globalThis as any).document.querySelector(s);
        if (!el) return false;
        if (el.disabled || el.hasAttribute('disabled')) return false;
        el.click();
        return true;
      }, sel)
      .catch(() => false);
  }

  let proxyUsado: string | null = null;
  try {
    // [+proxy-residencial] Escolhe o proxy residencial online (sai pelo IP de casa). Se há lista
    // configurada mas NENHUM está online, aborta com erro claro (não arrisca o IP de datacenter).
    const temLista = lerProxies().length > 0;
    const escolhido = await escolherProxy();
    if (temLista && !escolhido) {
      return { ok: false, erro: 'Nenhum aparelho de casa (proxy residencial) está online agora. Ligue um PC/celular da lista e tente de novo.', motivoFalha: 'outro', duracaoMs: Date.now() - start, screenshots };
    }
    const launchOpts: any = {
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    };
    if (escolhido) {
      launchOpts.proxy = escolhido.playwright;
      proxyUsado = escolhido.node.nome || escolhido.node.host;
      logI('usando proxy residencial', { proxy: proxyUsado });
    } else {
      logW('SEM proxy residencial configurado — saindo pelo IP do servidor (datacenter); o Google provavelmente vai recusar');
    }
    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({
      storageState: sess.storageState,
      userAgent: sess.userAgent || USER_AGENT_DEFAULT,
      viewport: { width: 1400, height: 900 },
      locale: 'pt-BR'
    });
    page = await ctx.newPage();
    await page.setExtraHTTPHeaders({ 'User-Agent': sess.userAgent || USER_AGENT_DEFAULT });
    // Sobrescreve navigator.userAgent ANTES de qualquer script (tela "navegador incompatível").
    try {
      await page.addInitScript((ua: string) => {
        try {
          const nav = (globalThis as unknown as { navigator: object }).navigator;
          Object.defineProperty(nav, 'userAgent', { get: () => ua });
        } catch { /* ignore */ }
      }, sess.userAgent || USER_AGENT_DEFAULT);
    } catch { /* best-effort */ }

    // ETAPA 1: abrir o diálogo de upload PELO STUDIO (www.youtube.com/upload cai na tela de navegador incompatível).
    logI('abrindo Studio pra upload');
    await page.goto('https://studio.youtube.com', { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await delayHumano(2500, 4000);
    await snap('01-studio-home');

    if (/accounts\.google\.com/.test(page.url())) {
      return { ok: false, erro: 'Sessão YT expirou (redirecionou pro login do Google). Pode ser desafio de IP de datacenter — talvez precise de proxy residencial BR.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    // Criar → Enviar vídeos
    await page
      .locator('button[aria-label="Criar"], ytcp-button.ytcpAppHeaderCreateIcon, ytcp-button#create-icon')
      .first()
      .click({ timeout: 8000 })
      .catch(() => {});
    await delayHumano(700, 1300);
    await page
      .locator('tp-yt-paper-item[test-id="upload"], tp-yt-paper-item:has-text("Enviar vídeos"), tp-yt-paper-item:has-text("Enviar vídeo"), tp-yt-paper-item:has-text("Upload")')
      .first()
      .click({ timeout: 8000 })
      .catch(() => {});
    await delayHumano(1500, 2500);
    await snap('01b-dialog-upload');

    if (/accounts\.google\.com/.test(page.url())) {
      return { ok: false, erro: 'Sessão YT expirou. Reconecte/exporte a sessão de novo.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots, proxyUsado };
    }

    // ETAPA 2: upload do arquivo
    logI('localizando input de upload');
    const fileInput = page.locator('input[type="file"]').first();
    await fileInput.waitFor({ state: 'attached', timeout: 15_000 });
    logI('fazendo upload', { arquivo });
    await fileInput.setInputFiles(arquivo);
    await delayHumano(3000, 5000);
    await snap('02-arquivo-anexado');

    // ETAPA 3: aguardar form de Details (ancora nos IDs estáveis, NÃO em [label], que é traduzido)
    const SEL_TITULO = [
      '#title-textarea #textbox',
      'ytcp-social-suggestions-textbox#title-textarea #textbox',
      'ytcp-mention-textbox[label="Title"] #textbox',
      'ytcp-mention-textbox[label="Título"] #textbox'
    ].join(', ');
    const SEL_DESC = [
      '#description-textarea #textbox',
      'ytcp-social-suggestions-textbox#description-textarea #textbox',
      'ytcp-mention-textbox[label="Description"] #textbox',
      'ytcp-mention-textbox[label="Descrição"] #textbox'
    ].join(', ');

    logI('aguardando upload completar');
    try {
      await page.waitForSelector(SEL_TITULO, { timeout: 120_000 });
      await delayHumano(1500, 2500);
      await snap('03-form-details-visivel');
    } catch {
      await snap('03-timeout-upload');
      throw new Error('Timeout aguardando YouTube preparar form de Details');
    }

    // ETAPA 4: título (via execCommand — atravessa o scrim do upload)
    logI('preenchendo titulo');
    const okTitulo = await page
      .evaluate((texto: string) => {
        const doc = (globalThis as any).document;
        const box =
          doc.querySelector('#title-textarea #textbox') ||
          doc.querySelector('ytcp-social-suggestions-textbox#title-textarea #textbox');
        if (!box) return false;
        try { box.scrollIntoView({ block: 'center' }); } catch { /* ignore */ }
        box.focus();
        const sel = (globalThis as any).getSelection();
        const range = doc.createRange();
        range.selectNodeContents(box);
        sel.removeAllRanges();
        sel.addRange(range);
        doc.execCommand('insertText', false, texto);
        const agora = box.textContent || '';
        return agora.indexOf(texto.slice(0, 20)) !== -1;
      }, titulo.slice(0, 100))
      .catch(() => false);
    await delayHumano(400, 800);
    if (!okTitulo) {
      try {
        const tituloBox = page.locator(SEL_TITULO).first();
        await tituloBox.click({ timeout: 8000 });
        await page.keyboard.press('Control+A');
        await page.keyboard.press('Delete');
        await tituloBox.pressSequentially(titulo.slice(0, 100), { delay: 20 });
      } catch {
        logW('título pode não ter entrado (seguindo mesmo assim)');
      }
    }
    await snap('04-titulo-preenchido');

    // ETAPA 5: descrição (best-effort — nunca derruba a publicação)
    if (descricao && descricao.trim()) {
      try {
        logI('preenchendo descricao');
        await page.waitForSelector(SEL_DESC, { timeout: 8000 }).catch(() => {});
        await delayHumano(300, 600);
        const okDesc = await page
          .evaluate((texto: string) => {
            const doc = (globalThis as any).document;
            const box =
              doc.querySelector('#description-textarea #textbox') ||
              doc.querySelector('ytcp-social-suggestions-textbox#description-textarea #textbox');
            if (!box) return false;
            try { box.scrollIntoView({ block: 'center' }); } catch { /* ignore */ }
            box.focus();
            const sel = (globalThis as any).getSelection();
            const range = doc.createRange();
            range.selectNodeContents(box);
            sel.removeAllRanges();
            sel.addRange(range);
            doc.execCommand('insertText', false, texto);
            const agora = box.textContent || '';
            return agora.indexOf(texto.slice(0, 24)) !== -1;
          }, descricao.slice(0, 4900))
          .catch(() => false);
        await delayHumano(400, 800);
        await snap(okDesc ? '05-descricao-preenchida' : '05-descricao-vazia');
        if (!okDesc) logW('descrição pode não ter entrado (seguindo mesmo assim)');
      } catch (e) {
        logW('não consegui preencher a descrição (seguindo sem)', { erro: (e as Error).message });
        await snap('05-descricao-falhou');
      }
    }

    // ETAPA 5b: capa (thumbnail) — best-effort, mas ROBUSTO.
    // [+capa-fix 02/11] BUG antigo: o seletor incluía '#file-loader', que é o input do VÍDEO (ytcp-uploads-file-picker).
    // Em vídeos LONGOS (upload ainda rolando) esse input continua no DOM, então .first() pegava ELE e o setInputFiles
    // jogava a capa no lugar errado → vídeo saía SEM capa (acontecia no Teclado/Comunidade, 15 min; nos curtos
    // Baixo/Violão o input do vídeo já tinha sumido e pegava o certo). Agora usa SÓ o input do uploader de miniatura,
    // espera ele aparecer (até 30s), anexa, detecta recusa do YouTube (formato/resolução/tamanho) e tenta de novo 1x.
    if (thumbnailPath && fs.existsSync(thumbnailPath)) {
      const anexarCapa = async (): Promise<'ok' | 'sem-campo' | 'recusada'> => {
        const inputCapa = page.locator('ytcp-thumbnail-uploader input[type="file"]').first();
        try { await inputCapa.waitFor({ state: 'attached', timeout: 30_000 }); } catch { return 'sem-campo'; }
        try { await page.evaluate(() => { const u: any = (globalThis as any).document.querySelector('ytcp-thumbnail-uploader'); if (u && u.scrollIntoView) u.scrollIntoView({ block: 'center' }); }); } catch { /* ok */ }
        await inputCapa.setInputFiles(thumbnailPath).catch(() => { /* segue pra verificar */ });
        await delayHumano(2500, 4000);
        const erroCapa = await page
          .evaluate(() => {
            const doc: any = (globalThis as any).document;
            const up = doc.querySelector('ytcp-thumbnail-uploader');
            const t = (((up && up.innerText) || '') + ' ' + (doc.body?.innerText || '')).slice(0, 6000);
            const m = t.match(/(n[ãa]o foi poss[íi]vel[^.\n]*miniatura|miniatura[^.\n]*n[ãa]o[^.\n]*(suport|compat)|formato[^.\n]*(n[ãa]o|inv[áa]lid|incompat)|resolu[çc][ãa]o[^.\n]*(baixa|m[íi]nim)|arquivo[^.\n]*(muito grande|grande demais)|2\s*MB|m[áa]ximo de 2|invalid (image|file|format)|unsupported|too large)/i);
            return m ? m[0].replace(/\s+/g, ' ').slice(0, 120) : '';
          })
          .catch(() => '');
        if (erroCapa) { logW('YouTube recusou a capa', { motivo: erroCapa }); return 'recusada'; }
        return 'ok';
      };
      let res5b = await anexarCapa();
      if (res5b === 'sem-campo') { await delayHumano(3000, 5000); res5b = await anexarCapa(); }
      if (res5b === 'ok') { logI('capa anexada'); await snap('05b-capa-anexada'); }
      else if (res5b === 'recusada') { await snap('05b-capa-recusada'); }
      else { logW('campo da capa não apareceu (seguindo sem ela)'); await snap('05b-capa-falhou'); }
    }

    // ETAPA 6: Not made for kids (clique nativo — atravessa o scrim)
    logI('marcando "Nao para criancas"');
    try {
      const okKids = await clicarNativo('tp-yt-paper-radio-button[name="VIDEO_MADE_FOR_KIDS_NOT_MFK"]');
      if (!okKids) {
        await page.click('tp-yt-paper-radio-button[name="VIDEO_MADE_FOR_KIDS_NOT_MFK"]', { timeout: 10_000 });
      }
      await delayHumano(500, 1000);
      await snap('06-nao-para-criancas');
    } catch (e) {
      logW('nao foi possivel marcar Not for kids (continuando)', { erro: (e as Error).message });
    }

    // ETAPA 7: avançar pelo wizard (Monetização + Autocertificação), parando na Visibilidade.
    let autocertEnviada = false; // depois de enviar 1x, o questionário fica TRAVADO na tela — não re-tenta
    async function preencherAutoCertificacao(): Promise<boolean> {
      if (autocertEnviada) return false;
      const totalRadios = await page
        .locator('tp-yt-paper-radio-button[name^="VIDEO_SELF_CERTIFICATION_ANSWER_"]')
        .count()
        .catch(() => 0);
      if (!totalRadios) return false;
      // Já foi enviada numa passada anterior? (os campos continuam na tela, só travados) → não re-tenta.
      const jaBloqueada = await page
        .evaluate(() => {
          const t = ((globalThis as any).document.body?.innerText || '');
          return /bloqueado porque você enviou|bloqueado porque voce enviou|classifica[çc][ãa]o enviada/i.test(t);
        })
        .catch(() => false);
      if (jaBloqueada) { autocertEnviada = true; return false; }
      logI('autocertificacao detectada — expandindo e marcando "nao contem" em tudo', { totalRadios });
      await page
        .evaluate(async () => {
          const doc = (globalThis as any).document;
          const panels = Array.from(doc.querySelectorAll('ytcp-expansion-panel')).filter((p: any) =>
            /VIDEO_SELF_CERTIFICATION_QUESTION_/.test(p.id)
          );
          panels.forEach((p: any) => { try { p.expand(); } catch { /* ok */ } });
          await new Promise((r) => setTimeout(r, 400));
          let n = 0;
          panels.forEach((p: any) => {
            const first = p.querySelector('tp-yt-paper-radio-button');
            if (first) { try { first.click(); n++; } catch { /* ok */ } }
          });
          await new Promise((r) => setTimeout(r, 500));
          return n;
        })
        .catch(() => 0);
      await delayHumano(400, 800);
      await snap('07a-autocert-preenchida');
      try {
        await page.waitForFunction(
          () => {
            const doc = (globalThis as any).document;
            const b = doc.querySelector('#submit-questionnaire-button');
            return b && !b.disabled && !b.hasAttribute('disabled');
          },
          { timeout: 15_000 }
        );
        const okSubmit = await clicarNativo('#submit-questionnaire-button');
        if (!okSubmit) await page.click('#submit-questionnaire-button', { timeout: 8000 });
        autocertEnviada = true;
        await delayHumano(1000, 1800);
        await snap('07b-autocert-enviada');
        logI('classificacao enviada');
      } catch (e) {
        logW('botao Enviar classificacao nao habilitou/clicou', { erro: (e as Error).message });
        await snap('07b-autocert-submit-falhou');
      }
      return true;
    }

    async function configurarMonetizacao(): Promise<boolean> {
      const tem = await page.locator('ytcp-video-monetization').count().catch(() => 0);
      if (!tem) return false;
      const jaAtiva = await page
        .evaluate(() => {
          const mon = (globalThis as any).document.querySelector('ytcp-video-monetization');
          return /ativad/i.test((mon && mon.innerText) || '');
        })
        .catch(() => false);
      if (jaAtiva) return false;
      logI('ativando monetizacao (video longo)');
      await page
        .evaluate(() => {
          const mon = (globalThis as any).document.querySelector('ytcp-video-monetization');
          const edit = mon && mon.querySelector('ytcp-icon-button');
          if (edit) edit.click();
        })
        .catch(() => {});
      await delayHumano(700, 1200);
      const marcaESalva = async (radioSel: string): Promise<boolean> => {
        return await page
          .evaluate(async (sel: string) => {
            const doc = (globalThis as any).document;
            const r = doc.querySelector(sel);
            if (!r) return false;
            r.click();
            await new Promise((res) => setTimeout(res, 600));
            const save = doc.querySelector('#save-button');
            if (save && !save.hasAttribute('disabled') && !save.disabled) { save.click(); return true; }
            return false;
          }, radioSel)
          .catch(() => false);
      };
      let ok = await marcaESalva('#radio-on');
      if (ok) {
        await delayHumano(900, 1500);
        await snap('07m-monetizacao-ativada');
        logI('monetizacao ATIVADA');
        return true;
      }
      logW('nao consegui ATIVAR monetizacao — publicando SEM anuncios (destrava com Desativar)');
      ok = await marcaESalva('#radio-off');
      await delayHumano(900, 1500);
      await snap(ok ? '07m-monetizacao-desativada-fallback' : '07m-monetizacao-falhou');
      return ok;
    }

    const MAX_PASSOS = 6;
    let chegouVisibilidade = false;
    for (let passo = 1; passo <= MAX_PASSOS; passo++) {
      await configurarMonetizacao().catch(() => {});
      await preencherAutoCertificacao().catch(() => {});

      const naVisibilidade = await page
        .locator('tp-yt-paper-radio-button[name="PUBLIC"], tp-yt-paper-radio-button[name="PRIVATE"], tp-yt-paper-radio-button[name="UNLISTED"]')
        .first()
        .count()
        .catch(() => 0);
      if (naVisibilidade) { chegouVisibilidade = true; break; }

      logI('clicando Avancar (passo ' + passo + ')');
      try {
        await page.waitForFunction(
          () => {
            const doc = (globalThis as any).document;
            const btn = doc.querySelector('#next-button');
            return btn && !btn.disabled && !btn.hasAttribute('disabled');
          },
          { timeout: 60_000 }
        );
        await delayHumano(700, 1300);
        const ok = await clicarNativo('#next-button');
        if (!ok) await page.click('#next-button', { timeout: 10_000 });
        await delayHumano(1400, 2400);
        await snap('07-avancar-' + passo);
      } catch (e) {
        await snap('07-avancar-' + passo + '-falhou');
        try {
          await page.getByRole('button', { name: /^(next|pr[óo]ximo|avan[çc]ar)$/i }).first().click({ timeout: 5000 });
          await delayHumano(1400, 2400);
        } catch {
          throw new Error('Botao Avancar (passo ' + passo + ') nao encontrado ou desabilitado');
        }
      }
    }
    if (!chegouVisibilidade) logW('nao confirmei a etapa de Visibilidade apos ' + MAX_PASSOS + ' passos — seguindo mesmo assim');

    // ETAPA 8: visibilidade (default PUBLIC)
    const nomeRadio = privacyAlvo === 'PRIVATE' ? 'PRIVATE' : privacyAlvo === 'UNLISTED' ? 'UNLISTED' : 'PUBLIC';
    logI('marcando visibilidade', { nomeRadio });
    try {
      const radio = page.locator('tp-yt-paper-radio-button[name="' + nomeRadio + '"]').first();
      await radio.waitFor({ state: 'visible', timeout: 10_000 });
      const okPub = await clicarNativo('tp-yt-paper-radio-button[name="' + nomeRadio + '"]');
      if (!okPub) await radio.click();
      await delayHumano(500, 1000);
      await snap('08-visibilidade-marcada');
    } catch (e) {
      logW('seletor de visibilidade nao encontrado, tentando por texto', { erro: (e as Error).message });
      try {
        await page.getByRole('radio', { name: /^p[úu]blico|^public$/i }).first().click({ timeout: 5000 });
      } catch {
        await snap('08-visibilidade-nao-marcada');
        throw new Error('Nao consegui marcar a visibilidade');
      }
    }

    // ETAPA 9: publicar — aguarda verificação concluir (até 10 min), publica, confirma "mesmo assim".
    logI('aguardando verificacao concluir (ate 10 min)');
    const MAX_ESPERA_MS = 6 * 60_000; // [producao] 6min (antes 10) p/ caber no timeout do worker; se não concluir, o diálogo "publicar mesmo assim" resolve
    const t0 = Date.now();
    let ultimoLog = 0;
    while (Date.now() - t0 < MAX_ESPERA_MS) {
      const verifConcluida = await page
        .evaluate(() => {
          const doc = (globalThis as any).document;
          const t = (doc.body && doc.body.innerText) || '';
          if (/verifica[çc][õo]es conclu[íi]das/i.test(t)) return true;
          if (/verifica[çc][ãa]o\s+\d+\s*%|\d+\s*minutos?\s+restantes/i.test(t)) return false;
          return false;
        })
        .catch(() => false);
      if (verifConcluida) { logI('verificacao concluida'); break; }
      const decorrido = Date.now() - t0;
      if (decorrido - ultimoLog > 60_000) {
        ultimoLog = decorrido;
        logI('ainda verificando...', { segundos: Math.round(decorrido / 1000) });
        await snap('09-aguardando-verificacao');
      }
      await delayHumano(6000, 9000);
    }
    await snap('09-antes-de-publicar');

    logI('clicando Publicar');
    try {
      await page.waitForFunction(
        () => {
          const doc = (globalThis as any).document;
          const btn = doc.querySelector('#done-button');
          return btn && !btn.disabled && !btn.hasAttribute('disabled');
        },
        { timeout: 30_000 }
      );
      const ok = await clicarNativo('#done-button');
      if (!ok) await page.click('#done-button', { timeout: 10_000 });
      await delayHumano(1500, 2500);
      await snap('09b-publish-clicado');
    } catch (e) {
      await snap('09b-publish-nao-clicado');
      throw new Error('Falha ao clicar Publicar: ' + (e as Error).message);
    }

    // [+publish-fix 02/11] BUG: em vídeo LONGO a verificação do YouTube não termina a tempo; ao clicar Publicar
    // aparece o diálogo "publicar mesmo com a verificação em andamento?". A versão antiga, se não achasse um botão
    // com texto EXATAMENTE "Publicar", caía no #cancel-button => CANCELAVA => o vídeo ficava em RASCUNHO (e ainda
    // assim capturava a URL do rascunho e reportava sucesso). Agora: clica SEMPRE no confirmar (#confirm-button ou
    // botão cujo rótulo contenha "publicar"), NUNCA no cancelar; e confere se saiu do rascunho, re-tentando até 3x.
    async function confirmarPublicarMesmoAssim(): Promise<boolean> {
      return await page
        .evaluate(() => {
          const doc = (globalThis as any).document;
          const dialogs = Array.from(doc.querySelectorAll('ytcp-confirmation-dialog, tp-yt-paper-dialog'));
          for (const d of dialogs as any[]) {
            const r = d.getBoundingClientRect ? d.getBoundingClientRect() : { width: 0, height: 0 };
            if (r.width < 10 || r.height < 10) continue;
            const txt = (d.innerText || '').toLowerCase();
            if (!/verifica|publicar|publicando/.test(txt)) continue; // só o diálogo de verificação/publicação
            // 1) botão oficial de confirmar; 2) senão, um botão cujo rótulo contenha "publicar" (NUNCA o cancelar)
            let pub: any = d.querySelector('#confirm-button');
            if (!pub) {
              const cands = Array.from(d.querySelectorAll('ytcp-button, button')) as any[];
              pub = cands.find((b) => /publicar/i.test(b.textContent || '') && b.id !== 'cancel-button') || null;
            }
            if (pub && pub.id !== 'cancel-button') { try { pub.click(); } catch { /* ok */ } return true; }
          }
          return false;
        })
        .catch(() => false);
    }
    // ainda NO wizard (não publicou) = radios de visibilidade ainda visíveis
    async function aindaNoWizard(): Promise<boolean> {
      return await page
        .evaluate(() => {
          const doc = (globalThis as any).document;
          const radio = doc.querySelector('tp-yt-paper-radio-button[name="PUBLIC"], tp-yt-paper-radio-button[name="PRIVATE"], tp-yt-paper-radio-button[name="UNLISTED"]');
          if (!radio) return false;
          const r = radio.getBoundingClientRect ? radio.getBoundingClientRect() : { width: 0, height: 0 };
          return r.width > 5 && r.height > 5;
        })
        .catch(() => false);
    }
    async function jaPublicou(): Promise<boolean> {
      const okTxt = await page
        .evaluate(() => {
          const doc = (globalThis as any).document;
          const t = (doc.body && doc.body.innerText) || '';
          return /v[íi]deo publicado|publicado com sucesso|compartilhe seu v[íi]deo|enviado ao youtube|v[íi]deo enviado/i.test(t);
        })
        .catch(() => false);
      if (okTxt) return true;
      return !(await aindaNoWizard());
    }
    // 1ª rodada: confirma o "publicar mesmo assim"
    for (let tent = 0; tent < 6; tent++) {
      const confirmou = await confirmarPublicarMesmoAssim();
      if (confirmou) {
        logI('diálogo "ainda verificando" — cliquei Publicar mesmo assim');
        await delayHumano(1200, 2000);
        await snap('09c-publicar-mesmo-assim');
        break;
      }
      await delayHumano(700, 1200);
    }
    // confere que SAIU do rascunho; se continua no wizard, re-clica Publicar + confirma (até 3x)
    let publicou = await jaPublicou();
    for (let v = 0; v < 3 && !publicou; v++) {
      logW('ainda em rascunho — re-clicando Publicar (tentativa ' + (v + 1) + ')');
      try {
        const okBtn = await clicarNativo('#done-button');
        if (!okBtn) await page.click('#done-button', { timeout: 8000 });
      } catch { /* segue */ }
      await delayHumano(1500, 2500);
      for (let t2 = 0; t2 < 6; t2++) {
        if (await confirmarPublicarMesmoAssim()) { await delayHumano(1200, 2000); break; }
        await delayHumano(700, 1200);
      }
      await delayHumano(2500, 4000);
      publicou = await jaPublicou();
    }
    if (publicou) { await snap('09d-publicacao-confirmada'); logI('publicacao confirmada (saiu do rascunho)'); }
    else { await snap('09d-ainda-rascunho'); logW('ATENCAO: o video pode ter ficado em RASCUNHO (nao saiu do wizard)'); }

    // ETAPA 10: capturar URL
    logI('aguardando URL do video');
    let videoUrl: string | undefined;
    try {
      await page.waitForSelector('a[href*="youtu.be/"], a[href*="youtube.com/shorts/"]', { timeout: 60_000 });
      videoUrl = await page.evaluate(() => {
        const doc = (globalThis as any).document;
        const link = doc.querySelector('a[href*="youtu.be/"], a[href*="youtube.com/shorts/"]');
        return link ? link.href : undefined;
      });
      await snap('10-url-capturada');
    } catch {
      logW('nao consegui capturar URL do video publicado');
      await snap('10-url-nao-capturada');
    }

    // [+capa-edit-fix 03/11] GARANTIA DE CAPA: a ETAPA 5b (capa durante o upload) falha em vídeo LONGO
    // (o campo da miniatura fica instável enquanto o vídeo ainda sobe → Teclado/Violão/Comunidade saíam sem capa;
    // Baixo processava rápido e colava). Aqui, com o vídeo JÁ publicado, abrimos a PÁGINA DE EDIÇÃO (onde só existe
    // o input da miniatura, sem o input do vídeo) e aplicamos a capa — caminho confiável, sem cota de API.
    // Best-effort: nunca derruba a publicação.
    if (thumbnailPath && fs.existsSync(thumbnailPath) && videoUrl) {
      const vidEdit = (String(videoUrl).match(/(?:youtu\.be\/|shorts\/|[?&]v=)([\w-]{6,})/) || [])[1] || '';
      if (vidEdit) {
        try {
          logI('garantindo capa pela pagina de edicao', { vidEdit });
          await page.goto('https://studio.youtube.com/video/' + vidEdit + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 });
          await delayHumano(2500, 4000);
          const inputCapaEdit = page.locator('ytcp-thumbnail-uploader input[type="file"]').first();
          await inputCapaEdit.waitFor({ state: 'attached', timeout: 30_000 });
          await page.evaluate(() => { const u: any = (globalThis as any).document.querySelector('ytcp-thumbnail-uploader'); if (u && u.scrollIntoView) u.scrollIntoView({ block: 'center' }); }).catch(() => {});
          await inputCapaEdit.setInputFiles(thumbnailPath).catch(() => {});
          await delayHumano(2500, 4000);
          const salvouCapa = await page.evaluate(() => {
            const s: any = (globalThis as any).document.querySelector('ytcp-button#save, #save');
            if (!s) return 'sem-botao';
            if (s.hasAttribute('disabled')) return 'desabilitado';
            s.click(); return 'clicou';
          }).catch(() => 'erro');
          await delayHumano(2500, 4500);
          logI('capa pela edicao: ' + salvouCapa);
          await snap('11-capa-edit-' + salvouCapa);
        } catch (e) {
          logW('nao consegui garantir a capa pela edicao (seguindo)', { erro: (e as Error).message });
          try { await snap('11-capa-edit-falhou'); } catch { /* ok */ }
        }
      }
    }

    await delayHumano(1500, 3000);
    return { ok: true, postUrl: videoUrl, rascunho: !publicou, duracaoMs: Date.now() - start, screenshots, proxyUsado };
  } catch (e) {
    const msg = (e as Error).message;
    logW('erro publicando', { projeto, erro: msg });
    try { await snap('erro-final'); } catch { /* ok */ }
    return { ok: false, erro: msg, motivoFalha: 'ui_mudou', duracaoMs: Date.now() - start, screenshots, proxyUsado };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}

// [+capa-edit-fix 03/11] Aplica a capa em UM vídeo JÁ publicado pela PÁGINA DE EDIÇÃO do Studio, via navegador
// invisível (sem cota de API — serve quando a cota diária do YouTube estourou). Abre sessão própria (proxy +
// storageState do canal). Reutilizada pelo endpoint /pub/youtube/recapa-navegador e disponível p/ correção em massa.
export async function aplicarCapaViaEdit(opts: { projeto: string; videoId: string; thumbnailPath: string }): Promise<{ ok: boolean; erro?: string; detalhe?: string; proxyUsado?: string | null }> {
  const projeto = String(opts.projeto || '').trim().toLowerCase();
  const videoId = String(opts.videoId || '').trim();
  const thumbnailPath = opts.thumbnailPath || '';
  if (!projeto || !videoId) return { ok: false, erro: 'projeto/videoId obrigatórios' };
  if (!thumbnailPath || !fs.existsSync(thumbnailPath)) return { ok: false, erro: 'capa não encontrada no servidor: ' + thumbnailPath };
  const sess = lerSessao(projeto);
  if (!sess) return { ok: false, erro: 'sem sessão YT na nuvem para ' + projeto };
  let browser: any = null; let ctx: any = null; let page: any = null; let proxyUsado: string | null = null;
  try {
    const temLista = lerProxies().length > 0;
    const escolhido = await escolherProxy();
    if (temLista && !escolhido) return { ok: false, erro: 'nenhum aparelho de casa (proxy residencial) online' };
    const launchOpts: any = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] };
    if (escolhido) { launchOpts.proxy = escolhido.playwright; proxyUsado = escolhido.node.nome || escolhido.node.host; }
    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({ storageState: sess.storageState, userAgent: sess.userAgent || USER_AGENT_DEFAULT, viewport: { width: 1400, height: 900 }, locale: 'pt-BR' });
    page = await ctx.newPage();
    try { await page.addInitScript((ua: string) => { try { const nav = (globalThis as unknown as { navigator: object }).navigator; Object.defineProperty(nav, 'userAgent', { get: () => ua }); } catch { /* ignore */ } }, sess.userAgent || USER_AGENT_DEFAULT); } catch { /* best-effort */ }
    await page.goto('https://studio.youtube.com/video/' + videoId + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await delayHumano(2500, 4000);
    if (/accounts\.google\.com/.test(page.url())) return { ok: false, erro: 'sessão YT expirou (redirecionou pro login do Google)', proxyUsado };
    const inputCapa = page.locator('ytcp-thumbnail-uploader input[type="file"]').first();
    try { await inputCapa.waitFor({ state: 'attached', timeout: 30_000 }); } catch { return { ok: false, erro: 'campo da miniatura não apareceu (UI mudou ou vídeo inexistente)', proxyUsado }; }
    try { await page.evaluate(() => { const u: any = (globalThis as any).document.querySelector('ytcp-thumbnail-uploader'); if (u && u.scrollIntoView) u.scrollIntoView({ block: 'center' }); }); } catch { /* ok */ }
    await inputCapa.setInputFiles(thumbnailPath).catch(() => { /* segue pra verificar */ });
    await delayHumano(2500, 4000);
    const erroCapa = await page.evaluate(() => {
      const doc: any = (globalThis as any).document;
      const up = doc.querySelector('ytcp-thumbnail-uploader');
      const t = (((up && up.innerText) || '') + ' ' + (doc.body?.innerText || '')).slice(0, 6000);
      const m = t.match(/(n[ãa]o foi poss[íi]vel[^.\n]*miniatura|formato[^.\n]*(n[ãa]o|inv[áa]lid|incompat)|2\s*MB|m[áa]ximo de 2|unsupported|too large|invalid)/i);
      return m ? m[0].replace(/\s+/g, ' ').slice(0, 120) : '';
    }).catch(() => '');
    if (erroCapa) return { ok: false, erro: 'YouTube recusou a capa: ' + erroCapa, proxyUsado };
    const salvou = await page.evaluate(() => {
      const s: any = (globalThis as any).document.querySelector('ytcp-button#save, #save');
      if (!s) return 'sem-botao';
      if (s.hasAttribute('disabled')) return 'desabilitado';
      s.click(); return 'clicou';
    }).catch(() => 'erro');
    await delayHumano(2500, 4500);
    if (salvou === 'clicou') return { ok: true, detalhe: 'capa salva', proxyUsado };
    if (salvou === 'desabilitado') return { ok: false, erro: 'botão Salvar não habilitou (a capa pode não ter sido aceita)', proxyUsado };
    return { ok: false, erro: 'não achei o botão Salvar (' + salvou + ')', proxyUsado };
  } catch (e: any) {
    return { ok: false, erro: String(e?.message || e).slice(0, 200), proxyUsado };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}

// [+tela-final 07/11 Ezequias] Aplica a TELA FINAL (end screen) em UM vídeo horizontal já publicado, copiando de um
// vídeo-MOLDE do próprio canal via "Importar do vídeo" no Studio (navegador invisível, sessão + proxy do canal).
// O YouTube NÃO tem API de tela final — só dá pelo Studio. Molde por canal (traz "Envio mais recente" + link do combo
// com a arte, 20s). Instrumentado com snaps em cada passo p/ diagnóstico. NÃO monta na mão (usa o molde).
export async function aplicarTelaFinalViaEdit(opts: { projeto: string; videoId: string; moldeId: string; moldeTitulo?: string; snaps?: boolean }): Promise<{ ok: boolean; erro?: string; detalhe?: string; jaTinha?: boolean; proxyUsado?: string | null; screenshots?: string[]; debug?: any }> {
  const projeto = String(opts.projeto || '').trim().toLowerCase();
  const videoId = String(opts.videoId || '').trim();
  const moldeId = String(opts.moldeId || '').trim();
  const moldeTitulo = String(opts.moldeTitulo || '').trim();
  const querSnaps = !!opts.snaps;
  const screenshots: string[] = [];
  if (!projeto || !videoId || !moldeId) return { ok: false, erro: 'projeto/videoId/moldeId obrigatórios' };
  if (videoId === moldeId) return { ok: false, erro: 'videoId == moldeId (não aplicar no próprio molde)' };
  const sess = lerSessao(projeto);
  if (!sess) return { ok: false, erro: 'sem sessão YT na nuvem para ' + projeto };
  let browser: any = null; let ctx: any = null; let page: any = null; let proxyUsado: string | null = null;
  try { if (querSnaps) fs.mkdirSync(SNAP_DIR, { recursive: true }); } catch { /* ok */ }
  async function snap(etapa: string): Promise<void> {
    if (!querSnaps || !page) return;
    try { const p = path.join(SNAP_DIR, projeto + '-tf-' + Date.now() + '-' + etapa + '.png'); await page.screenshot({ path: p, fullPage: false }); screenshots.push(p); } catch { /* ignore */ }
  }
  try {
    const temLista = lerProxies().length > 0;
    const escolhido = await escolherProxy();
    if (temLista && !escolhido) return { ok: false, erro: 'nenhum aparelho de casa (proxy residencial) online', screenshots };
    const launchOpts: any = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] };
    if (escolhido) { launchOpts.proxy = escolhido.playwright; proxyUsado = escolhido.node.nome || escolhido.node.host; }
    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({ storageState: sess.storageState, userAgent: sess.userAgent || USER_AGENT_DEFAULT, viewport: { width: 1400, height: 900 }, locale: 'pt-BR' });
    page = await ctx.newPage();
    try { await page.addInitScript((ua: string) => { try { const nav = (globalThis as unknown as { navigator: object }).navigator; Object.defineProperty(nav, 'userAgent', { get: () => ua }); } catch { /* ignore */ } }, sess.userAgent || USER_AGENT_DEFAULT); } catch { /* best-effort */ }

    await page.goto('https://studio.youtube.com/video/' + videoId + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await delayHumano(4000, 6000);
    await snap('tf-01-edit');
    if (/accounts\.google\.com/.test(page.url())) return { ok: false, erro: 'sessão YT expirou (redirecionou pro login do Google)', proxyUsado, screenshots };

    // clicador real por TEXTO (tenta locator; se não, mouse na bounding box) — os gatilhos Polymer do Studio só respondem a clique real
    async function clicarTexto(textos: string[], timeoutMs = 8000): Promise<boolean> {
      for (const t of textos) {
        try { const loc = page.getByText(t, { exact: false }).first(); if (await loc.count()) { await loc.scrollIntoViewIfNeeded().catch(() => {}); await loc.click({ timeout: timeoutMs }).catch(() => {}); return true; } } catch { /* tenta próximo */ }
      }
      return false;
    }
    async function temTexto(re: RegExp): Promise<boolean> {
      return await page.evaluate((src: string) => { const t = ((globalThis as any).document.body?.innerText || ''); return new RegExp(src, 'i').test(t); }, re.source).catch(() => false);
    }

    // ======== FLUXO POR LOCATORS ========
    // IMPORTANTE: no navegador stealth da nuvem (patchright), page.evaluate com varredura pesada de
    // shadow DOM falha/estoura em silêncio. Então usamos LOCATORS do Playwright (que funcionam lá,
    // igual a automação de CAPA). Seletores confirmados ao vivo no Studio (09/11):
    //   - card "Tela final":   ytcp-text-dropdown-trigger com texto "Tela final" (role=button; a linha toda abre)
    //   - botão importar:      texto "Importar do vídeo"
    //   - busca da grade:      input[placeholder="Pesquise nos seus vídeos"]
    //   - cards da grade:      ytcp-entity-card[role="option"]  (dentro de tp-yt-paper-dialog "Escolha um vídeo específico")
    //   - salvar do editor:    ytcp-button#save-button / "Salvar" habilitado
    const selCard = 'ytcp-text-dropdown-trigger:has-text("Tela final"), ytcp-text-dropdown-trigger:has-text("Telas finais"), ytcp-text-dropdown-trigger:has-text("End screen")';
    const selImportar = 'text=/importar do v[íi]deo|import from video/i';

    // 1) ESPERA o card "Tela final" aparecer (a página de Detalhes demora ~5s) e clica nele
    let cardLoc: any = null;
    try { const l = page.locator(selCard).first(); await l.waitFor({ state: 'visible', timeout: 30_000 }); cardLoc = l; } catch { /* trata abaixo */ }
    if (!cardLoc) { await snap('tf-02-sem-card'); return { ok: false, erro: 'não achei o card "Tela final" na página de detalhes (UI mudou ou vídeo inexistente)', proxyUsado, screenshots }; }
    let abriu = false;
    for (let i = 0; i < 4 && !abriu; i++) {
      try { await cardLoc.scrollIntoViewIfNeeded(); } catch { /* ok */ }
      try { await cardLoc.click({ timeout: 8000 }); } catch { /* ok */ }
      await delayHumano(1800, 2600);
      // editor abriu = botão "Importar do vídeo" visível (só existe no editor de tela final)
      try { abriu = await page.locator(selImportar).first().isVisible({ timeout: 2500 }); } catch { abriu = false; }
      if (!abriu) await snap('tf-02-tentativa-abrir-' + (i + 1));
    }
    await snap('tf-02-editor-tela-final');
    if (!abriu) return { ok: false, erro: 'cliquei no card Tela final mas o editor não abriu (sem botão Importar do vídeo)', proxyUsado, screenshots };

    // 2) clicar "Importar do vídeo" -> abre o modal "Escolha um vídeo específico"
    try { const imp = page.locator(selImportar).first(); await imp.waitFor({ state: 'visible', timeout: 8000 }); await imp.click({ timeout: 8000 }); } catch { /* ok */ }
    await delayHumano(1500, 2500);
    try { await page.locator('ytcp-entity-card[role="option"], input[placeholder*="Pesquise nos seus" i]').first().waitFor({ state: 'visible', timeout: 12_000 }); } catch { /* ok */ }
    await snap('tf-03-grade-videos');

    // 3) buscar o MOLDE pelo título no campo do modal (a grade filtra) e clicar no 1º card
    const termo = (moldeTitulo || '').trim().slice(0, 40);
    if (termo) {
      try {
        const busca = page.locator('input[placeholder*="Pesquise nos seus" i], input[placeholder*="search your videos" i]').first();
        await busca.waitFor({ state: 'visible', timeout: 8000 });
        await busca.click({ timeout: 5000 }).catch(() => { /* ok */ });
        await busca.fill(termo).catch(async () => { await page.keyboard.type(termo, { delay: 40 }).catch(() => { /* ok */ }); });
        await delayHumano(600, 1000);
        await page.keyboard.press('Enter').catch(() => { /* ok */ });
      } catch { /* ok */ }
      await delayHumano(3000, 4200);
      await snap('tf-03a-busca');
    }
    // card do molde: 1º pelo título; senão o 1º card da grade (após o filtro é o molde)
    let cardMolde: any = null;
    if (termo) { try { const c = page.locator('ytcp-entity-card[role="option"]', { hasText: termo.slice(0, 22) }).first(); if (await c.count()) cardMolde = c; } catch { /* ok */ } }
    if (!cardMolde) { try { const c = page.locator('ytcp-entity-card[role="option"]').first(); if (await c.count()) cardMolde = c; } catch { /* ok */ } }
    if (!cardMolde) { await snap('tf-03b-molde-nao-achado'); return { ok: false, erro: 'não achei o vídeo-molde na grade (busca: "' + termo + '")', proxyUsado, screenshots }; }
    try { await cardMolde.scrollIntoViewIfNeeded(); } catch { /* ok */ }
    try { await cardMolde.click({ timeout: 8000 }); } catch { /* ok */ }
    await delayHumano(2000, 3000);
    await snap('tf-04-molde-aplicado');

    // 4) Salvar (botão "Salvar" habilitado do editor de tela final)
    let salvou = false;
    try { const s = page.locator('ytcp-button#save-button:not([disabled]), #save-button:not([disabled]), ytcp-button:not([disabled]):has-text("Salvar"), ytcp-button:not([disabled]):has-text("Save")').first(); if (await s.count()) { await s.click({ timeout: 8000 }).catch(() => { /* ok */ }); salvou = true; } } catch { /* ok */ }
    if (!salvou) salvou = await clicarTexto(['Salvar', 'Save']);
    await delayHumano(2500, 4000);
    await snap('tf-05-salvo');

    // 5) conferir: reabrir /edit, abrir o card Tela final e checar os elementos (vídeo + link do combo)
    await page.goto('https://studio.youtube.com/video/' + videoId + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 }).catch(() => { /* ok */ });
    await delayHumano(3000, 4500);
    let conferido = false;
    for (let i = 0; i < 3 && !conferido; i++) {
      try { const l = page.locator(selCard).first(); await l.waitFor({ state: 'visible', timeout: 12_000 }); await l.click({ timeout: 6000 }); } catch { /* ok */ }
      await delayHumano(1600, 2300);
      conferido = await temTexto(/envio mais recente|v[íi]deo mais recente|most recent upload|teclado\.melodiaseriffs|melodiaseriffs\.com\.br|importar do v[íi]deo/);
    }
    await snap('tf-06-conferencia');
    if (conferido) return { ok: true, detalhe: 'tela final aplicada e confirmada (molde ' + moldeId + ')', proxyUsado, screenshots };
    return { ok: true, detalhe: 'cliquei em salvar, mas não confirmei os elementos na reabertura (revisar pelos snaps)', proxyUsado, screenshots };
  } catch (e: any) {
    try { await snap('tf-erro'); } catch { /* ok */ }
    return { ok: false, erro: String(e?.message || e).slice(0, 200), proxyUsado, screenshots };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}

// [+video-relacionado 09/11 Ezequias] VÍDEO RELACIONADO (o "botãozinho" no rodapé dos SHORTS, equivalente da tela final).
// Aponta 1 vídeo-ÂNCORA FIXO por instrumento (ex.: baixo -> "Guia completo Baixo Gospel"). YouTube só aceita vídeo
// relacionado do PRÓPRIO canal. Portado do app do PC (youtube-video-relacionado.ts), por LOCATORS (patchright na nuvem).
// Fluxo REAL do Studio: abrir /edit do Short -> card "Vídeo relacionado" (card INTEIRO clicável, sem lápis) abre direto
// o modal "Escolha um vídeo específico" -> buscar pelo TÍTULO -> clicar o card pelo ID na miniatura (i.ytimg.com/vi/<ID>)
// -> Salvar. NUNCA clica no "1º card" genérico (apontaria pro vídeo errado em silêncio).
export async function aplicarVideoRelacionadoViaEdit(opts: { projeto: string; videoId: string; ancoraId: string; ancoraTitulo?: string; snaps?: boolean }): Promise<{ ok: boolean; erro?: string; detalhe?: string; jaTinha?: boolean; proxyUsado?: string | null; screenshots?: string[] }> {
  const projeto = String(opts.projeto || '').trim().toLowerCase();
  const videoId = String(opts.videoId || '').trim();
  const ancoraId = String(opts.ancoraId || '').trim();
  const ancoraTitulo = String(opts.ancoraTitulo || '').trim();
  const querSnaps = !!opts.snaps;
  const screenshots: string[] = [];
  if (!projeto || !videoId || !ancoraId) return { ok: false, erro: 'projeto/videoId/ancoraId obrigatórios' };
  if (videoId === ancoraId) return { ok: false, erro: 'videoId == ancoraId (não aponta pra si mesmo)' };
  const sess = lerSessao(projeto);
  if (!sess) return { ok: false, erro: 'sem sessão YT na nuvem para ' + projeto };
  let browser: any = null; let ctx: any = null; let page: any = null; let proxyUsado: string | null = null;
  try { if (querSnaps) fs.mkdirSync(SNAP_DIR, { recursive: true }); } catch { /* ok */ }
  async function snap(etapa: string): Promise<void> {
    if (!querSnaps || !page) return;
    try { const p = path.join(SNAP_DIR, projeto + '-vr-' + Date.now() + '-' + etapa + '.png'); await page.screenshot({ path: p, fullPage: false }); screenshots.push(p); } catch { /* ignore */ }
  }
  try {
    const temLista = lerProxies().length > 0;
    const escolhido = await escolherProxy();
    if (temLista && !escolhido) return { ok: false, erro: 'nenhum aparelho de casa (proxy residencial) online', screenshots };
    const launchOpts: any = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] };
    if (escolhido) { launchOpts.proxy = escolhido.playwright; proxyUsado = escolhido.node.nome || escolhido.node.host; }
    const chromium = await carregarChromium();
    browser = await chromium.launch(launchOpts);
    ctx = await browser.newContext({ storageState: sess.storageState, userAgent: sess.userAgent || USER_AGENT_DEFAULT, viewport: { width: 1400, height: 900 }, locale: 'pt-BR' });
    page = await ctx.newPage();
    try { await page.addInitScript((ua: string) => { try { const nav = (globalThis as unknown as { navigator: object }).navigator; Object.defineProperty(nav, 'userAgent', { get: () => ua }); } catch { /* ignore */ } }, sess.userAgent || USER_AGENT_DEFAULT); } catch { /* best-effort */ }

    // WARMUP: passa pelo youtube.com p/ o Google re-emitir o token rotativo (evita "sessão expirou" após ~1h parada)
    try { await page.goto('https://www.youtube.com/', { waitUntil: 'domcontentloaded', timeout: 60_000 }); await delayHumano(1500, 2500); } catch { /* best-effort */ }
    if (/accounts\.google\.com/.test(page.url())) return { ok: false, erro: 'sessão YT expirou (login do Google)', proxyUsado, screenshots };

    await page.goto('https://studio.youtube.com/video/' + videoId + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await delayHumano(3500, 5000);
    await snap('vr-01-edit');
    if (/accounts\.google\.com/.test(page.url())) return { ok: false, erro: 'sessão YT expirou (redirecionou pro login do Google)', proxyUsado, screenshots };
    try { await page.waitForLoadState('networkidle', { timeout: 20_000 }); } catch { /* ok */ }

    async function temTexto(re: RegExp): Promise<boolean> {
      return await page.evaluate((src: string) => { const t = ((globalThis as any).document.body?.innerText || ''); return new RegExp(src, 'i').test(t); }, re.source).catch(() => false);
    }
    const RX_VR = /v[íi]deo relacionado|related video/i;
    const selModal = '[role="dialog"], tp-yt-paper-dialog, ytcp-dialog';

    // 1) ESPERA o card "Vídeo relacionado" aparecer (só nasce quando o Short fica pronto; se não vier, ainda processando)
    let cardVR: any = null;
    for (let i = 0; i < 6 && !cardVR; i++) {
      const l = page.getByText(RX_VR).first();
      try { await l.waitFor({ state: 'visible', timeout: 10_000 }); cardVR = l; break; } catch { /* espera + retenta: pode estar processando */ }
      await delayHumano(3000, 4500);
    }
    if (!cardVR) { await snap('vr-02-sem-card'); return { ok: false, erro: 'card "Vídeo relacionado" não apareceu (Short ainda processando ou não é Short?)', proxyUsado, screenshots }; }

    // 2) clica o card INTEIRO (sem lápis) -> abre direto o modal "Escolha um vídeo específico". 3 tentativas.
    let abriu = false;
    for (let i = 0; i < 3 && !abriu; i++) {
      try { await cardVR.scrollIntoViewIfNeeded(); } catch { /* ok */ }
      try { await cardVR.click({ timeout: 6000 }); } catch {
        try { await cardVR.locator('xpath=ancestor::*[self::ytcp-form-input-container or self::div][1]').first().click({ timeout: 5000 }); } catch { /* ok */ }
      }
      await delayHumano(1800, 2800);
      // detecta o modal pelo TÍTULO/busca (NÃO .first() amplo — há diálogos OCULTOS do Studio que davam falso-negativo)
      try { abriu = await page.locator(selModal).filter({ hasText: /escolha um v[íi]deo|choose a video|selecione um v[íi]deo|select a video/i }).first().isVisible({ timeout: 2500 }); } catch { abriu = false; }
      if (!abriu) { try { abriu = await page.locator('input[placeholder*="Pesquise nos seus" i], input[placeholder*="search your videos" i]').first().isVisible({ timeout: 1500 }); } catch { abriu = false; } }
      if (!abriu) await snap('vr-02-tentativa-abrir-' + (i + 1));
    }
    await snap('vr-02-modal');
    if (!abriu) return { ok: false, erro: 'cliquei no card Vídeo relacionado mas o seletor não abriu', proxyUsado, screenshots };

    // escopo = o modal (evita clicar na busca GLOBAL do Studio, que fecharia o modal)
    const modal = page.locator(selModal).filter({ hasText: /escolha um v[íi]deo|choose a video|selecione um v[íi]deo|select a video/i }).first();
    const temModal = await modal.isVisible({ timeout: 2000 }).catch(() => false);
    const escopo = temModal ? modal : page;

    // 3) busca pelo TÍTULO do âncora (clica o campo e digita; o picker filtra por título)
    if (ancoraTitulo) {
      try {
        const campo = escopo.locator('input[placeholder*="Pesquise nos seus" i], input[placeholder*="search your videos" i]').first();
        await campo.waitFor({ state: 'visible', timeout: 8000 });
        await campo.click({ timeout: 5000 }).catch(() => { /* ok */ });
        await campo.fill(ancoraTitulo.slice(0, 40)).catch(async () => { await page.keyboard.type(ancoraTitulo.slice(0, 40), { delay: 40 }).catch(() => { /* ok */ }); });
        await delayHumano(600, 1000);
        await page.keyboard.press('Enter').catch(() => { /* ok */ });
      } catch { /* ok */ }
      await delayHumano(3000, 4200);
      await snap('vr-03-busca');
    }

    // 4) clica o card do ÂNCORA pelo ID na miniatura (i.ytimg.com/vi/<ID>). SEM fallback genérico (não aponta errado).
    let escolheu = false;
    const alvos = [
      () => escopo.locator('xpath=.//*[contains(@style,"' + ancoraId + '")]/ancestor::ytcp-entity-card[1]').first(),
      () => escopo.locator('xpath=.//*[contains(@style,"' + ancoraId + '")]/ancestor::*[@role="option"][1]').first(),
      () => escopo.locator('[href*="' + ancoraId + '"]').first(),
      () => escopo.locator('[video-id="' + ancoraId + '"]').first()
    ];
    for (const criar of alvos) {
      try { const el = criar(); await el.waitFor({ state: 'visible', timeout: 4000 }); await el.scrollIntoViewIfNeeded().catch(() => { /* ok */ }); await el.click({ timeout: 4000 }); escolheu = true; break; } catch { /* tenta próximo */ }
    }
    if (!escolheu) { await snap('vr-04-ancora-nao-achada'); return { ok: false, erro: 'não achei o vídeo-âncora ' + ancoraId + ' no seletor (âncora é de OUTRO canal? busca: "' + ancoraTitulo + '")', proxyUsado, screenshots }; }
    await delayHumano(1200, 2000);
    // confirma o seletor se tiver botão (nem sempre tem)
    try { await escopo.getByRole('button', { name: /^\s*(conclu[íi]do|ok|selecionar|select|done|confirmar|confirm)\s*$/i }).first().click({ timeout: 5000 }); } catch { /* sem botão: a escolha já valeu */ }
    await delayHumano(1000, 1800);
    await snap('vr-04-escolhido');

    // 5) Salvar
    let salvou = false;
    try { const s = page.locator('ytcp-button#save-button:not([disabled]), #save-button:not([disabled]), ytcp-button:not([disabled]):has-text("Salvar"), ytcp-button:not([disabled]):has-text("Guardar"), ytcp-button:not([disabled]):has-text("Save")').first(); if (await s.count()) { await s.click({ timeout: 8000 }).catch(() => { /* ok */ }); salvou = true; } } catch { /* ok */ }
    if (!salvou) { try { await page.getByRole('button', { name: /^\s*(salvar|guardar|save)\s*$/i }).first().click({ timeout: 8000 }); salvou = true; } catch { /* ok */ } }
    await delayHumano(2500, 4000);
    await snap('vr-05-salvo');

    // 6) conferir: toast "alterações salvas" OU a linha Vídeo relacionado não mostra mais "Nenhum"
    let ok = await temTexto(/salvo|guardad|altera[çc][õo]es\s+(salvas|guardadas)|saved|changes\s+saved/i);
    if (!ok) { await page.goto('https://studio.youtube.com/video/' + videoId + '/edit', { waitUntil: 'domcontentloaded', timeout: 60_000 }).catch(() => { /* ok */ }); await delayHumano(3000, 4500); ok = !(await temTexto(/v[íi]deo relacionado[\s\S]{0,40}\b(nenhum|nada|none)\b/i)); }
    await snap('vr-06-conferencia');
    if (ok) return { ok: true, detalhe: 'vídeo relacionado aplicado (âncora ' + ancoraId + ')', proxyUsado, screenshots };
    return { ok: true, detalhe: 'cliquei em salvar, mas não confirmei (revisar pelos snaps)', proxyUsado, screenshots };
  } catch (e: any) {
    try { await snap('vr-erro'); } catch { /* ok */ }
    return { ok: false, erro: String(e?.message || e).slice(0, 200), proxyUsado, screenshots };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}
