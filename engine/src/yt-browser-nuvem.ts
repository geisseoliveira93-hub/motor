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

import { chromium } from 'playwright';
import * as fs from 'fs';
import * as path from 'path';

const USER_AGENT_DEFAULT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36';

const YT_SESS_DIR = '/data/yt-sessions';
const SNAP_DIR = YT_SESS_DIR + '/_snaps';

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
  erro?: string;
  motivoFalha?: 'sessao_expirada' | 'sem_sessao' | 'arquivo_inexistente' | 'ui_mudou' | 'outro';
  duracaoMs: number;
  screenshots?: string[];
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

  try {
    browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    });
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
      return { ok: false, erro: 'Sessão YT expirou (redirecionou pro login do Google). Pode ser desafio de IP de datacenter — talvez precise de proxy residencial BR.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots };
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
      return { ok: false, erro: 'Sessão YT expirou. Reconecte/exporte a sessão de novo.', motivoFalha: 'sessao_expirada', duracaoMs: Date.now() - start, screenshots };
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

    // ETAPA 5b: capa (thumbnail) — opcional, best-effort. Recebe caminho já pronto no servidor.
    if (thumbnailPath && fs.existsSync(thumbnailPath)) {
      try {
        const inputCapa = page.locator('ytcp-thumbnail-uploader input[type="file"], #file-loader').first();
        await inputCapa.waitFor({ state: 'attached', timeout: 15_000 });
        await inputCapa.setInputFiles(thumbnailPath);
        await delayHumano(2000, 3500);
        await snap('05b-capa-anexada');
        logI('capa anexada');
      } catch (e) {
        logW('não consegui anexar a capa (seguindo sem ela)', { erro: (e as Error).message });
        await snap('05b-capa-falhou');
      }
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
    async function preencherAutoCertificacao(): Promise<boolean> {
      const totalRadios = await page
        .locator('tp-yt-paper-radio-button[name^="VIDEO_SELF_CERTIFICATION_ANSWER_"]')
        .count()
        .catch(() => 0);
      if (!totalRadios) return false;
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
    const MAX_ESPERA_MS = 10 * 60_000;
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

    async function confirmarPublicarMesmoAssim(): Promise<boolean> {
      return await page
        .evaluate(() => {
          const doc = (globalThis as any).document;
          const dialogs = Array.from(doc.querySelectorAll('ytcp-confirmation-dialog'));
          for (const d of dialogs as any[]) {
            const r = d.getBoundingClientRect ? d.getBoundingClientRect() : { width: 0, height: 0 };
            if (r.width < 10 || r.height < 10) continue;
            const txt = d.innerText || '';
            if (!/verifica/i.test(txt)) continue;
            const btns = Array.from(d.querySelectorAll('ytcp-button, button'));
            let pub = (btns as any[]).find((b) => /^\s*publicar\s*$/i.test(b.textContent || ''));
            if (!pub) pub = d.querySelector('#cancel-button') || d.querySelector('ytcp-button#cancel-button');
            if (pub) { pub.click(); return true; }
          }
          return false;
        })
        .catch(() => false);
    }
    for (let tent = 0; tent < 4; tent++) {
      const confirmou = await confirmarPublicarMesmoAssim();
      if (confirmou) {
        logI('diálogo "ainda verificando" — cliquei Publicar mesmo assim');
        await delayHumano(1200, 2000);
        await snap('09c-publicar-mesmo-assim');
        break;
      }
      await delayHumano(700, 1200);
    }

    try {
      await page.waitForFunction(
        () => {
          const doc = (globalThis as any).document;
          const btn = doc.querySelector('#done-button');
          const r = btn && btn.getBoundingClientRect ? btn.getBoundingClientRect() : null;
          const fechou = !btn || (r && r.width < 5);
          const txt = (doc.body && doc.body.innerText) || '';
          const sucesso = /v[íi]deo publicado|publicado com sucesso|enviado ao youtube|v[íi]deo enviado/i.test(txt);
          return fechou || sucesso;
        },
        { timeout: 60_000 }
      );
      await snap('09d-publicacao-confirmada');
      logI('publicacao confirmada');
    } catch {
      logW('nao confirmei explicitamente a publicacao (seguindo p/ capturar URL)');
    }

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

    await delayHumano(1500, 3000);
    return { ok: true, postUrl: videoUrl, duracaoMs: Date.now() - start, screenshots };
  } catch (e) {
    const msg = (e as Error).message;
    logW('erro publicando', { projeto, erro: msg });
    try { await snap('erro-final'); } catch { /* ok */ }
    return { ok: false, erro: msg, motivoFalha: 'ui_mudou', duracaoMs: Date.now() - start, screenshots };
  } finally {
    try { if (ctx) await ctx.close(); } catch { /* ignore */ }
    try { if (browser) await browser.close(); } catch { /* ignore */ }
  }
}
