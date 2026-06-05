/**
 * Token & Cache Monitor for SillyTavern  v4.0.0
 * ====================================================================
 *
 * v4 architecture — reads ST's internal APIs directly instead of
 * intercepting network responses.  No more Response.prototype hacks.
 *
 * Data sources (priority order):
 *   1. context.generateRawData()          — ST PR #5249+
 *   2. main_api.lastResponse.usage        — chat completions module
 *   3. streamingProcessor.lastResponse    — streaming SSE processor
 *
 * Token counting:
 *   1. getTokenCountAsync()               — ST's built-in tokenizer
 *   2. estimateTokensFromText()           — fallback estimator
 *
 * Features:
 *   - Per-message token badges (RikkaHub style)
 *   - Multi-round tool-call accumulation (DeepSeek/Claude function calling)
 *   - DeepSeek / Anthropic / OpenAI / Gemini cache detection
 *   - RMB pricing (¥) with cache-savings calculation
 *   - Chinese number formatting (万/亿)
 *   - ST CSS-variable theme integration (auto dark/light)
 *   - Toggle Σ button — hide panel, click to restore
 *   - Per-chat independent persistence (localStorage)
 *   - Mini trend chart, efficiency score, throughput, projected cost
 *   - Cache-breaker detection, safety timeout
 *   - STscript commands: /token-stats, /token-reset
 *   - Touch + mouse drag, double-click collapse
 */

// ── Imports ─────────────────────────────────────────────────────────────
import {
    eventSource,
    event_types,
    saveSettingsDebounced,
} from '../../../script.js';

import {
    extension_settings as extSettings,
    getContext,
} from '../../extensions.js';

// Optional — available in newer ST builds; degrade gracefully
let getTokenCountAsync = null;
let getGeneratingModel = null;
let main_api           = null;
let streamingProcessor = null;

try {
    const st = await import('../../../script.js');
    getTokenCountAsync  = st.getTokenCountAsync  ?? null;
    getGeneratingModel  = st.getGeneratingModel  ?? null;
    main_api            = st.main_api            ?? null;
    streamingProcessor  = st.streamingProcessor  ?? null;
} catch { /* pre-V4 ST — use fallbacks */ }

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const NAME     = 'token-cache-monitor';
const HIST_MAX = 20;
const LS_PFX   = 'tcm4_';

/** Pricing ¥/百万 tokens (人民币) */
const PRICES = {
    'deepseek-v4-pro':        { hit: 0.026, miss: 3.13, output: 6.26 },
    'deepseek-v4-flash':      { hit: 0.020, miss: 1.01, output: 2.02 },
    'deepseek-v3':            { hit: 0.1,   miss: 1.0,  output: 2.0  },
    'deepseek-r1':            { hit: 1.0,   miss: 4.0,  output: 16.0 },
    'claude-sonnet-4':        { hit: 1.09,  miss: 10.9, output: 54.5 },
    'claude-sonnet-4-5':      { hit: 1.09,  miss: 10.9, output: 54.5 },
    'claude-haiku-4-5':       { hit: 0.073, miss: 0.73, output: 3.64 },
    'gpt-4o':                 { hit: 1.82,  miss: 18.2, output: 72.7 },
    'gpt-4o-mini':            { hit: 0.073, miss: 0.73, output: 3.64 },
};

// ═══════════════════════════════════════════════════════════════════════════
// Settings
// ═══════════════════════════════════════════════════════════════════════════

const DEFAULTS = {
    panelVisible:   true,
    panelCollapsed: false,
    panelPos:       { x: null, y: null },
    showCache:      true,
    showSession:    true,
    showCost:       true,
    showThroughput: true,
    showTrend:      true,
    showBadges:     true,
    costModel:      'auto',       // 'auto' | 'deepseek-v4-pro' | ... | 'custom'
    customPrice:    { hit: 0.1, miss: 1.0, output: 2.0 },
};

let cfg = { ...DEFAULTS };
let _chatId = '';

// ═══════════════════════════════════════════════════════════════════════════
// Per-chat stats
// ═══════════════════════════════════════════════════════════════════════════

function fresh() { return {
    lastPrompt:0, lastCompletion:0, lastCacheHit:0, lastCacheMiss:0,
    lastDuration:0, totalPrompt:0, totalCompletion:0, totalCacheHit:0,
    totalCacheMiss:0, requests:0, cost:0, totalDuration:0,
    streamTokens:0, genStart:0, lastSysPrompt:'', cacheBreaks:0,
    history:[],
    // multi-round tool-call
    round:0, basePrompt:0, accCompletion:0, toolAcc:false, _finTimer:null,
};}

let S = fresh();

// ═══════════════════════════════════════════════════════════════════════════
// Persistence
// ═══════════════════════════════════════════════════════════════════════════

function chatId() {
    try {
        const c = getContext();
        return c?.chatId || c?.chat?.chat_id || c?.chatMetadata?.chat_id || 'default';
    } catch { return 'default'; }
}
function lsKey(id) { return LS_PFX + (id || _chatId); }

function save() {
    if (!_chatId) return;
    try { localStorage.setItem(lsKey(), JSON.stringify({
        totalPrompt:S.totalPrompt, totalCompletion:S.totalCompletion,
        totalCacheHit:S.totalCacheHit, totalCacheMiss:S.totalCacheMiss,
        requests:S.requests, cost:S.cost, totalDuration:S.totalDuration,
        cacheBreaks:S.cacheBreaks, history:S.history.slice(0,HIST_MAX),
        savedAt:Date.now(),
    }));} catch {}
}

function load(id) {
    try {
        const r = localStorage.getItem(lsKey(id));
        if (!r) return fresh();
        const d = JSON.parse(r), s = fresh();
        if (d.totalPrompt    !==undefined) s.totalPrompt    =d.totalPrompt;
        if (d.totalCompletion!==undefined) s.totalCompletion=d.totalCompletion;
        if (d.totalCacheHit  !==undefined) s.totalCacheHit  =d.totalCacheHit;
        if (d.totalCacheMiss !==undefined) s.totalCacheMiss =d.totalCacheMiss;
        if (d.requests       !==undefined) s.requests       =d.requests;
        if (d.cost           !==undefined) s.cost           =d.cost;
        if (d.totalDuration  !==undefined) s.totalDuration  =d.totalDuration;
        if (d.cacheBreaks    !==undefined) s.cacheBreaks    =d.cacheBreaks;
        if (Array.isArray(d.history))       s.history       =d.history.slice(0,HIST_MAX);
        return s;
    } catch { return fresh(); }
}

function swapChat(newId) {
    if (newId === _chatId) return;
    save(); _chatId = newId; S = load(newId);
    refresh();
}

function reset() {
    S = fresh();
    try { localStorage.removeItem(lsKey()); } catch {}
    refresh();
}

// ═══════════════════════════════════════════════════════════════════════════
// Settings helpers
// ═══════════════════════════════════════════════════════════════════════════

function loadCfg() {
    if (extSettings[NAME]) cfg = { ...DEFAULTS, ...extSettings[NAME] };
}
function saveCfg() { extSettings[NAME] = cfg; saveSettingsDebounced(); }

function getPrice(model) {
    if (cfg.costModel !== 'auto') return PRICES[cfg.costModel] || null;
    if (!model) return null;
    const m = model.toLowerCase();
    for (const [k, v] of Object.entries(PRICES))
        if (m.includes(k.replace(/-/g,''))) return v;
    return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// Formatting
// ═══════════════════════════════════════════════════════════════════════════

function fmt(n) {
    if (n == null || isNaN(n)) return '-';
    if (n >= 1e8) return (n/1e8).toFixed(1)+'亿';
    if (n >= 1e4) return (n/1e4).toFixed(1)+'万';
    if (n >= 1e3) return n.toLocaleString();
    return String(Math.round(n));
}
function fc(n) {
    if (n == null || isNaN(n)) return '¥0';
    if (n >= 1) return '¥'+n.toFixed(2);
    if (n >= 0.01) return '¥'+n.toFixed(4);
    return '¥'+n.toFixed(6);
}
function estTokens(t) {
    if (!t || typeof t !== 'string') return 0;
    const cjk = (t.match(/[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]/g)||[]).length;
    return Math.ceil(cjk/1.5) + Math.ceil((t.length-cjk)/4);
}

// ═══════════════════════════════════════════════════════════════════════════
// Cache detection
// ═══════════════════════════════════════════════════════════════════════════

function detectCache(u) {
    if (!u) return { s:null, d:'', hit:0 };

    // Anthropic
    if (u.cache_read_input_tokens !== undefined) {
        const r=u.cache_read_input_tokens||0, c=u.cache_creation_input_tokens||0;
        const i=u.input_tokens||0;
        if (r>0&&c>0) return {s:'PARTIAL',d:`创建${fmt(c)}·读取${fmt(r)}`,hit:r};
        if (r>0) return {s:'HIT',d:`${fmt(r)}/${fmt(i)}`,hit:r};
        if (c>0) return {s:'MISS',d:`创建${fmt(c)}`,hit:0};
        return {s:'MISS',d:'无缓存',hit:0};
    }
    // DeepSeek
    if (u.prompt_cache_hit_tokens!==undefined||u.prompt_cache_miss_tokens!==undefined) {
        const h=u.prompt_cache_hit_tokens||0, m=u.prompt_cache_miss_tokens||0;
        if (h>0&&m>0) return {s:'PARTIAL',d:`命中${fmt(h)}·未中${fmt(m)}`,hit:h};
        if (h>0) return {s:'HIT',d:`${fmt(h)} tokens`,hit:h};
        return {s:'MISS',d:`首次请求`,hit:0};
    }
    // OpenAI
    if (u.prompt_tokens_details?.cached_tokens!==undefined) {
        const c=u.prompt_tokens_details.cached_tokens||0, p=u.prompt_tokens||0;
        if (c>0) return {s:c<p?'PARTIAL':'HIT',d:`${fmt(c)}/${fmt(p)}`,hit:c};
        return {s:'MISS',d:'无缓存',hit:0};
    }
    return {s:null,d:'',hit:0};
}

// ═══════════════════════════════════════════════════════════════════════════
// Data extraction from ST internals
// ═══════════════════════════════════════════════════════════════════════════

async function getUsage() {
    try {
        const ctx = getContext();
        if (typeof ctx?.generateRawData === 'function') {
            const r = await ctx.generateRawData();
            if (r?.usage) return r.usage;
        }
    } catch {}
    if (main_api?.lastResponse?.usage) return main_api.lastResponse.usage;
    if (streamingProcessor?.lastResponse?.usage) return streamingProcessor.lastResponse.usage;
    return null;
}

async function countPrompt() {
    try {
        const ctx = getContext();
        let text = '';
        if (typeof ctx?.getPrompt === 'function') text = await ctx.getPrompt();
        if (!text && ctx?.chat) text = ctx.chat.map(m=>(m.mes||'')).join('\n');
        if (!text) return 0;
        if (getTokenCountAsync) return await getTokenCountAsync(text) || estTokens(text);
        return estTokens(text);
    } catch { return 0; }
}

// ═══════════════════════════════════════════════════════════════════════════
// Record
// ═══════════════════════════════════════════════════════════════════════════

function record(prompt, completion, cacheHit, cacheMiss, dur) {
    const now=Date.now();
    S.lastPrompt=prompt; S.lastCompletion=completion;
    S.lastCacheHit=cacheHit; S.lastCacheMiss=cacheMiss;
    S.lastDuration=dur||(now-(S.genStart||now));

    S.totalPrompt+=prompt; S.totalCompletion+=completion;
    S.totalCacheHit+=cacheHit; S.totalCacheMiss+=cacheMiss;
    S.requests++; S.totalDuration+=S.lastDuration; S.streamTokens=0;

    const pr = getPrice(stateModel());
    if (pr) {
        S.cost += (cacheMiss/1e6)*pr.miss + (cacheHit/1e6)*pr.hit + (completion/1e6)*pr.output;
    }

    const tps = S.lastDuration>0?Math.round(completion/(S.lastDuration/1000)):0;
    const eff = (cacheHit+cacheMiss)>0?Math.round(cacheHit/(cacheHit+cacheMiss)*100):0;

    S.history.unshift({time:now,prompt,completion,cacheHit,cacheMiss,tps,eff,duration:S.lastDuration});
    if (S.history.length>HIST_MAX) S.history.pop();
    save(); refresh();
}

function stateModel() {
    try { return getGeneratingModel?.()?.label || getGeneratingModel?.()?.id || null; } catch { return null; }
}

// ═══════════════════════════════════════════════════════════════════════════
// Cache-breaker
// ═══════════════════════════════════════════════════════════════════════════

function checkPrefix() {
    const ctx=getContext(); if(!ctx?.chat)return;
    const cur=(ctx.chat.system_prompt||'').slice(0,2000);
    if(S.lastSysPrompt&&S.lastSysPrompt!==cur&&S.requests>0)S.cacheBreaks++;
    S.lastSysPrompt=cur;
}

// ═══════════════════════════════════════════════════════════════════════════
// Event handlers
// ═══════════════════════════════════════════════════════════════════════════

async function onGenStart() {
    if (S._finTimer) { clearTimeout(S._finTimer); S._finTimer=null; }
    S.round++; S.genStart=Date.now(); S.streamTokens=0;
    if (S.round>=2) S.toolAcc=true;

    if (S.round===1) {
        S.basePrompt = await countPrompt();
        S.lastPrompt = S.basePrompt;
    }
    checkPrefix();
    refresh();
}

function onStreamToken(t) {
    S.streamTokens += (typeof t==='string')?estTokens(t):1;
    if (S.streamTokens%5===0) refresh();
}

async function onGenEnd() {
    const dur = S.genStart?Date.now()-S.genStart:0;
    const usage = await getUsage();

    if (usage) {
        const rc = usage.output_tokens!==undefined?usage.output_tokens:(usage.completion_tokens||0);
        S.accCompletion += rc;
        S.lastCompletion = S.accCompletion;

        if (S.round===1) {
            const pc = usage.input_tokens!==undefined?usage.input_tokens:(usage.prompt_tokens||S.basePrompt);
            S.lastPrompt = S.basePrompt = pc;
        }

        const ca = detectCache(usage);
        S.lastCacheHit = ca.hit;
        S.lastCacheMiss = (usage.input_tokens||usage.prompt_tokens||S.basePrompt) - ca.hit;
        record(S.lastPrompt, S.lastCompletion, S.lastCacheHit, Math.max(0,S.lastCacheMiss), dur);
    } else {
        S.accCompletion += S.streamTokens;
        S.lastCompletion = S.accCompletion;
    }

    S.streamTokens=0; S.genStart=0;

    // Multi-round debounce
    if (S._finTimer) clearTimeout(S._finTimer);
    S._finTimer = setTimeout(() => { S.toolAcc=false; S.round=0; refresh(); }, 3000);

    if (cfg.showBadges) setTimeout(addBadge, 250);
    refresh();
}

function onGenStop() {
    S.accCompletion += S.streamTokens;
    S.lastCompletion = S.accCompletion;
    S.streamTokens=0; S.genStart=0;
    refresh();
}

function onChatChange() { swapChat(chatId()); }

// ═══════════════════════════════════════════════════════════════════════════
// Per-message badge
// ═══════════════════════════════════════════════════════════════════════════

function addBadge() {
    const ms = document.querySelectorAll('.mes');
    if (!ms.length) return;
    const last = ms[ms.length-1];
    if (last.querySelector('.tcm-badge')) return;

    const p=S.lastPrompt, c=S.lastCompletion, ch=S.lastCacheHit;
    const rate=p>0?Math.round(ch/p*100):null;
    const cr=(rate??0)>=50?'#4caf50':(rate??0)>=20?'#ff9800':'#f44336';
    const pr=getPrice(stateModel());
    const cost=pr?((S.lastCacheMiss/1e6)*pr.miss+(ch/1e6)*pr.hit+(c/1e6)*pr.output):0;

    const b=document.createElement('div'); b.className='tcm-badge';
    b.innerHTML=`<span class="tcm-bp">P:${fmt(p)}</span><span class="tcm-bc">C:${fmt(c)}</span>`+
        (ch>0?`<span class="tcm-bh" style="color:${cr}">⚡${fmt(ch)}(${rate??0}%)</span>`:'')+
        `<span class="tcm-b$">${fc(cost)}</span>`;
    last.appendChild(b);
}

function startBadgeObs() {
    if (!cfg.showBadges) return;
    const a=document.querySelector('#chat');
    if (!a) return setTimeout(startBadgeObs,1000);
    new MutationObserver(ms=>{for(const m of ms)for(const n of m.addedNodes)
        if(n.nodeType===1&&(n.classList?.contains('mes')||n.querySelector?.('.mes')))
            setTimeout(addBadge,300);
    }).observe(a,{childList:true,subtree:true});
}

// ═══════════════════════════════════════════════════════════════════════════
// Analytics
// ═══════════════════════════════════════════════════════════════════════════

function cScore(){const t=S.totalCacheHit+S.totalCacheMiss;return t>0?Math.round(S.totalCacheHit/t*100):0;}
function sLabel(s){return s>=80?{t:'优秀',c:'#4caf50'}:s>=50?{t:'良好',c:'#8bc34a'}:s>=30?{t:'一般',c:'#ff9800'}:{t:'偏低',c:'#f44336'};}
function avgT(){const v=S.history.filter(h=>h.tps>0);return v.length?Math.round(v.reduce((a,h)=>a+h.tps,0)/v.length):0;}
function avgR(){return S.requests?Math.round((S.totalPrompt+S.totalCompletion)/S.requests):0;}
function projCost(n){return S.requests?S.cost+(S.cost/S.requests)*n:0;}

// ═══════════════════════════════════════════════════════════════════════════
// UI
// ═══════════════════════════════════════════════════════════════════════════

let root=null, drag=false, dX=0, dY=0;
function $(s){return root?.querySelector(s);}

function render() {
    if (!root) return;
    const gen=getContext()?.generating??false;
    const lTot=S.lastPrompt+S.lastCompletion+S.streamTokens;
    const sTot=S.totalPrompt+S.totalCompletion+S.streamTokens;
    const lRate=S.lastPrompt>0?Math.round(S.lastCacheHit/S.lastPrompt*100):null;
    const eff=cScore(), el=sLabel(eff);
    const tps=avgT(), lTps=S.lastDuration>0?Math.round((S.lastCompletion||S.streamTokens)/(S.lastDuration/1000)):0;

    put('tcm-p',fmt(S.lastPrompt)); put('tcm-c',fmt(S.lastCompletion+S.streamTokens));
    put('tcm-t',fmt(lTot)); put('tcm-s',lTps>0?lTps+' tok/s':'-');
    put('tcm-h',fmt(S.lastCacheHit)); put('tcm-m',fmt(S.lastCacheMiss));
    put('tcm-r',lRate!==null?lRate+'%':'-');
    put('tcm-e',eff); put('tcm-el',el.t);
    put('tcm-sp',fmt(S.totalPrompt)); put('tcm-sc',fmt(S.totalCompletion+S.streamTokens));
    put('tcm-st',fmt(sTot)); put('tcm-sr',S.requests); put('tcm-sa',fmt(avgR()));
    put('tcm-ss',tps>0?tps+' tok/s':'-');
    put('tcm-co',fc(S.cost)); put('tcm-cp',fc(projCost(50)));
    put('tcm-d',gen?'🟢':'⚪');

    const cb=$('#tcm-cw'); if(cb)cb.style.display=S.cacheBreaks>0?'':'none';
    const re=$('#tcm-r'); if(re&&lRate!==null)re.style.color=lRate>=50?'#4caf50':lRate>=20?'#ff9800':'#f44336';
    const ba=$('#tcm-ef'); if(ba){ba.style.width=eff+'%';ba.style.background=el.c;}
    drawT();
}

function put(i,v){const e=$(`#${i}`);if(e)e.textContent=v;}

function drawT(){
    const ct=$('#tcm-tb');if(!ct||!cfg.showTrend)return;
    const bars=ct.querySelectorAll('.tcm-tb');
    const items=S.history.slice(0,bars.length).reverse();
    bars.forEach((b,i)=>{
        const it=items[i];
        if(it){const mx=Math.max(it.prompt,it.completion,1);
            b.querySelector('.tcm-tp').style.height=(it.prompt/mx*100)+'%';
            b.querySelector('.tcm-tc').style.height=(it.completion/mx*100)+'%';
            b.title=`#${S.requests-items.length+i+1}: P=${fmt(it.prompt)} C=${fmt(it.completion)} @${it.tps} tok/s`;
            b.style.opacity='1';}
        else{b.querySelector('.tcm-tp').style.height='0%';b.querySelector('.tcm-tc').style.height='0%';b.style.opacity='0.35';}
    });
}

const B=Array.from({length:HIST_MAX},()=>`<div class="tcm-tb"><div class="tcm-tp"></div><div class="tcm-tc"></div></div>`).join('');

const P=/*html*/`
<div id="tcm-panel" class="tcm-p${cfg.panelCollapsed?' tcm-c':''}">
  <div class="tcm-hd">
    <span class="tcm-hl"><span id="tcm-d">⚪</span> Token Monitor</span>
    <span class="tcm-hr">
      <button class="tcm-b" id="tcm-bs" title="设置">⚙</button>
      <button class="tcm-b" id="tcm-bt" title="折叠">${cfg.panelCollapsed?'➕':'➖'}</button>
      <button class="tcm-b" id="tcm-br" title="重置">↺</button>
      <button class="tcm-b" id="tcm-bx" title="隐藏">✕</button>
    </span>
  </div>
  <div class="tcm-bd"${cfg.panelCollapsed?' style="display:none"':''}>
    <div id="tcm-cw" class="tcm-cw" style="display:${S.cacheBreaks>0?'':'none'}">⚠ 缓存断裂 ${S.cacheBreaks}次 — prefix 已变化</div>
    <div class="tcm-s">
      <div class="tcm-st">▼ 最近</div>
      <div class="tcm-rx"><span>Prompt</span><span id="tcm-p">-</span></div>
      <div class="tcm-rx"><span>Completion</span><span id="tcm-c">-</span></div>
      <div class="tcm-rx"><span>总计</span><span id="tcm-t">-</span></div>
      <div class="tcm-rx" id="tcm-sr"${cfg.showThroughput?'':' style="display:none"'}><span>速度</span><span id="tcm-s">-</span></div>
    </div>
    <div class="tcm-s" id="tcm-cs"${cfg.showCache?'':' style="display:none"'}>
      <div class="tcm-st">▼ 缓存</div>
      <div class="tcm-rx"><span>命中</span><span class="tcm-g" id="tcm-h">-</span></div>
      <div class="tcm-rx"><span>未命中</span><span class="tcm-r2" id="tcm-m">-</span></div>
      <div class="tcm-rx"><span>命中率</span><span id="tcm-r">-</span></div>
      <div class="tcm-rx"><span>效率</span><span><span id="tcm-e">0</span> <span id="tcm-el" style="font-size:9px">-</span></span></div>
      <div class="tcm-eb"><div class="tcm-ef" id="tcm-ef"></div></div>
    </div>
    <div class="tcm-s" id="tcm-ss"${cfg.showSession?'':' style="display:none"'}>
      <div class="tcm-st">▼ 会话</div>
      <div class="tcm-rx"><span>Prompt</span><span id="tcm-sp">0</span></div>
      <div class="tcm-rx"><span>Completion</span><span id="tcm-sc">0</span></div>
      <div class="tcm-rx"><span>总计</span><span id="tcm-st">0</span></div>
      <div class="tcm-rx"><span>请求</span><span id="tcm-sr">0</span></div>
      <div class="tcm-rx"><span>均/请求</span><span id="tcm-sa">0</span></div>
      <div class="tcm-rx"><span>均速度</span><span id="tcm-ss">-</span></div>
    </div>
    <div class="tcm-s" id="tcm-cos"${cfg.showCost?'':' style="display:none"'}>
      <div class="tcm-st">▼ 费用 (¥)</div>
      <div class="tcm-rx"><span>本对话</span><span class="tcm-y" id="tcm-co">¥0</span></div>
      <div class="tcm-rx"><span>+50预估</span><span class="tcm-y" id="tcm-cp">¥0</span></div>
    </div>
    <div class="tcm-s" id="tcm-ts"${cfg.showTrend?'':' style="display:none"'}>
      <div class="tcm-st">▼ 趋势</div>
      <div class="tcm-tr"><div class="tcm-tb" id="tcm-tb">${B}</div>
        <div class="tcm-tl"><span><span class="tcm-lp"></span>Prompt</span><span><span class="tcm-lc"></span>Completion</span></div>
      </div>
    </div>
  </div>
</div>`;

function build() {
    if(root)root.remove();
    const w=document.createElement('div');w.innerHTML=P;root=w.firstElementChild;
    document.body.appendChild(root);place();wire();render();
}
function place(){
    if(!root)return;
    if(cfg.panelPos.x!==null){root.style.right=root.style.bottom='auto';root.style.left=cfg.panelPos.x+'px';root.style.top=cfg.panelPos.y+'px';}
    else{root.style.left=root.style.top='auto';root.style.right='12px';root.style.bottom='90px';}
}
function wire(){
    $('#tcm-bt')?.addEventListener('click',()=>{
        cfg.panelCollapsed=!cfg.panelCollapsed;saveCfg();
        const bd=root.querySelector('.tcm-bd'),bt=$('#tcm-bt');
        if(cfg.panelCollapsed){bd.style.display='none';if(bt)bt.textContent='➕';root.classList.add('tcm-c');}
        else{bd.style.display='';if(bt)bt.textContent='➖';root.classList.remove('tcm-c');}
    });
    $('#tcm-br')?.addEventListener('click',reset);
    $('#tcm-bx')?.addEventListener('click',()=>{root.style.display='none';showToggle();});
    $('#tcm-bs')?.addEventListener('click',settings);
    const h=root.querySelector('.tcm-hd');
    h?.addEventListener('mousedown',e=>{if(e.target.tagName==='BUTTON')return;drag=true;const r=root.getBoundingClientRect();dX=e.clientX-r.left;dY=e.clientY-r.top;root.style.cursor='grabbing';e.preventDefault();});
    h?.addEventListener('touchstart',e=>{if(e.target.tagName==='BUTTON')return;const t=e.touches[0];drag=true;const r=root.getBoundingClientRect();dX=t.clientX-r.left;dY=t.clientY-r.top;root.style.cursor='grabbing';},{passive:true});
    h?.addEventListener('dblclick',()=>{cfg.panelCollapsed=!cfg.panelCollapsed;saveCfg();rebuild();});
    document.addEventListener('mousemove',e=>{if(!drag)return;cfg.panelPos.x=e.clientX-dX;cfg.panelPos.y=e.clientY-dY;root.style.right=root.style.bottom='auto';root.style.left=cfg.panelPos.x+'px';root.style.top=cfg.panelPos.y+'px';});
    document.addEventListener('touchmove',e=>{if(!drag)return;const t=e.touches[0];cfg.panelPos.x=t.clientX-dX;cfg.panelPos.y=t.clientY-dY;root.style.right=root.style.bottom='auto';root.style.left=cfg.panelPos.x+'px';root.style.top=cfg.panelPos.y+'px';},{passive:false});
    document.addEventListener('mouseup',()=>{if(!drag)return;drag=false;root.style.cursor='';saveCfg();});
    document.addEventListener('touchend',()=>{if(!drag)return;drag=false;root.style.cursor='';saveCfg();});
}
function rebuild(){
    if(!root)return build();
    const pos=cfg.panelPos,col=cfg.panelCollapsed;root.remove();root=null;build();
    if(col){cfg.panelCollapsed=true;const bd=root.querySelector('.tcm-bd');bd.style.display='none';root.classList.add('tcm-c');}
    if(pos.x!==null){root.style.left=pos.x+'px';root.style.top=pos.y+'px';root.style.right=root.style.bottom='auto';}
    ['tcm-cs','tcm-ss','tcm-cos','tcm-ts','tcm-sr'].forEach(id=>{
        const e=$(`#${id}`);if(!e)return;
        if(id==='tcm-cs')e.style.display=cfg.showCache?'':'none';
        if(id==='tcm-ss')e.style.display=cfg.showSession?'':'none';
        if(id==='tcm-cos')e.style.display=cfg.showCost?'':'none';
        if(id==='tcm-ts')e.style.display=cfg.showTrend?'':'none';
        if(id==='tcm-sr')e.style.display=cfg.showThroughput?'':'none';
    });render();
}

// ── Toggle Σ button ──────────────────────────────────────────────────

let tgl=null;
function showToggle(){
    if(tgl){tgl.style.display='flex';return;}
    tgl=document.createElement('div');tgl.id='tcm-tgl';tgl.title='Token Monitor';
    tgl.innerHTML='<span style="font-size:18px;line-height:1">Σ</span>';
    Object.assign(tgl.style,{position:'fixed',zIndex:'9998',cursor:'pointer',width:'36px',height:'36px',borderRadius:'8px',
        background:'var(--SmartThemeBodyColor,#2a2a2a)',color:'var(--SmartThemeBodyTextColor,#ccc)',
        display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 8px rgba(0,0,0,0.3)',
        opacity:'0.7',transition:'opacity .2s',userSelect:'none',right:'12px',bottom:'50px'});
    tgl.addEventListener('mouseenter',()=>tgl.style.opacity='1');
    tgl.addEventListener('mouseleave',()=>tgl.style.opacity='0.7');
    tgl.addEventListener('click',()=>{if(root)root.style.display='';tgl.style.display='none';cfg.panelVisible=true;saveCfg();});
    document.body.appendChild(tgl);
}

// ═══════════════════════════════════════════════════════════════════════════
// Settings
// ═══════════════════════════════════════════════════════════════════════════

function settings(){
    const ov=document.createElement('div');ov.className='tcm-o';
    ov.innerHTML=/*html*/`
      <div class="tcm-set">
        <h3>Token &amp; Cache Monitor</h3>
        <label><input type="checkbox" id="tcs-b" ${cfg.showBadges?'checked':''}> 消息 token 徽章</label>
        <label><input type="checkbox" id="tcs-c" ${cfg.showCache?'checked':''}> 缓存信息</label>
        <label><input type="checkbox" id="tcs-s" ${cfg.showSession?'checked':''}> 会话统计</label>
        <label><input type="checkbox" id="tcs-o" ${cfg.showCost?'checked':''}> 费用估算</label>
        <label><input type="checkbox" id="tcs-t" ${cfg.showThroughput?'checked':''}> 吞吐量</label>
        <label><input type="checkbox" id="tcs-r" ${cfg.showTrend?'checked':''}> 趋势图</label>
        <label>计费: <select id="tcs-m">
          <option value="auto" ${cfg.costModel==='auto'?'selected':''}>自动检测</option>
          <option value="deepseek-v4-pro" ${cfg.costModel==='deepseek-v4-pro'?'selected':''}>DeepSeek V4 Pro</option>
          <option value="deepseek-v4-flash" ${cfg.costModel==='deepseek-v4-flash'?'selected':''}>DeepSeek V4 Flash</option>
          <option value="deepseek-v3" ${cfg.costModel==='deepseek-v3'?'selected':''}>DeepSeek V3</option>
          <option value="deepseek-r1" ${cfg.costModel==='deepseek-r1'?'selected':''}>DeepSeek R1</option>
          <option value="claude-sonnet-4" ${cfg.costModel==='claude-sonnet-4'?'selected':''}>Claude Sonnet 4</option>
          <option value="gpt-4o" ${cfg.costModel==='gpt-4o'?'selected':''}>GPT-4o</option>
          <option value="custom" ${cfg.costModel==='custom'?'selected':''}>自定义</option>
        </select></label>
        <div id="tcs-x" style="display:${cfg.costModel==='custom'?'block':'none'}">
          <label>命中 ¥/M: <input type="number" id="tcs-h" value="${cfg.customPrice.hit}" step="0.001"></label>
          <label>未命中 ¥/M: <input type="number" id="tcs-m2" value="${cfg.customPrice.miss}" step="0.01"></label>
          <label>输出 ¥/M: <input type="number" id="tcs-o2" value="${cfg.customPrice.output}" step="0.01"></label>
        </div>
        <div class="tcm-sb"><button id="tcs-ok">应用</button><button id="tcs-no">取消</button></div>
      </div>`;
    document.body.appendChild(ov);
    ov.querySelector('#tcs-m').addEventListener('change',function(){ov.querySelector('#tcs-x').style.display=this.value==='custom'?'block':'none';});
    ov.addEventListener('click',e=>{if(e.target===ov)ov.remove();});
    ov.querySelector('#tcs-no').addEventListener('click',()=>ov.remove());
    ov.querySelector('#tcs-ok').addEventListener('click',()=>{
        cfg.showBadges=ov.querySelector('#tcs-b').checked;cfg.showCache=ov.querySelector('#tcs-c').checked;
        cfg.showSession=ov.querySelector('#tcs-s').checked;cfg.showCost=ov.querySelector('#tcs-o').checked;
        cfg.showThroughput=ov.querySelector('#tcs-t').checked;cfg.showTrend=ov.querySelector('#tcs-r').checked;
        cfg.costModel=ov.querySelector('#tcs-m').value;
        if(cfg.costModel==='custom'){cfg.customPrice.hit=+ov.querySelector('#tcs-h').value||0;cfg.customPrice.miss=+ov.querySelector('#tcs-m2').value||0;cfg.customPrice.output=+ov.querySelector('#tcs-o2').value||0;}
        saveCfg();ov.remove();rebuild();
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// STscript
// ═══════════════════════════════════════════════════════════════════════════

function regCmd(){
    try{const ctx=getContext();if(typeof ctx.registerSlashCommand!=='function')return;
        ctx.registerSlashCommand('token-stats',()=>{const e=cScore(),l=sLabel(e),t=avgT();
            const m=['**Token Monitor**',`请求:${S.requests} 均/次:${fmt(avgR())}`,
                `Prompt:${fmt(S.totalPrompt)} Completion:${fmt(S.totalCompletion)}`,
                `缓存:✓${fmt(S.totalCacheHit)} ✗${fmt(S.totalCacheMiss)} 效率:${e}%(${l.t})`,
                `吞吐:${t} tok/s 费用:${fc(S.cost)} 预估+50:${fc(projCost(50))}`].join('\n');
            try{ctx.sendSystemMessage?.(m);}catch{console.log(m);}},{description:'显示 token 统计'});
        ctx.registerSlashCommand('token-reset',()=>{reset();try{ctx.sendSystemMessage?.('✅ 已重置');}catch{}},{description:'重置当前对话统计'});
    }catch{}
}

// ═══════════════════════════════════════════════════════════════════════════
// Init
// ═══════════════════════════════════════════════════════════════════════════

function refresh(){render();}

function init(){
    loadCfg();_chatId=chatId();S=load(_chatId);
    eventSource.on(event_types.GENERATION_STARTED,onGenStart);
    eventSource.on(event_types.STREAM_TOKEN_RECEIVED,onStreamToken);
    eventSource.on(event_types.GENERATION_ENDED,onGenEnd);
    eventSource.on(event_types.GENERATION_STOPPED,onGenStop);
    eventSource.on(event_types.CHAT_CHANGED,onChatChange);
    build();startBadgeObs();regCmd();
    if(!cfg.panelVisible){root.style.display='none';showToggle();}
}

jQuery(()=>{init();});

window.TokenCacheMonitor={stats:S,cfg,reset,refresh};
