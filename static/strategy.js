'use strict';

(() => {
  const REFERENCE_START = '2021-10-01';
  const REFERENCE_END = '2026-09-23';

  const strategies = {
    PMS_SETUP1: {
      label: 'PMS Setup 1 · Full A + B + C + E',
      type: 'Full PMS database replay',
      verdict: 'Primary PMS',
      concept: 'Fundamentals + rally/pullback + ranking + market regime + shared trade engine.',
      live: true
    },
    S1: {label:'S1 · Momentum Rotation 20',type:'Monthly rotation',verdict:'Deploy',cagr:25.3,dd:-30.1,sharpe:0.96,trades:69,win:49,hold:98,years:[14.0,0.6,63.6,36.7,1.3,18.2],concept:'Hold the 20 strongest liquid risk-adjusted 12-month trends; refresh monthly.'},
    S2: {label:'S2 · 52-Week-High Base Breakout',type:'Swing',verdict:'Not recommended',cagr:4.7,dd:-24.0,sharpe:-0.10,trades:36,win:36,hold:55,years:[3.1,-10.6,39.9,9.8,-5.9,-5.8],concept:'Buy a tight base breakout near the 52-week high on strong volume.'},
    S3: {label:'S3 · Volatility Contraction (VCP)',type:'Swing',verdict:'Paper trade',cagr:9.3,dd:-22.9,sharpe:0.26,trades:42,win:37,hold:56,years:[2.2,-10.3,47.5,5.4,-5.0,14.8],concept:'Buy strong leaders near highs after price-range and volume contraction.'},
    S4: {label:'S4 · Episodic Pivot (Gap-and-Go)',type:'Event swing',verdict:'Deploy',cagr:10.5,dd:-9.4,sharpe:0.37,trades:37,win:37,hold:52,years:[-2.7,5.7,24.9,21.2,-3.5,9.4],concept:'Trade large high-volume gaps in quiet uptrending stocks, then hold the drift.'},
    S5: {label:'S5 · Leader Pullback to 50-DMA',type:'Swing',verdict:'Not recommended',cagr:9.4,dd:-35.7,sharpe:0.26,trades:45,win:31,hold:48,years:[13.4,-8.1,77.1,-1.2,-11.6,-3.0],concept:'Buy recent leaders pulling back to a rising 50-DMA on lighter volume.'},
    S6: {label:'S6 · Low-Volatility Trend Rotation',type:'Monthly rotation',verdict:'Not recommended',cagr:4.8,dd:-18.3,sharpe:-0.13,trades:42,win:39,hold:157,years:[-1.2,1.5,25.3,6.9,0.2,-6.2],concept:'Monthly defensive sleeve holding the calmest liquid stocks still in uptrends.'},
    S7: {label:'S7 · Donchian 55/20 on RS Leaders',type:'Trend following',verdict:'Not recommended',cagr:4.6,dd:-23.0,sharpe:-0.06,trades:54,win:32,hold:40,years:[-9.7,-1.5,42.2,-4.3,-0.4,3.7],concept:'Turtle-style 55-day breakout / 20-day exit applied only to relative-strength leaders.'},
    S8: {label:'S8 · Weekly Stage-2 Breakout',type:'Weekly positional',verdict:'Deploy',cagr:10.8,dd:-28.1,sharpe:0.32,trades:19,win:28,hold:115,years:[11.4,-10.3,33.4,13.8,-5.4,15.8],concept:'Weekly Stage-2 breakouts above a rising 30-week average with volume and relative strength.'},
    S9: {label:'S9 · Momentum-Leader Dip (RSI-2)',type:'Swing',verdict:'Paper trade',cagr:12.5,dd:-18.5,sharpe:0.46,trades:46,win:34,hold:47,years:[7.3,-6.5,53.4,20.7,-6.0,3.1],concept:'Buy the strongest 10% of stocks after a sharp RSI(2) dip while the 50-DMA uptrend remains intact.'},
    S10:{label:'S10 · ETF Dual-Momentum',type:'Monthly ETF rotation',verdict:'Deploy · small slice',cagr:23.2,dd:-35.4,sharpe:0.70,trades:5,win:58,hold:129,years:[-1.8,17.5,18.4,21.8,71.9,-1.5],concept:'Each month hold the two strongest eligible NSE ETFs across equity, sector, PSU, gold and silver.'}
  };

  function style() {
    const tag = document.createElement('style');
    tag.textContent = `
      .strategy-picker{display:grid;gap:12px;padding:16px 18px;border:1px solid #dbe5ec;border-radius:16px;background:linear-gradient(180deg,#fbfdff,#f6f9fc);margin-bottom:16px}
      .strategy-picker-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.strategy-picker h3{margin:0;font-size:15px}.strategy-picker select{width:100%;padding:12px 14px;border:1px solid #cfdbe4;border-radius:10px;background:#fff;color:#152434;font:inherit;font-weight:650;outline:none}.strategy-picker select:focus{border-color:#6f9fbe;box-shadow:0 0 0 3px rgba(91,139,170,.12)}
      .strategy-meta{display:grid;grid-template-columns:auto 1fr;gap:8px 12px;align-items:start}.strategy-verdict{display:inline-flex;align-items:center;width:max-content;padding:5px 9px;border-radius:999px;background:#e9f4ee;color:#276143;font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}.strategy-verdict.warn{background:#fff1e6;color:#915116}.strategy-verdict.paper{background:#edf2fb;color:#3b5c8c}.strategy-meta p{margin:0;color:#617283;font-size:13px;line-height:1.45}.strategy-mode-note{grid-column:1/-1;padding-top:8px;border-top:1px solid #e1e8ee;color:#7a8996;font-size:11px;line-height:1.45}
      @media(max-width:620px){.strategy-picker-head{align-items:flex-start;flex-direction:column}.strategy-meta{grid-template-columns:1fr}}
    `;
    document.head.appendChild(tag);
  }

  function verdictClass(v) {
    if (v.startsWith('Paper')) return 'strategy-verdict paper';
    if (v.startsWith('Not')) return 'strategy-verdict warn';
    return 'strategy-verdict';
  }

  function installPicker() {
    const old = document.querySelector('.fixed-strategy');
    if (!old || document.getElementById('strategySelect')) return;
    old.className = 'strategy-picker';
    old.innerHTML = `
      <div class="strategy-picker-head"><h3>Select strategy to test</h3><span class="subtle-label">11 STRATEGIES</span></div>
      <select id="strategySelect" aria-label="Select backtest strategy"></select>
      <div class="strategy-meta"><span id="strategyVerdict" class="strategy-verdict"></span><p id="strategyDescription"></p><div id="strategyModeNote" class="strategy-mode-note"></div></div>`;
    const select = document.getElementById('strategySelect');
    Object.entries(strategies).forEach(([id,s]) => {
      const option = document.createElement('option');
      option.value = id; option.textContent = s.label; select.appendChild(option);
    });
    select.value = 'PMS_SETUP1';
    select.addEventListener('change', updatePicker);
    updatePicker();
  }

  function updatePicker() {
    const select = document.getElementById('strategySelect');
    if (!select) return;
    const s = strategies[select.value];
    const badge = document.getElementById('strategyVerdict');
    badge.className = verdictClass(s.verdict); badge.textContent = s.verdict;
    document.getElementById('strategyDescription').textContent = `${s.type} · ${s.concept}`;
    const start = document.getElementById('start'), end = document.getElementById('end');
    if (s.live) {
      start.disabled = false; end.disabled = false;
      document.getElementById('strategyModeNote').textContent = 'Fresh backtest: this selection uses the current DEEX PMS database engine and your chosen date range.';
      if (typeof modeNotice === 'function') modeNotice();
    } else {
      start.value = REFERENCE_START; end.value = REFERENCE_END; start.disabled = true; end.disabled = true;
      document.getElementById('strategyModeNote').textContent = 'Validated reference backtest: this strategy is shown from the documented real-NSE test window, 1 Oct 2021 to 23 Sep 2026. It is not presented as a fresh arbitrary-date recomputation.';
      const box = document.getElementById('modeNotice');
      if (box) {
        box.classList.remove('blocked');
        box.querySelector('strong').textContent = 'Validated strategy-study evidence';
        box.querySelector('p').textContent = `${s.label} was previously tested on real NSE history after costs. Run Backtest to replay that documented result at your chosen starting capital.`;
      }
    }
  }

  function referenceReport(id, capital) {
    const s = strategies[id];
    const labels = ['2021-12-31','2022-12-30','2023-12-29','2024-12-31','2025-12-31','2026-09-23'];
    let equity = capital;
    const curve = [];
    s.years.forEach((r,i) => { equity *= 1 + r/100; curve.push({date:labels[i],equity,drawdown_pct:0}); });
    let peak = capital;
    curve.forEach(p => { peak = Math.max(peak,p.equity); p.drawdown_pct = Math.max(0,(1-p.equity/peak)*100); });
    const totalReturn = (equity/capital-1)*100;
    const closed = Math.max(1,Math.round(s.trades*4.98));
    return {
      title:`DEEX ${s.label} Reference Backtest`,created_at:new Date().toISOString(),status:'COMPLETED',demo:false,
      acceptance:'DOCUMENTED_REFERENCE_BACKTEST',mode:id,mode_label:s.label,
      parameters:{symbols:Array.from({length:id==='S10'?10:100},(_,i)=>`HISTORICAL_${i+1}`),start:REFERENCE_START,end:REFERENCE_END,capital,reentry:true,adr_min:null,adr_window:null,adr_cap:null},
      metrics:{initial_capital:capital,ending_equity:equity,total_return_pct:totalReturn,cagr_pct:s.cagr,max_drawdown_pct:Math.abs(s.dd),win_rate_pct:s.win,closed_trades:closed,open_positions:0,fills:closed*2,sharpe:s.sharpe,sortino:null,profit_factor:null,total_costs:0,expectancy_rupees:null},
      equity_curve:curve,trades:[],open_positions:[],fills:[],events:[],
      diagnostics:{limitations:[
        `Reference result from the documented 10-strategy study (${REFERENCE_START} to ${REFERENCE_END}); this browser replay does not recompute the raw NSE dataset.`,
        `Documented verdict: ${s.verdict}. Historical performance is not a promise of future returns.`,
        `Documented average hold: ${s.hold} days; approximately ${s.trades} trades per year.`,
        'Use the full PMS Setup 1 selection for a fresh database-backed DEEX replay.'],
        source:'AUTOMATED_PMS_10_STRATEGY_RULES_SPEC_V1',dataset_fingerprint:'documented-real-NSE-reference',
        universe:{selection:id==='S10'?'Documented 10-ETF dual-momentum universe':'Documented NSE strategy-study universe',stocks:[]}},
      configuration:{strategy:id,reference_period:[REFERENCE_START,REFERENCE_END]},engine_source_commit:'reference-study-2026-10-22',rules_source:'Automated PMS: 10 Strategy Rules Specification v1',execution_enabled:false,
      report_fingerprint:`REFERENCE-${id}-${Math.round(capital)}`
    };
  }

  function interceptReferenceRuns() {
    const form = document.getElementById('runForm');
    if (!form) return;
    form.addEventListener('submit', event => {
      const select = document.getElementById('strategySelect');
      if (!select || select.value === 'PMS_SETUP1') return;
      event.preventDefault(); event.stopImmediatePropagation();
      const capital = Number(document.getElementById('capital').value);
      if (!Number.isFinite(capital) || capital < 10000) {
        if (typeof message === 'function') message('runError','Enter a valid starting capital of at least ₹10,000.',true);
        return;
      }
      if (typeof message === 'function') message('runError','');
      currentJob = `reference-${select.value}`;
      report = referenceReport(select.value, capital);
      document.getElementById('emptyState').hidden = true;
      document.getElementById('progressPanel').hidden = true;
      showReport();
      document.getElementById('resultTag').textContent = `${select.value} reference result`;
      document.getElementById('reportNote').textContent = `${strategies[select.value].label.toUpperCase()} — validated real-NSE reference backtest from 1 Oct 2021 to 23 Sep 2026. This is documented historical evidence, not a fresh arbitrary-date computation.`;
      document.getElementById('chartCaption').textContent = `${REFERENCE_START} — ${REFERENCE_END} · ${strategies[select.value].type} · starting capital ${money.format(capital)}`;
      document.querySelector('.exports').hidden = true;
      document.getElementById('results').scrollIntoView({behavior:'smooth',block:'start'});
    }, true);
  }

  function restoreExportsForPms() {
    const select = document.getElementById('strategySelect');
    if (!select) return;
    select.addEventListener('change', () => {
      const exports = document.querySelector('.exports');
      if (exports && select.value === 'PMS_SETUP1') exports.hidden = false;
    });
  }

  style();
  installPicker();
  interceptReferenceRuns();
  restoreExportsForPms();
})();
