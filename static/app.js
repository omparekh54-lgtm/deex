'use strict';
const $=id=>document.getElementById(id);
let csrfToken='';
let state={broker_connected:false,database_configured:false}, currentJob=null, report=null, pollTimer=null, resultView='trades';
const money=new Intl.NumberFormat('en-IN',{maximumFractionDigits:0,style:'currency',currency:'INR'});
const num=(n,d=2)=>n==null?'—':Number(n).toLocaleString('en-IN',{maximumFractionDigits:d});
const pct=n=>n==null?'—':num(n)+'%';
async function api(path,body){
 if(body!==undefined&&!csrfToken){const session=await api('/api/status');csrfToken=session.csrf_token;}
 const response=await fetch(path,{method:body===undefined?'GET':'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:body===undefined?{}:{'Content-Type':'application/json','X-CSRF-Token':csrfToken},body:body===undefined?undefined:JSON.stringify(body)});
 const data=await response.json(); if(data.csrf_token)csrfToken=data.csrf_token; if(!response.ok)throw new Error(typeof data.detail==='string'?data.detail:'Request failed. Please retry.');return data;
}
function message(id,text,error=false){$(id).textContent=text;$(id).classList.toggle('error',error);}
function updateStatus(){
 $('connectionState').textContent=state.broker_connected?'Verified':'Not verified';$('connectionState').classList.toggle('connected',state.broker_connected);
 $('connectForm').hidden=state.broker_connected;$('disconnect').hidden=!state.broker_connected;modeNotice();
}
function modeNotice(){
 const box=$('modeNotice');box.classList.toggle('blocked',!state.database_configured);
 box.querySelector('strong').textContent=state.database_configured?'Full PMS strategy · 100-stock scan':'Historical dataset connection required';
 box.querySelector('p').textContent=state.database_configured?'The app selects 100 stocks from pre-start historical data and replays your full A + B + C + E strategy. All input coverage checks must pass.':'Your full PMS strategy needs historical fundamentals, prices and reference data. A run starts only when the complete dataset is available.';
}
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>{document.querySelectorAll('.view').forEach(v=>v.hidden=v.id!==button.dataset.view);document.querySelectorAll('.nav').forEach(v=>v.classList.toggle('active',v===button));if(button.dataset.view==='lab'&&report)drawChart();}));
$('connectForm').addEventListener('submit',async event=>{
 event.preventDefault();$('connectButton').disabled=true;message('connectMessage','Connecting securely…');
 const body={api_key:$('apiKey').value,method:'totp',client_code:$('clientCode').value,pin:$('pin').value,totp:$('totp').value};
 ['apiKey','pin','totp'].forEach(id=>$(id).value='');
 try{await api('/api/connect',body);state.broker_connected=true;['apiKey','pin','totp'].forEach(id=>$(id).value='');message('connectMessage','Connection verified. Your credentials were discarded.');updateStatus();}
 catch(error){message('connectMessage',error.message,true);$('apiKey').value='';$('pin').value='';$('totp').value='';}
 finally{Object.keys(body).forEach(k=>body[k]='');$('connectButton').disabled=false;}
});
$('disconnect').addEventListener('click',async()=>{try{await api('/api/disconnect',{});state.broker_connected=false;updateStatus();report=null;currentJob=null;clearTimeout(pollTimer);$('reportPanel').hidden=true;$('progressPanel').hidden=true;$('emptyState').hidden=false;lockRun(false);message('connectMessage','Session cleared. Previous report links are invalid.');}catch(e){message('connectMessage',e.message,true);}});
function lockRun(locked){$('runButton').disabled=locked;}
function begin(job){
 currentJob=job.id;report=null;lockRun(true);$('emptyState').hidden=true;$('reportPanel').hidden=true;$('progressPanel').hidden=false;$('cancelButton').disabled=false;$('resultTag').textContent='In progress';message('runError','');$('progressBar').value=0;$('progressPercent').textContent='0%';$('progressMessage').textContent='Waiting for research worker';$('results').scrollIntoView({behavior:'smooth',block:'start'});poll();
}
$('runForm').addEventListener('submit',async event=>{
 event.preventDefault();message('runError','');
 if(!state.database_configured){message('runError','The full PMS historical dataset is not connected yet. This is required to run your strategy across 100 stocks.',true);return;}
 const body={start:$('start').value,end:$('end').value,capital:Number($('capital').value)};
 lockRun(true);try{begin(await api('/api/runs',body));}catch(e){message('runError',e.message,true);lockRun(false);}
});
$('cancelButton').addEventListener('click',async()=>{if(!currentJob)return;try{await api('/api/runs/'+currentJob+'/cancel',{});$('cancelButton').disabled=true;$('progressMessage').textContent='Cancelling after the current operation…';}catch(e){message('runError',e.message,true);}});
async function poll(){
 try{
 const job=await api('/api/runs/'+currentJob);$('progressBar').value=job.progress;$('progressPercent').textContent=job.progress+'%';$('progressMessage').textContent=job.message;
 if(job.status==='QUEUED'||job.status==='RUNNING'){pollTimer=setTimeout(poll,1500);return;}
 lockRun(false);$('progressPanel').hidden=true;$('resultTag').textContent=job.status.replaceAll('_',' ');
 if(job.report){report=job.report;showReport();}
 else{$('emptyState').hidden=false;message('runError',job.error||job.message,true);}
 }catch(error){lockRun(false);$('progressPanel').hidden=true;$('emptyState').hidden=false;message('runError',error.message+' Reports may have expired after a service restart.',true);}
}
function element(tag,text,className){const e=document.createElement(tag);if(text!=null)e.textContent=text;if(className)e.className=className;return e;}
function showReport(){
 $('reportPanel').hidden=false;$('emptyState').hidden=true;$('resultTag').textContent=report.status==='NO_TRADES'?'No closed trades':'100-stock PMS report';
 $('reportNote').textContent='FULL PMS STRATEGY — 100 automatically selected stocks, one shared portfolio, original strategy rules. Historical results remain exploratory and uncertified.';
 const m=report.metrics;const cards=[['Total return',pct(m.total_return_pct),money.format(m.ending_equity)+' ending equity',m.total_return_pct],['Annualized return',pct(m.cagr_pct),'CAGR over selected dates',m.cagr_pct],['Max drawdown',pct(m.max_drawdown_pct),'Daily marked portfolio NAV',-m.max_drawdown_pct],['Win rate',pct(m.win_rate_pct),m.closed_trades+' completed cycles',null]];
 $('metrics').replaceChildren(...cards.map(([label,value,sub,sign])=>{const box=element('div',null,'metric');box.append(element('div',label,'metric-label'),element('div',value,'metric-value'+(sign==null?'':sign<0?' negative':' positive')),element('div',sub,'metric-sub'));return box;}));
 $('chartCaption').textContent=report.parameters.start+' — '+report.parameters.end+' · '+report.parameters.symbols.length+' automatically selected NSE stocks';
 ['csv','json','html'].forEach(kind=>$(kind+'Download').href='/api/runs/'+currentJob+'/download/'+kind);
 resultView='trades';document.querySelectorAll('.result-tab').forEach(b=>b.classList.toggle('active',b.dataset.result===resultView));renderTable();requestAnimationFrame(drawChart);
}
function table(headers,rows){
 const t=element('table'),thead=element('thead'),tr=element('tr');headers.forEach(h=>tr.append(element('th',h)));thead.append(tr);t.append(thead);const body=element('tbody');rows.forEach(row=>{const tr=element('tr');row.forEach(v=>tr.append(element('td',v==null?'—':String(v))));body.append(tr);});t.append(body);return t;
}
function renderTable(){
 const area=$('tableArea');area.replaceChildren();if(!report)return;
 if(resultView==='diagnostics'){
 const box=element('div',null,'diagnostics');const m=report.metrics;
 box.append(element('p','Sharpe: '+num(m.sharpe)+' · Sortino: '+num(m.sortino)+' · Profit factor: '+num(m.profit_factor)+' · Total costs: '+money.format(m.total_costs)));
 box.append(element('p','Open positions: '+m.open_positions+' · All fills: '+m.fills+' · Expectancy per completed cycle: '+(m.expectancy_rupees==null?'—':money.format(m.expectancy_rupees))));
 const ul=element('ul');(report.diagnostics.limitations||[]).forEach(v=>ul.append(element('li',v)));box.append(ul);
 if(report.diagnostics.rejections&&Object.keys(report.diagnostics.rejections).length){box.append(element('strong','Signal rejection counts'));box.append(element('p',Object.entries(report.diagnostics.rejections).map(([k,v])=>k.replaceAll('_',' ')+': '+v).join(' · ')));}
 box.append(element('p','Report SHA256: '+report.report_fingerprint,'fingerprint'));box.append(element('p','Dataset: '+(report.diagnostics.dataset_fingerprint||'Not recorded'),'fingerprint'));box.append(element('p','Engine source commit: '+report.engine_source_commit,'fingerprint'));area.append(box);return;
 }
 if(resultView==='universe'){
 const universe=report.diagnostics.universe||{};
 area.append(element('p',universe.selection||'Historical stock sample','diagnostics'));
 area.append(table(['#','Stock','Sector','Prior average daily value'],(universe.stocks||[]).map((r,i)=>[i+1,r.instrument_id,r.sector,money.format(r.adtv)])));
 return;
 }
 if(resultView==='drawdown'){area.append(table(['Session','Portfolio equity','Drawdown'],report.equity_curve.map(r=>[r.date,money.format(r.equity),pct(r.drawdown_pct)])));return;}
 const rows=resultView==='trades'?report.trades:report.fills;
 if(!rows.length){area.append(element('div',resultView==='trades'?'No completed position cycles. Inspect all fills, open positions and coverage assumptions.':'No fills were generated for this period.','table-empty'));return;}
 if(resultView==='fills')area.append(table(['Symbol','Date','Side','Price','Shares','Costs','Reason'],rows.map(r=>[r.instrument_id,r.trade_date,r.side,num(r.price),r.quantity,num(r.costs.total),r.reason.replaceAll('_',' ')])));
 else area.append(table(['Symbol','Entry date','Exit date','Net P&L','R multiple','Days'],rows.map(r=>[r.instrument,r.entry_date,r.exit_date,money.format(r.pnl),num(r.r_multiple),r.holding_days])));
}
document.querySelectorAll('.result-tab').forEach(button=>button.addEventListener('click',()=>{resultView=button.dataset.result;document.querySelectorAll('.result-tab').forEach(b=>b.classList.toggle('active',b===button));renderTable();}));
function drawChart(){
 if(!report||$('lab').hidden)return;
 const canvas=$('equityChart'),rect=canvas.getBoundingClientRect(),w=rect.width,h=rect.height,dpr=window.devicePixelRatio||1;
 if(!w||!h)return;canvas.width=w*dpr;canvas.height=h*dpr;const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);
 const points=[{date:report.parameters.start,equity:report.metrics.initial_capital},...report.equity_curve];
 const min=Math.min(...points.map(p=>p.equity)),max=Math.max(...points.map(p=>p.equity)),spread=max-min||max*.01,lo=min-spread*.12,hi=max+spread*.12;
 const left=65,right=w-12,top=14,bottom=h-26,x=i=>left+i/(points.length-1||1)*(right-left),y=v=>bottom-(v-lo)/(hi-lo)*(bottom-top);
 ctx.font='9px -apple-system, sans-serif';ctx.textBaseline='middle';
 for(let i=0;i<4;i++){const v=lo+i/3*(hi-lo),yy=y(v);ctx.strokeStyle='#edf1f5';ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();ctx.fillStyle='#98a8b6';ctx.fillText(v>=1e7?(v/1e7).toFixed(2)+'cr':v>=1e5?(v/1e5).toFixed(2)+'L':Math.round(v).toLocaleString(),2,yy);}
 const gradient=ctx.createLinearGradient(0,top,0,bottom);gradient.addColorStop(0,'rgba(71,132,177,.17)');gradient.addColorStop(1,'rgba(71,132,177,.01)');
 ctx.beginPath();ctx.moveTo(x(0),bottom);points.forEach((p,i)=>ctx.lineTo(x(i),y(p.equity)));ctx.lineTo(x(points.length-1),bottom);ctx.closePath();ctx.fillStyle=gradient;ctx.fill();
 ctx.beginPath();points.forEach((p,i)=>i?ctx.lineTo(x(i),y(p.equity)):ctx.moveTo(x(i),y(p.equity)));ctx.strokeStyle='#4d88b2';ctx.lineWidth=1.8;ctx.stroke();
 ctx.fillStyle='#99a8b4';ctx.textBaseline='bottom';for(let i=0;i<3;i++){const idx=Math.round(i/2*(points.length-1));const text=points[idx].date;ctx.fillText(text,Math.min(w-ctx.measureText(text).width,x(idx)),h-1);}
 canvas.onmousemove=event=>{const i=Math.max(0,Math.min(points.length-1,Math.round((event.offsetX-left)/(right-left)*(points.length-1))));const p=points[i];$('chartTooltip').textContent=p.date+' · '+money.format(p.equity);$('chartTooltip').hidden=false;$('chartTooltip').style.left=Math.max(0,Math.min(w-180,event.offsetX-80))+'px';};canvas.onmouseleave=()=>{$('chartTooltip').hidden=true;};
}
let resizeTimer;window.addEventListener('resize',()=>{clearTimeout(resizeTimer);resizeTimer=setTimeout(drawChart,100);});
(async()=>{try{state=await api('/api/status');updateStatus();}catch(e){message('runError','Service is starting. Refresh in a moment.',true);}})();
