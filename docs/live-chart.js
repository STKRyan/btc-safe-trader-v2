(() => {
const select=document.getElementById('chartAsset'),status=document.getElementById('chartStatus'),priceEl=document.getElementById('chartPrice'),signalEl=document.getElementById('chartSignal'),timeEl=document.getElementById('chartTime'),host=document.getElementById('liveChart');
const symbols={BTC:'COINBASE:BTCUSD',ETH:'COINBASE:ETHUSD',SOL:'COINBASE:SOLUSD',XRP:'COINBASE:XRPUSD',DOGE:'COINBASE:DOGEUSD'};
let loaded=false;
function loadChart(){
 host.innerHTML='';
 const wrap=document.createElement('div');wrap.className='tradingview-widget-container';wrap.style.height='390px';wrap.style.width='100%';
 const widget=document.createElement('div');widget.className='tradingview-widget-container__widget';widget.style.height='390px';widget.style.width='100%';
 wrap.appendChild(widget);
 const attribution=document.createElement('div');attribution.className='tradingview-widget-copyright';attribution.innerHTML='<a href="https://www.tradingview.com/" rel="noopener nofollow" target="_blank"><span class="blue-text">Track all markets on TradingView</span></a>';
 wrap.appendChild(attribution);
 const script=document.createElement('script');script.type='text/javascript';script.src='https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';script.async=true;
 script.text=JSON.stringify({autosize:true,symbol:symbols[select.value],interval:'1',timezone:'Etc/UTC',theme:'dark',style:'1',locale:'en',allow_symbol_change:true,calendar:false,support_host:'https://www.tradingview.com',hide_top_toolbar:false,hide_legend:false,save_image:false});
 host.appendChild(wrap);wrap.appendChild(script);
 status.textContent='LIVE CHART · TradingView market feed';priceEl.textContent='Live market chart';timeEl.textContent='Choose chart intervals inside the chart; the bot signal below is a separate scanner snapshot.';
}
async function updateSignal(){try{const r=await fetch('data.json?t='+Date.now());if(!r.ok)throw Error();const d=await r.json();const a=select.value,ms=(d.markets||[]).filter(m=>m.asset===a&&[5,15].includes(Number(m.timeframe))).sort((x,y)=>x.timeframe-y.timeframe);signalEl.textContent=ms.length?ms.map(m=>m.timeframe+'m '+m.side+' · '+(m.action||'')).join('  |  '):'No verified bot signal for '+a+' yet';}catch(e){signalEl.textContent='Latest bot snapshot unavailable';}}
select.addEventListener('change',()=>{loadChart();updateSignal();});loadChart();updateSignal();setInterval(updateSignal,30000);
})();