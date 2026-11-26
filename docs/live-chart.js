(() => {
const canvas=document.getElementById('liveChart'),ctx=canvas.getContext('2d');
const select=document.getElementById('chartAsset'),status=document.getElementById('chartStatus'),priceEl=document.getElementById('chartPrice'),signalEl=document.getElementById('chartSignal'),timeEl=document.getElementById('chartTime');
const products={BTC:'BTC-USD',ETH:'ETH-USD',SOL:'SOL-USD',XRP:'XRP-USD',DOGE:'DOGE-USD'};
const latest={},series={};Object.keys(products).forEach(a=>series[a]=[]);
let socket=null,retry=1000,lastTick=0;
function connect(){
 status.textContent='CONNECTING LIVE FEED…';
 try{socket=new WebSocket('wss://ws-feed.exchange.coinbase.com');}
 catch(e){status.textContent='FEED UNAVAILABLE · RETRYING';setTimeout(connect,retry);retry=Math.min(retry*2,30000);return;}
 socket.onopen=()=>{retry=1000;status.textContent='LIVE FEED CONNECTED';socket.send(JSON.stringify({type:'subscribe',product_ids:Object.values(products),channels:['ticker']}));};
 socket.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='ticker'&&m.product_id&&m.price){const a=Object.keys(products).find(k=>products[k]===m.product_id);if(a){latest[a]=Number(m.price);lastTick=Date.now();}}}catch(_){}};
 socket.onerror=()=>{status.textContent='FEED ISSUE · RECONNECTING';};
 socket.onclose=()=>{status.textContent='FEED DISCONNECTED · RECONNECTING';setTimeout(connect,retry);retry=Math.min(retry*2,30000);};
}
function draw(){
 const a=select.value,arr=series[a],w=canvas.clientWidth,h=canvas.clientHeight,dpr=Math.max(1,window.devicePixelRatio||1);
 canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);
 ctx.clearRect(0,0,w,h);ctx.font='12px system-ui';ctx.fillStyle='#91a2bd';ctx.strokeStyle='#26344b';ctx.lineWidth=1;
 const left=64,right=12,top=18,bottom=26,pw=w-left-right,ph=h-top-bottom;
 for(let i=0;i<5;i++){const y=top+i*ph/4;ctx.beginPath();ctx.moveTo(left,y);ctx.lineTo(w-right,y);ctx.stroke();}
 if(arr.length<2){ctx.fillText('Waiting for live trades…',left,top+25);return;}
 const vals=arr.map(x=>x.p),lo=Math.min(...vals),hi=Math.max(...vals),pad=(hi-lo)*.12||Math.max(hi*.0002,.01),min=lo-pad,max=hi+pad;
 const y=p=>top+(max-p)/(max-min)*ph;
 for(let i=0;i<5;i++){const v=max-i*(max-min)/4;ctx.fillText(v.toLocaleString('en-US',{maximumFractionDigits:2}),4,top+i*ph/4+4);}
 ctx.strokeStyle='#53d8ff';ctx.lineWidth=2;ctx.beginPath();
 arr.forEach((pt,i)=>{const x=left+i*pw/(arr.length-1);if(i===0)ctx.moveTo(x,y(pt.p));else ctx.lineTo(x,y(pt.p));});ctx.stroke();
 const cur=arr[arr.length-1];ctx.fillStyle='#45e0a0';ctx.beginPath();ctx.arc(w-right,y(cur.p),4,0,Math.PI*2);ctx.fill();
 ctx.fillStyle='#91a2bd';ctx.fillText('120 seconds · 1-second samples',left,h-7);
}
function sample(){const a=select.value,p=latest[a];if(p){const now=Date.now();series[a].push({t:now,p});series[a]=series[a].filter(x=>now-x.t<=120000);priceEl.textContent=p.toLocaleString('en-US',{style:'currency',currency:'USD',maximumFractionDigits:p<1?5:2});timeEl.textContent='Last market tick '+new Date(lastTick).toLocaleTimeString();status.textContent=Date.now()-lastTick<10000?'LIVE · TICK DATA':'PRICE STALE · WAITING FOR TICK';}else{priceEl.textContent='—';timeEl.textContent='Waiting for first trade price';}draw();}
async function updateSignal(){try{const r=await fetch('data.json?t='+Date.now());if(!r.ok)return;const d=await r.json();const a=select.value,ms=(d.markets||[]).filter(m=>m.asset===a&&[5,15].includes(Number(m.timeframe))).sort((x,y)=>x.timeframe-y.timeframe);signalEl.textContent=ms.length?ms.map(m=>m.timeframe+'m '+m.side+' · '+(m.action||'')).join('  |  '):'No verified bot signal for '+a+' yet';}catch(e){signalEl.textContent='Bot signal snapshot unavailable';}}
select.addEventListener('change',()=>{sample();updateSignal();});
window.addEventListener('resize',draw);connect();sample();updateSignal();setInterval(sample,1000);setInterval(updateSignal,30000);
})();