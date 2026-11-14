// Ludo Royale - tiny free server: serves the app + relays room messages over WebSocket.
const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const PUB=__dirname,PORT=process.env.PORT||3000;
const TYPES={'.html':'text/html','.js':'text/javascript','.json':'application/json','.webmanifest':'application/manifest+json','.png':'image/png'};
const server=http.createServer((req,res)=>{
  let p=decodeURIComponent(req.url.split('?')[0]);if(p==='/health'){res.end('ok');return}
  if(p==='/ice'){const E=process.env;
    fetch('https://global.xirsys.net/_turn/'+E.XIRSYS_CHANNEL,{method:'PUT',headers:{Authorization:'Basic '+Buffer.from(E.XIRSYS_IDENT+':'+E.XIRSYS_SECRET).toString('base64'),'Content-Type':'application/json'},body:JSON.stringify({format:'urls'})})
    .then(r=>r.json()).then(j=>{let s=j&&j.v&&j.v.iceServers;s=s?(Array.isArray(s)?s:[s]):[];res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(s))})
    .catch(()=>{res.writeHead(200,{'Content-Type':'application/json'});res.end('[]')});return}
  if(p==='/')p='/index.html';const f=path.join(PUB,path.normalize(p));
  if(!f.startsWith(PUB)){res.writeHead(403);return res.end()}
  fs.readFile(f,(e,d)=>{if(e){res.writeHead(404);return res.end('not found')}
    res.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream'});res.end(d)})});
const wss=new WebSocketServer({server,path:'/ws',maxPayload:16*1024});
const rooms=new Map();let nid=1;
wss.on('connection',(ws,req)=>{
  const name=(new URL(req.url,'http://x').searchParams.get('room')||'').slice(0,40);
  if(!name){ws.close();return}
  ws.id='p'+nid++;ws.alive=true;ws.n=0;ws.t0=Date.now();
  let set=rooms.get(name);if(!set){set=new Set();rooms.set(name,set)}
  if(set.size>=8){ws.close();return}
  set.forEach(o=>o.readyState===1&&o.send(JSON.stringify({peers:[ws.id]})));
  set.add(ws);
  ws.on('pong',()=>ws.alive=true);
  ws.on('message',buf=>{
    const now=Date.now();if(now-ws.t0>1000){ws.t0=now;ws.n=0}if(++ws.n>60)return; // rate limit
    let m;try{m=JSON.parse(buf)}catch(e){return}if(!m||typeof m.t!=='string')return;
    const out=JSON.stringify({t:m.t,d:m.d});
    set.forEach(o=>{if(o!==ws&&o.readyState===1)o.send(out)})});
  ws.on('close',()=>{set.delete(ws);if(!set.size)rooms.delete(name)});
});
setInterval(()=>wss.clients.forEach(c=>{if(!c.alive)return c.terminate();c.alive=false;c.ping()}),25000);
server.listen(PORT,()=>console.log('Ludo server on '+PORT));
