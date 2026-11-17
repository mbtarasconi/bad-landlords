// Bad Landlords: static page + tiny realtime relay (Server-Sent Events + POST). No dependencies.
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const ROOT = __dirname;
const MAX_BODY = 64 * 1024;
const MAX_PER_CH = 40;          // game rooms: 6 players + reconnect slack
const MAX_LOBBY = 5000;         // the lobby is shared by everyone on the site
const MAX_PER_IP = 80;          // open streams per address, against floods
const CH_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const ID_RE = /^[A-Za-z0-9_-]{4,64}$/;

const channels = new Map(); // ch -> { clients: Map(id -> res), presence: Map(id -> obj) }
function chan(ch) {
  let c = channels.get(ch);
  if (!c) { c = { clients: new Map(), presence: new Map() }; channels.set(ch, c); }
  return c;
}
function send(res, event, data) { try { res.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n'); } catch (e) {} }
function peersOf(c) { return [...c.clients.keys()].map(id => ({ id, presence: c.presence.get(id) || null })); }
function pushNow(c) { c.timer = null; c.last = Date.now(); const l = peersOf(c); for (const res of c.clients.values()) send(res, 'peers', l); }
// big channels (the lobby) coalesce peer updates to at most one every 2s
function pushPeers(c) {
  if (c.clients.size <= 12) return pushNow(c);
  if (c.timer) return;
  c.timer = setTimeout(() => pushNow(c), Math.max(0, 2000 - (Date.now() - (c.last || 0))));
}
const perIp = new Map();
function ipOf(req) { return String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim(); }

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
const STATIC = new Set(['/index.html', '/favicon.svg', '/og.png']);

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const parts = [];
    req.on('data', d => { size += d.length; if (size > MAX_BODY) { reject(new Error('too big')); req.destroy(); } else parts.push(d); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(parts).toString('utf8') || '{}')); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  try {
    if (req.method === 'GET' && p === '/rt/sse') {
      const ch = url.searchParams.get('ch') || '', id = url.searchParams.get('id') || '';
      if (!CH_RE.test(ch) || !ID_RE.test(id)) { res.writeHead(400); return res.end(); }
      const c = chan(ch);
      const cap = ch === 'lobby' ? MAX_LOBBY : MAX_PER_CH;
      if (c.clients.size >= cap && !c.clients.has(id)) { res.writeHead(429); return res.end(); }
      const ip = ipOf(req), nIp = perIp.get(ip) || 0;
      if (nIp >= MAX_PER_IP) { res.writeHead(429); return res.end(); }
      perIp.set(ip, nIp + 1);
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write('retry: 2000\n\n');
      const old = c.clients.get(id); if (old && old !== res) try { old.end(); } catch (e) {}
      c.clients.set(id, res);
      pushPeers(c);
      const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 15000);
      req.on('close', () => {
        clearInterval(ping);
        const k = (perIp.get(ip) || 1) - 1; if (k > 0) perIp.set(ip, k); else perIp.delete(ip);
        if (c.clients.get(id) === res) { c.clients.delete(id); c.presence.delete(id); pushPeers(c); }
        if (!c.clients.size) { if (c.timer) clearTimeout(c.timer); channels.delete(ch); }
      });
      return;
    }
    if (req.method === 'POST' && p.startsWith('/rt/')) {
      const b = await readBody(req);
      const ch = String(b.ch || ''), id = String(b.id || '');
      if (!CH_RE.test(ch) || !ID_RE.test(id)) { res.writeHead(400); return res.end(); }
      const c = channels.get(ch);
      if (p === '/rt/emit') {
        const topic = String(b.topic || '').slice(0, 32);
        if (c) for (const [cid, r] of c.clients) if (cid !== id) send(r, 'msg', { topic, data: b.data });
      } else if (p === '/rt/presence') {
        if (c && c.clients.has(id)) { c.presence.set(id, b.presence && typeof b.presence === 'object' ? b.presence : null); pushPeers(c); }
      } else if (p === '/rt/leave') {
        if (c && c.clients.has(id)) { try { c.clients.get(id).end(); } catch (e) {} c.clients.delete(id); c.presence.delete(id); pushPeers(c); }
      } else { res.writeHead(404); return res.end(); }
      res.writeHead(204); return res.end();
    }
    if (req.method === 'GET' && p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
    if (req.method === 'GET' || req.method === 'HEAD') {
      const f = p === '/' ? '/index.html' : p;
      if (!STATIC.has(f)) { res.writeHead(302, { Location: '/' }); return res.end(); }
      const file = path.join(ROOT, f);
      fs.readFile(file, (err, buf) => {
        if (err) { res.writeHead(404); return res.end(); }
        res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream', 'Cache-Control': f === '/index.html' ? 'no-cache' : 'public, max-age=86400' });
        res.end(req.method === 'HEAD' ? undefined : buf);
      });
      return;
    }
    res.writeHead(405); res.end();
  } catch (e) {
    if (!res.headersSent) res.writeHead(400);
    res.end();
  }
});
server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log('Bad Landlords on :' + PORT));
