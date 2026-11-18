// Bad Landlords: static page + tiny realtime relay (Server-Sent Events + POST). No dependencies.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const ROOT = __dirname;
const MAX_BODY = 64 * 1024;
const MAX_PER_CH = 40;          // game rooms: 6 players + reconnect slack
const MAX_LOBBY = 5000;         // the lobby is shared by everyone on the site
const MAX_PER_IP = 80;          // open streams per address, against floods
const CH_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const ID_RE = /^[a-f0-9]{20}-[a-z0-9]{3,10}$/;   // <fingerprint>-<tab>
const KEY_RE = /^[A-Za-z0-9_-]{20,64}$/;
// the fingerprint is derived from a secret only the browser knows, so nobody can speak as someone else
function fp(key) { return crypto.createHash('sha256').update(key).digest('hex').slice(0, 20); }
function authed(id, key) { return ID_RE.test(id) && KEY_RE.test(key) && id.slice(0, 20) === fp(key); }
// token buckets: per connection and per address
const buckets = new Map();
function allow(k, rate, cap) {
  const now = Date.now(); let b = buckets.get(k);
  if (!b) { b = { t: cap, at: now }; buckets.set(k, b); }
  b.t = Math.min(cap, b.t + (now - b.at) / 1000 * rate); b.at = now;
  if (b.t < 1) return false; b.t -= 1; return true;
}
setInterval(() => { const old = Date.now() - 120000; for (const [k, b] of buckets) if (b.at < old) buckets.delete(k); }, 60000).unref();

const channels = new Map(); // ch -> { clients: Map(id -> res), presence: Map(id -> obj) }
function chan(ch) {
  let c = channels.get(ch);
  if (!c) { c = { clients: new Map(), presence: new Map() }; channels.set(ch, c); }
  return c;
}
function send(res, event, data) { try { res.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n'); } catch (e) {} }
function peersOf(c) { return [...c.clients.keys()].map(id => ({ id: id.slice(0, 20), presence: c.presence.get(id) || null })); }
function pushNow(c) { c.timer = null; c.last = Date.now(); const l = peersOf(c); for (const res of c.clients.values()) send(res, 'peers', l); }
// big channels (the lobby) coalesce peer updates to at most one every 2s
function pushPeers(c) {
  if (c.clients.size <= 12) return pushNow(c);
  if (c.timer) return;
  c.timer = setTimeout(() => pushNow(c), Math.max(0, 2000 - (Date.now() - (c.last || 0))));
}
const perIp = new Map();
// the proxy appends the real address at the end; anything before it can be forged by the client
function ipOf(req) { const xf = String(req.headers['x-forwarded-for'] || '').split(',').map(x => x.trim()).filter(Boolean); return xf.length ? xf[xf.length - 1] : String(req.socket.remoteAddress || ''); }

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
      const ch = url.searchParams.get('ch') || '', id = url.searchParams.get('id') || '', key = url.searchParams.get('k') || '';
      if (!CH_RE.test(ch) || !authed(id, key)) { res.writeHead(400); return res.end(); }
      if (!allow('c:' + ipOf(req), 2, 30)) { res.writeHead(429); return res.end(); }
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
      const ip = ipOf(req);
      if (!allow('i:' + ip, 300, 900)) { res.writeHead(429); return res.end(); }
      const b = await readBody(req);
      const ch = String(b.ch || ''), id = String(b.id || ''), key = String(b.k || '');
      if (!CH_RE.test(ch) || !authed(id, key)) { res.writeHead(403); return res.end(); }
      const c = channels.get(ch);
      // only someone actually connected to the room can talk in it
      if (!c || !c.clients.has(id)) { res.writeHead(403); return res.end(); }
      if (!allow('p:' + id, 120, 400)) { res.writeHead(429); return res.end(); }
      if (p === '/rt/emit') {
        const topic = String(b.topic || '').slice(0, 32);
        const from = id.slice(0, 20);
        for (const [cid, r] of c.clients) if (cid !== id) send(r, 'msg', { topic, data: b.data, from });
      } else if (p === '/rt/presence') {
        const pr = b.presence && typeof b.presence === 'object' ? b.presence : null;
        if (pr && JSON.stringify(pr).length > 600) { res.writeHead(413); return res.end(); }
        c.presence.set(id, pr); pushPeers(c);
      } else if (p === '/rt/leave') {
        try { c.clients.get(id).end(); } catch (e) {} c.clients.delete(id); c.presence.delete(id); pushPeers(c);
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
        res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream', 'Cache-Control': f === '/index.html' ? 'no-cache' : 'public, max-age=86400',
          'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'strict-origin-when-cross-origin' });
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
