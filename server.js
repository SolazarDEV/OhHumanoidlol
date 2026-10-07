
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({ limit: '4mb' }));

const PORT = process.env.PORT || 3000;
const API_TOKEN = process.env.RELAY_TOKEN || 'CHANGE_ME_TOKEN';

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/roblox' });

const clients = new Map();
const pending = new Map();
let reqCounter = 0;

function authed(req) {
    const h = req.headers['authorization'] || '';
    return h === `Bearer ${API_TOKEN}`;
}

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://x');
    const jobId = url.searchParams.get('jobId') || ('job_' + Date.now());
    const placeId = url.searchParams.get('placeId') || 'unknown';
    const player = url.searchParams.get('player') || 'unknown';
    const entry = { jobId, placeId, player, connectedAt: Date.now(), ws };
    clients.set(jobId, entry);
    console.log(`[roblox+] ${player}@${placeId} job=${jobId} total=${clients.size}`);

    ws.on('message', (raw) => {
        let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (msg.type === 'result' && pending.has(msg.requestId)) {
            const p = pending.get(msg.requestId);
            clearTimeout(p.timeout);
            p.resolve(msg.payload);
            pending.delete(msg.requestId);
        }
    });
    ws.on('close', () => { clients.delete(jobId); console.log(`[roblox-] job=${jobId}`); });
    ws.on('error', () => {});
});

function pickClient(preferredJobId) {
    if (preferredJobId && clients.has(preferredJobId)) return clients.get(preferredJobId);
    const list = [...clients.values()];
    if (!list.length) return null;
    return list.sort((a, b) => b.connectedAt - a.connectedAt)[0];
}

function execOnClient(client, code, timeoutMs = 20000) {
    return new Promise((resolve, reject) => {
        if (!client || client.ws.readyState !== client.ws.OPEN) return reject(new Error('cliente offline'));
        const requestId = `r${++reqCounter}`;
        const timeout = setTimeout(() => { pending.delete(requestId); reject(new Error('timeout')); }, timeoutMs);
        pending.set(requestId, { resolve, timeout });
        client.ws.send(JSON.stringify({ type: 'exec', requestId, payload: { code } }));
    });
}

// escapa string pra entrar em Lua com aspas duplas
function luaStr(s) {
    return '"' + String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n') + '"';
}

// monta a linha require(...) baseada no modo
function buildRequire(assetId, mode, arg, owners) {
    const id = String(assetId).replace(/[^0-9]/g, '');
    if (!id) throw new Error('assetId invalido');

    switch (mode) {
        case 'call':      // require(id)("user")
            return `require(${id})(${luaStr(arg)})`;
        case 'method':    // require(id).NOME("user")
            return `require(${id}).${arg}("user")`;
        case 'methodArg': // require(id).METODO("valor")
            return `require(${id}).${arg.method}(${luaStr(arg.value)})`;
        case 'table':     // require(id){Owners={...}, Prefix=";"}
            return `require(${id}){Owners = { ${owners.map(o => luaStr(o)).join(', ')} }, Prefix = ${luaStr(arg || ';')}}`;
        case 'raw':       // código cru (fallback)
            return String(assetId);
        default:
            throw new Error('mode desconhecido: ' + mode);
    }
}

app.post('/api/require', async (req, res) => {
    if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
    const { assetId, mode, arg, owners, jobId } = req.body || {};
    if (!assetId) return res.status(400).json({ error: 'assetId obrigatorio' });

    let code;
    try {
        code = buildRequire(assetId, mode || 'call', arg, owners || []);
    } catch (e) {
        return res.status(400).json({ error: e.message });
    }

    const client = pickClient(jobId);
    if (!client) return res.status(503).json({ error: 'nenhum roblox conectado' });

    try {
        const result = await execOnClient(client, code);
        res.json({ ok: true, code, job: client.jobId, place: client.placeId, player: client.player, result });
    } catch (e) {
        res.status(502).json({ ok: false, error: e.message, code });
    }
});

app.post('/api/exec', async (req, res) => {
    if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
    const { code, jobId } = req.body || {};
    if (!code) return res.status(400).json({ error: 'code obrigatorio' });
    const client = pickClient(jobId);
    if (!client) return res.status(503).json({ error: 'nenhum roblox conectado' });
    try {
        const result = await execOnClient(client, code);
        res.json({ ok: true, job: client.jobId, result });
    } catch (e) {
        res.status(502).json({ ok: false, error: e.message });
    }
});

app.get('/api/clients', (req, res) => {
    if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
    const list = [...clients.values()].map(c => ({
        jobId: c.jobId, placeId: c.placeId, player: c.player,
        uptime: Date.now() - c.connectedAt
    }));
    res.json({ count: list.length, clients: list });
});

app.get('/health', (req, res) => res.json({ ok: true, clients: clients.size }));

server.listen(PORT, () => console.log(`[relay] :${PORT}`));
