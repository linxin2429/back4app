const express = require('express');
const morgan = require('morgan');
const { Readable } = require('stream');
const { randomUUID } = require('crypto');

const app = express();

const PORT = Number(process.env.PORT || 3000);
const UPSTREAM_AUTH_BEARER = process.env.UPSTREAM_AUTH_BEARER || '';
const ALLOWED_PREFIXES = new Set(
  String(process.env.ALLOWED_PREFIXES || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean)
);

const PROXY_CONFIG = {
  '/openai': 'https://api.openai.com',
  '/claude': 'https://api.anthropic.com',
  '/gemini': 'https://generativelanguage.googleapis.com',
  '/groq': 'https://api.groq.com/openai',
  '/xai': 'https://api.x.ai',
  '/cohere': 'https://api.cohere.ai',
  '/together': 'https://api.together.xyz',
  '/novita': 'https://api.novita.ai',
  '/portkey': 'https://api.portkey.ai',
  '/fireworks': 'https://api.fireworks.ai',
  '/openrouter': 'https://openrouter.ai/api',
  '/minimax': 'https://api.minimaxi.com',
  '/minimax_a': 'https://api.minimaxi.com/anthropic',
  '/kimi': 'https://api.kimi.com/coding/v1',
  '/kimi_a': 'https://api.kimi.com/coding',
  '/aiping': 'https://aiping.cn/api',
  '/claudecn': 'https://claudecn.top',
  '/coyes': 'https://co.yes.vg',
  '/right': 'https://right.codes',
  '/timicc': 'https://timicc.com',
  '/sub2api': 'http://67.216.198.55:8080'
};

app.disable('x-powered-by');
app.use(morgan('tiny'));
app.use(express.raw({ type: '*/*', limit: '20mb' }));

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept, Origin, Range'
  };
}

function isHopByHop(name) {
  const n = name.toLowerCase();
  return n === 'connection' ||
    n === 'keep-alive' ||
    n === 'proxy-connection' ||
    n === 'te' ||
    n === 'trailer' ||
    n === 'transfer-encoding' ||
    n === 'upgrade' ||
    n === 'host' ||
    n === 'content-length';
}

function matchPrefix(pathname) {
  const prefixes = Object.keys(PROXY_CONFIG).sort((a, b) => b.length - a.length);
  for (const prefix of prefixes) {
    if (pathname === prefix || pathname.startsWith(`${prefix}/`)) {
      return prefix;
    }
  }
  return null;
}

function isAllowedPrefix(prefix) {
  if (ALLOWED_PREFIXES.size === 0) return true;
  return ALLOWED_PREFIXES.has(prefix);
}

function shouldStream(req, bodyBuffer) {
  const accept = String(req.headers.accept || '').toLowerCase();
  if (accept.includes('text/event-stream')) return true;

  const contentType = String(req.headers['content-type'] || '').toLowerCase();
  if (!contentType.includes('application/json')) return false;
  if (!bodyBuffer || bodyBuffer.length === 0) return false;

  try {
    const payload = JSON.parse(bodyBuffer.toString('utf8'));
    return payload && payload.stream === true;
  } catch (_) {
    return false;
  }
}

function writeCors(res) {
  const headers = corsHeaders();
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
}

app.options('*', (req, res) => {
  writeCors(res);
  res.status(204).end();
});

app.all('*', async (req, res) => {
  const requestId = randomUUID().slice(0, 8);
  const startedAt = Date.now();
  const log = (message) => {
    console.log(`[b4a-proxy][${requestId}] +${Date.now() - startedAt}ms ${message}`);
  };

  const pathname = req.path;
  const matchedPrefix = matchPrefix(pathname);
  if (!matchedPrefix) {
    writeCors(res);
    return res.status(404).json({ error: 'No proxy rule matched', path: pathname });
  }
  if (!isAllowedPrefix(matchedPrefix)) {
    writeCors(res);
    return res.status(403).json({ error: 'Prefix is not allowed', prefix: matchedPrefix });
  }

  const targetBase = PROXY_CONFIG[matchedPrefix];
  const restPath = pathname.slice(matchedPrefix.length);
  const query = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  const targetUrl = new URL(targetBase.replace(/\/$/, '') + restPath + query);

  const bodyBuffer = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
  const streamMode = shouldStream(req, bodyBuffer);
  log(`incoming method=${req.method} path=${pathname} stream=${streamMode} bodyBytes=${bodyBuffer.length} target=${targetUrl.toString()}`);

  const upstreamHeaders = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === 'undefined') continue;
    if (isHopByHop(k)) continue;
    if (Array.isArray(v)) {
      upstreamHeaders.set(k, v.join(', '));
    } else {
      upstreamHeaders.set(k, v);
    }
  }

  if (!upstreamHeaders.has('authorization') && UPSTREAM_AUTH_BEARER) {
    upstreamHeaders.set('authorization', `Bearer ${UPSTREAM_AUTH_BEARER}`);
  }

  upstreamHeaders.set('x-forwarded-for', req.ip || '');
  upstreamHeaders.set('x-forwarded-host', req.headers.host || '');
  upstreamHeaders.set('x-forwarded-proto', req.protocol || 'https');

  const upstreamInit = {
    method: req.method,
    headers: upstreamHeaders,
    redirect: 'manual',
    body: (req.method === 'GET' || req.method === 'HEAD') ? undefined : bodyBuffer
  };

  const abortController = new AbortController();
  req.on('close', () => {
    if (!res.writableEnded) {
      abortController.abort();
      log('downstream closed, abort upstream');
    }
  });

  try {
    if (streamMode) {
      writeCors(res);
      res.status(200);
      res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('Pragma', 'no-cache');
      if (typeof res.flushHeaders === 'function') {
        res.flushHeaders();
      }
      log('sse response started');

      let heartbeatCount = 0;
      const heartbeatTimer = setInterval(() => {
        if (!res.writableEnded) {
          heartbeatCount += 1;
          res.write(': keep-alive\n\n');
        }
      }, 15000);

      const upstreamResponse = await fetch(targetUrl, { ...upstreamInit, signal: abortController.signal });
      log(`upstream headers status=${upstreamResponse.status} contentType=${upstreamResponse.headers.get('content-type') || ''}`);

      if (!upstreamResponse.ok) {
        const txt = await upstreamResponse.text().catch(() => 'upstream error');
        clearInterval(heartbeatTimer);
        res.write(`event: error\ndata: ${JSON.stringify({ source: 'upstream', status: upstreamResponse.status, message: txt.slice(0, 800) })}\n\n`);
        return res.end();
      }

      if (!upstreamResponse.body) {
        clearInterval(heartbeatTimer);
        res.write('event: error\ndata: {"source":"proxy","message":"empty upstream body"}\n\n');
        return res.end();
      }

      const nodeReadable = Readable.fromWeb(upstreamResponse.body);
      let chunks = 0;
      let bytes = 0;

      nodeReadable.on('data', (chunk) => {
        chunks += 1;
        bytes += chunk.length;
        if (!res.writableEnded) {
          const ok = res.write(chunk);
          if (!ok) {
            nodeReadable.pause();
            res.once('drain', () => nodeReadable.resume());
          }
        }
      });

      nodeReadable.on('end', () => {
        clearInterval(heartbeatTimer);
        log(`stream completed heartbeat=${heartbeatCount} chunks=${chunks} bytes=${bytes}`);
        if (!res.writableEnded) res.end();
      });

      nodeReadable.on('error', (err) => {
        clearInterval(heartbeatTimer);
        log(`stream forward failed error=${err}`);
        if (!res.writableEnded) {
          res.write(`event: error\ndata: ${JSON.stringify({ source: 'proxy', message: 'stream forward failed' })}\n\n`);
          res.end();
        }
      });

      return;
    }

    const upstreamResponse = await fetch(targetUrl, { ...upstreamInit, signal: abortController.signal });
    writeCors(res);

    res.status(upstreamResponse.status);
    upstreamResponse.headers.forEach((value, key) => {
      if (isHopByHop(key)) return;
      if (key.toLowerCase() === 'content-length') return;
      res.setHeader(key, value);
    });

    if (upstreamResponse.body) {
      const nodeReadable = Readable.fromWeb(upstreamResponse.body);
      nodeReadable.pipe(res);
    } else {
      res.end();
    }
  } catch (error) {
    log(`proxy failed error=${error}`);
    if (!res.headersSent) {
      writeCors(res);
      res.status(502).json({ error: 'Proxy request failed' });
      return;
    }
    if (!res.writableEnded) {
      res.end();
    }
  }
});

app.listen(PORT, () => {
  console.log(`[b4a-proxy] listening on 0.0.0.0:${PORT}`);
});
