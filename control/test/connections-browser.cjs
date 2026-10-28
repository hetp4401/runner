const assert = require('node:assert/strict');
const http = require('node:http');
const { readFileSync } = require('node:fs');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const pageSource = readFileSync(require('node:path').join(__dirname, '../src/app.html'));
  let calls = 0;
  const status = () => ({ now: Date.now(), projects: ['demo', 'other'].map(name => ({ name, version: 1, enabled: true, replicas: 2, staying: 0, placed: [], regions: [] })),
    runs: [], slots: [], starting: [], regions: [], groups: [], servers: 0, dark: [], alerts: [], events: [], rebalance: { hot: [] } });
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    res.setHeader('cache-control', 'no-store');
    if (url.pathname === '/') { res.setHeader('content-type', 'text/html'); return res.end(pageSource); }
    res.setHeader('content-type', 'application/json');
    if (url.pathname.endsWith('/status')) return res.end(JSON.stringify(status()));
    if (url.pathname.endsWith('/unlock') || url.pathname.endsWith('/connections')) {
      if (req.headers['x-admin-password'] !== 'browser-test') { res.statusCode = 401; return res.end(JSON.stringify({ error: 'wrong password' })); }
      if (url.pathname.endsWith('/unlock')) return res.end('{"ok":true}');
      calls++;
      assert.equal(url.search.includes('browser-test'), false);
      const now = Date.now(), window = url.searchParams.get('window');
      let rows = [{ ip: '198.51.100.42', app: 'demo', replica: 1, machine: 1, first: now - 90_000, last: now - 20_000 },
        { ip: '2001:db8::42', app: 'demo', replica: 2, machine: 2, first: now - 240_000, last: now - 120_000 },
        { ip: '203.0.113.42', app: 'other', replica: 1, machine: 3, first: now - 50_000, last: now - 10_000 }];
      if (window === '1m') rows = rows.filter(row => now - row.last < 60_000);
      if (url.searchParams.get('app')) rows = rows.filter(row => row.app === url.searchParams.get('app'));
      if (url.searchParams.get('replica')) rows = rows.filter(row => String(row.replica) === url.searchParams.get('replica'));
      if (url.searchParams.get('q')) rows = rows.filter(row => row.ip.includes(url.searchParams.get('q')));
      return res.end(JSON.stringify({ now, window, total: rows.length, uniqueIPs: rows.length, rows, collectors: { reporting: 3, online: 3, lastReport: now, dropped: 0 } }));
    }
    res.statusCode = 404; res.end('{}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_EXECUTABLE ? { executablePath: process.env.CHROMIUM_EXECUTABLE } : {}) });
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/#/connections?app=demo`);
    await page.getByRole('button', { name: 'Unlock connections' }).waitFor();
    assert.equal(calls, 0, 'no private reads before unlocking');
    await page.getByRole('button', { name: 'Unlock connections' }).click();
    await page.locator('#pw-in').fill('wrong');
    await page.getByRole('button', { name: 'Continue', exact: true }).click();
    await page.getByText('wrong password', { exact: true }).waitFor();
    await page.locator('#pw-in').fill('browser-test');
    await page.getByRole('button', { name: 'Continue', exact: true }).click();
    await page.getByText('198.51.100.42', { exact: true }).waitFor();
    assert.equal(await page.getByText('2001:db8::42', { exact: true }).count(), 0);
    await page.locator('#co-window').selectOption('1h');
    await page.getByText('2001:db8::42', { exact: true }).waitFor();
    await page.locator('#co-replica').selectOption('2');
    await page.getByText('2001:db8::42', { exact: true }).waitFor();
    await page.waitForFunction(() => !document.querySelector('#co-body').textContent.includes('198.51.100.42'));
    await page.setViewportSize({ width: 375, height: 812 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.equal(await page.getByRole('table', { name: 'Visitor IPs' }).isVisible(), true);
    assert.equal(await page.evaluate(() => Object.values(localStorage).includes('browser-test')), false);
    await page.getByRole('button', { name: 'Lock', exact: true }).click();
    await page.getByRole('button', { name: 'Unlock connections' }).waitFor();
    assert.equal(await page.getByText('2001:db8::42', { exact: true }).count(), 0);
    await page.evaluate(() => { location.hash = '#/connections?app=other'; });
    await page.getByRole('button', { name: 'Unlock connections' }).waitFor();
    assert.deepEqual(errors, []);
    console.log('ok desktop/mobile connections view: admin prompt, wrong-password recovery, live/history/replica filters, lock and no persisted password');
  } finally { await browser.close(); await new Promise(resolve => server.close(resolve)); }
})().catch(error => { console.error(error); process.exitCode = 1; });
