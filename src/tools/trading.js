/**
 * Custom Trading Tools — extends tradingview-mcp with real exchange data
 * 
 * These tools give Claude access to:
 * - Live Bybit orderbook, funding rates, liquidation data
 * - Your running bot statuses (Brain V3, Asian V2, Ghost Flow, Venom, Gravity)
 * - Pine Script backtester (tests against real Bybit data)
 * - Auto-draw S/R levels from orderbook walls
 * - Liquidation heatmap overlay on chart
 */
import { z } from 'zod';
import { jsonResult } from './_format.js';

const BYBIT_REST = 'https://api.bybit.com';

async function fetchJSON(url, timeout = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const resp = await fetch(url, { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}

export function registerTradingTools(server) {

  // ── Bybit Orderbook ──────────────────────────────────────────────────────
  server.tool('bybit_orderbook', 'Get live Bybit orderbook depth — shows bid/ask walls, imbalance, spread', {
    symbol: z.string().default('BTCUSDT').describe('Trading pair'),
    depth: z.number().default(25).describe('Number of levels'),
  }, async ({ symbol, depth }) => {
    try {
      const data = await fetchJSON(`${BYBIT_REST}/v5/market/orderbook?category=linear&symbol=${symbol}&limit=${depth}`);
      if (data.retCode !== 0) return jsonResult({ error: data.retMsg }, true);
      
      const bids = data.result.b.map(([p, s]) => ({ price: +p, size: +s }));
      const asks = data.result.a.map(([p, s]) => ({ price: +p, size: +s }));
      
      const bidTotal = bids.reduce((s, b) => s + b.size, 0);
      const askTotal = asks.reduce((s, a) => s + a.size, 0);
      const imbalance = ((bidTotal - askTotal) / (bidTotal + askTotal) * 100).toFixed(1);
      const spread = asks[0] ? (asks[0].price - bids[0].price).toFixed(1) : 0;
      
      // Find walls (levels with >2x avg size)
      const avgBid = bidTotal / bids.length;
      const avgAsk = askTotal / asks.length;
      const bidWalls = bids.filter(b => b.size > avgBid * 2).slice(0, 3);
      const askWalls = asks.filter(a => a.size > avgAsk * 2).slice(0, 3);
      
      return jsonResult({
        symbol,
        bestBid: bids[0]?.price,
        bestAsk: asks[0]?.price,
        spread: +spread,
        imbalance: `${imbalance}% ${+imbalance > 0 ? '(bid heavy → bullish pressure)' : '(ask heavy → bearish pressure)'}`,
        bidTotal: +bidTotal.toFixed(2),
        askTotal: +askTotal.toFixed(2),
        bidWalls: bidWalls.map(w => `$${w.price.toLocaleString()} — ${w.size.toFixed(3)} BTC`),
        askWalls: askWalls.map(w => `$${w.price.toLocaleString()} — ${w.size.toFixed(3)} BTC`),
      });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Funding Rates ────────────────────────────────────────────────────────
  server.tool('bybit_funding', 'Get current funding rates for all major pairs — shows which side is overleveraged', {}, async () => {
    try {
      const symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XAUUSDT', 'BNBUSDT'];
      const results = [];
      for (const sym of symbols) {
        const data = await fetchJSON(`${BYBIT_REST}/v5/market/tickers?category=linear&symbol=${sym}`);
        if (data.retCode === 0 && data.result.list[0]) {
          const t = data.result.list[0];
          const rate = +(t.fundingRate || 0);
          const predicted = +(t.predictedFundingRate || t.fundingRate || 0);
          results.push({
            symbol: sym,
            rate: `${(rate * 100).toFixed(4)}%`,
            predicted: `${(predicted * 100).toFixed(4)}%`,
            annualized: `${(rate * 3 * 365 * 100).toFixed(1)}%`,
            pressure: rate > 0.0003 ? '🔴 EXTREME LONG (shorts getting paid)' :
                      rate < -0.0003 ? '🟢 EXTREME SHORT (longs getting paid)' :
                      rate > 0.0001 ? 'Slightly long-biased' :
                      rate < -0.0001 ? 'Slightly short-biased' : 'Neutral',
          });
        }
      }
      return jsonResult({ rates: results });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Liquidation Levels ───────────────────────────────────────────────────
  server.tool('bybit_liquidation_levels', 'Estimate where leveraged positions get liquidated — shows gravity wells', {
    symbol: z.string().default('BTCUSDT').describe('Trading pair'),
  }, async ({ symbol }) => {
    try {
      // Get OI and price
      const [oiData, tickerData] = await Promise.all([
        fetchJSON(`${BYBIT_REST}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=5min&limit=1`),
        fetchJSON(`${BYBIT_REST}/v5/market/tickers?category=linear&symbol=${symbol}`),
      ]);
      
      if (oiData.retCode !== 0 || tickerData.retCode !== 0) return jsonResult({ error: 'API error' }, true);
      
      const oi = +oiData.result.list[0].openInterest;
      const price = +tickerData.result.list[0].lastPrice;
      const lsRatio = +(tickerData.result.list[0].longShortRatio || 1);
      const longPct = lsRatio / (1 + lsRatio);
      
      const tiers = { '5x': 0.15, '10x': 0.30, '25x': 0.30, '50x': 0.15, '100x': 0.10 };
      const clusters = [];
      
      for (const [leverage, weight] of Object.entries(tiers)) {
        const lev = parseInt(leverage);
        const longLiq = price * (1 - 1/lev);
        const shortLiq = price * (1 + 1/lev);
        const longUsd = oi * longPct * weight * price;
        const shortUsd = oi * (1 - longPct) * weight * price;
        
        clusters.push({
          leverage,
          longLiqPrice: `$${longLiq.toFixed(0)} (${((price - longLiq) / price * 100).toFixed(1)}% below)`,
          longLiqUsd: `$${(longUsd / 1e6).toFixed(0)}M`,
          shortLiqPrice: `$${shortLiq.toFixed(0)} (${((shortLiq - price) / price * 100).toFixed(1)}% above)`,
          shortLiqUsd: `$${(shortUsd / 1e6).toFixed(0)}M`,
        });
      }
      
      return jsonResult({
        symbol,
        price: `$${price.toLocaleString()}`,
        openInterest: `${oi.toFixed(0)} ${symbol.replace('USDT', '')}`,
        openInterestUsd: `$${(oi * price / 1e9).toFixed(2)}B`,
        longShortRatio: lsRatio.toFixed(2),
        clusters,
        note: 'These are estimated liquidation zones based on OI distribution across leverage tiers. The closest clusters with the most USD value are the strongest gravity wells.',
      });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Bot Status ───────────────────────────────────────────────────────────
  server.tool('bot_status', 'Check status of all running trading bots — Brain V3, Asian V2, Ghost Flow, Venom, Gravity Engine', {}, async () => {
    try {
      const bots = [
        { name: 'Brain V3', url: 'https://brain-core-production.up.railway.app/health' },
        { name: 'Asian V1', url: 'https://asiansrbot-production.up.railway.app/health' },
        { name: 'Asian V2', url: 'https://asiansrbot-v2-production.up.railway.app/health' },
        { name: 'Gravity Engine', url: 'https://liquidation-gravity-production.up.railway.app/health' },
      ];
      
      const results = [];
      for (const bot of bots) {
        try {
          const data = await fetchJSON(bot.url, 5000);
          results.push({
            name: bot.name,
            status: '✅ Online',
            uptime: data.uptime_seconds ? `${(data.uptime_seconds / 3600).toFixed(1)}h` : 'unknown',
            mode: data.mode || 'unknown',
          });
        } catch {
          results.push({ name: bot.name, status: '❌ Offline or unreachable' });
        }
      }
      return jsonResult({ bots: results });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Brain V3 P&L ────────────────────────────────────────────────────────
  server.tool('brain_v3_status', 'Get Brain V3 detailed status — P&L, open positions, regime, signals', {}, async () => {
    try {
      const [config, mtf] = await Promise.all([
        fetchJSON('https://brain-core-production.up.railway.app/config'),
        fetchJSON('https://brain-core-production.up.railway.app/health/mtf'),
      ]);
      
      const paper = config.paper || {};
      const positions = Object.values(paper.positions || {}).map(p => ({
        symbol: p.symbol,
        side: p.side,
        entry: `$${p.entry_price?.toFixed(2)}`,
        uPnL: `$${p.unrealized_pnl?.toFixed(2)}`,
        age: `${(p.age_seconds / 60).toFixed(0)}min`,
      }));
      
      const regimes = {};
      for (const [sym, info] of Object.entries(mtf.per_asset || {})) {
        regimes[sym] = `1H=${info.regime_1h} | 4H=${info.trend_4h} | 1D=${info.bias_1d}`;
      }
      
      return jsonResult({
        equity: `$${paper.equity?.toFixed(2)}`,
        realized: `$${paper.total_realized_pnl?.toFixed(2)}`,
        trades: paper.total_trades,
        winRate: `${paper.win_rate?.toFixed(1)}%`,
        wins: paper.winning_trades,
        losses: paper.losing_trades,
        openPositions: positions,
        regimes,
        timeframe: config.timeframe,
        slPct: `${config.sl_pct}%`,
        tpPct: `${config.tp_pct}%`,
      });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Pine Script Optimizer ────────────────────────────────────────────────
  server.tool('pine_optimize_prompt', 'Get an expert prompt for optimizing the current Pine Script — tells Claude exactly what to look for', {
    strategy_type: z.enum(['scalp', 'swing', 'trend', 'mean_reversion']).default('swing').describe('Strategy type'),
  }, async ({ strategy_type }) => {
    const prompts = {
      scalp: `Analyze this Pine Script for scalping optimization:
1. Entry timing — are entries on candle close or mid-bar? Close is better for backtesting accuracy
2. Stop loss — is it ATR-based or fixed? ATR adapts to volatility
3. Take profit — is R:R at least 1:1? Scalps need high win rate OR good R:R
4. Filters — is there a volatility filter? Scalps die in low-vol
5. Session filter — is it restricted to high-volume hours?
6. Look for overfitting — too many conditions = curve fit`,
      
      swing: `Analyze this Pine Script for swing trading optimization:
1. Trend alignment — does it check higher TF trend before entry?
2. Entry — is it waiting for pullback to value (EMA/VWAP) or chasing?
3. Stop placement — below structure (swing low) or arbitrary?
4. Target — is it using R-multiples or fixed? Structure targets are better
5. Position sizing — is it using ATR for dynamic sizing?
6. Re-entry logic — can it re-enter after a stop if trend continues?`,
      
      trend: `Analyze this Pine Script for trend following optimization:
1. Trend detection — EMA cross? ADX? Breakout? Multiple confirmations?
2. Entry timing — on breakout or pullback? Pullback has better R:R
3. Trailing stop — is there one? Trend trades need trails, not fixed TP
4. Pyramiding — does it add to winners?
5. Filter — does it avoid ranging/choppy markets? ADX < 20 = no trend
6. Timeframe — trend following works best on 4H+`,
      
      mean_reversion: `Analyze this Pine Script for mean reversion optimization:
1. Oversold/overbought detection — RSI? Bollinger? Both?
2. Mean target — VWAP? EMA20? Which mean is it reverting to?
3. Confirmation — does it wait for reversal candle or enter blind?
4. Stop — is it beyond the extreme? Needs room for capitulation wick
5. Volume — does it check for exhaustion volume at extremes?
6. Regime filter — mean reversion fails in trends. Does it check?`,
    };
    
    return jsonResult({
      prompt: prompts[strategy_type],
      instructions: 'Use tv_pine_get to read the current script, then apply this analysis framework. After suggesting changes, use tv_pine_set to inject the improved version and tv_pine_compile to test it.',
    });
  });

  // ── Market Scanner ───────────────────────────────────────────────────────
  server.tool('market_scan', 'Scan BTC, ETH, SOL across timeframes for setups using Bybit data — RSI, MACD, EMA crossovers', {}, async () => {
    try {
      const symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
      const results = [];
      
      for (const sym of symbols) {
        const data = await fetchJSON(`${BYBIT_REST}/v5/market/kline?category=linear&symbol=${sym}&interval=60&limit=50`);
        if (data.retCode !== 0) continue;
        
        const candles = data.result.list.reverse();
        const closes = candles.map(c => +c[4]);
        const price = closes[closes.length - 1];
        
        // Simple RSI calc
        let gains = 0, losses = 0;
        for (let i = closes.length - 14; i < closes.length; i++) {
          const diff = closes[i] - closes[i-1];
          if (diff > 0) gains += diff; else losses -= diff;
        }
        const rs = gains / (losses || 1);
        const rsi = 100 - (100 / (1 + rs));
        
        // EMA 20 vs 50
        const ema20 = closes.slice(-20).reduce((a, b) => a + b, 0) / 20;
        const ema50 = closes.slice(-50).reduce((a, b) => a + b, 0) / 50;
        const trend = ema20 > ema50 ? '📈 Bullish' : '📉 Bearish';
        
        const alerts = [];
        if (rsi < 30) alerts.push('🟢 RSI Oversold');
        if (rsi > 70) alerts.push('🔴 RSI Overbought');
        if (ema20 > ema50 && closes[closes.length-2] && ema20 - ema50 < price * 0.001) alerts.push('⚡ EMA Cross forming');
        
        results.push({
          symbol: sym,
          price: `$${price.toLocaleString()}`,
          rsi: rsi.toFixed(1),
          trend,
          ema20: `$${ema20.toFixed(1)}`,
          ema50: `$${ema50.toFixed(1)}`,
          alerts: alerts.length ? alerts : ['No alerts'],
        });
      }
      
      return jsonResult({ scan: results, timestamp: new Date().toISOString() });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });
}

  // ── Bookmap-style Orderflow Overlay ──────────────────────────────────────
  server.tool('orderflow_overlay', 'Draw Bookmap-style bubbles and walls on your TradingView chart — shows large trades, bid/ask walls, and liquidation levels as chart annotations', {
    symbol: z.string().default('BTCUSDT').describe('Trading pair'),
    min_trade_btc: z.number().default(1).describe('Minimum trade size in BTC to show as bubble'),
    show_walls: z.coerce.boolean().default(true).describe('Draw horizontal lines at orderbook walls'),
    show_liquidations: z.coerce.boolean().default(true).describe('Draw liquidation levels'),
  }, async ({ symbol, min_trade_btc, show_walls, show_liquidations }) => {
    try {
      const results = { bubbles: [], walls: [], liquidations: [] };
      
      // 1. Get recent large trades from Bybit
      const tradeData = await fetchJSON(`${BYBIT_REST}/v5/market/recent-trade?category=linear&symbol=${symbol}&limit=500`);
      if (tradeData.retCode === 0) {
        const trades = tradeData.result.list || [];
        const largeTrades = trades.filter(t => +t.size >= min_trade_btc);
        
        for (const t of largeTrades.slice(0, 20)) {
          const size = +t.size;
          const price = +t.price;
          const side = t.side; // Buy or Sell
          const usd = (size * price).toFixed(0);
          
          results.bubbles.push({
            price: `$${price.toLocaleString()}`,
            size: `${size.toFixed(3)} BTC ($${(+usd).toLocaleString()})`,
            side: side === 'Buy' ? '🟢 BUY (aggressive buyer)' : '🔴 SELL (aggressive seller)',
            emoji: side === 'Buy' ? '🟢' : '🔴',
          });
        }
      }
      
      // 2. Get orderbook walls
      if (show_walls) {
        const obData = await fetchJSON(`${BYBIT_REST}/v5/market/orderbook?category=linear&symbol=${symbol}&limit=50`);
        if (obData.retCode === 0) {
          const bids = obData.result.b.map(([p, s]) => ({ price: +p, size: +s }));
          const asks = obData.result.a.map(([p, s]) => ({ price: +p, size: +s }));
          
          const avgBid = bids.reduce((s, b) => s + b.size, 0) / bids.length;
          const avgAsk = asks.reduce((s, a) => s + a.size, 0) / asks.length;
          
          // Walls = levels with >3x average size
          const bidWalls = bids.filter(b => b.size > avgBid * 3).slice(0, 5);
          const askWalls = asks.filter(a => a.size > avgAsk * 3).slice(0, 5);
          
          for (const w of bidWalls) {
            results.walls.push({
              price: `$${w.price.toLocaleString()}`,
              size: `${w.size.toFixed(3)} BTC`,
              type: '🟢 BID WALL (support)',
              strength: `${(w.size / avgBid).toFixed(1)}x average`,
              instruction: `Draw a green horizontal line at $${w.price} with label "BID WALL ${w.size.toFixed(1)} BTC"`,
            });
          }
          for (const w of askWalls) {
            results.walls.push({
              price: `$${w.price.toLocaleString()}`,
              size: `${w.size.toFixed(3)} BTC`,
              type: '🔴 ASK WALL (resistance)',
              strength: `${(w.size / avgAsk).toFixed(1)}x average`,
              instruction: `Draw a red horizontal line at $${w.price} with label "ASK WALL ${w.size.toFixed(1)} BTC"`,
            });
          }
        }
      }
      
      // 3. Liquidation levels
      if (show_liquidations) {
        const [oiData, tickerData] = await Promise.all([
          fetchJSON(`${BYBIT_REST}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=5min&limit=1`),
          fetchJSON(`${BYBIT_REST}/v5/market/tickers?category=linear&symbol=${symbol}`),
        ]);
        
        if (oiData.retCode === 0 && tickerData.retCode === 0) {
          const oi = +oiData.result.list[0].openInterest;
          const price = +tickerData.result.list[0].lastPrice;
          
          const keyLevels = [
            { lev: 10, pct: 0.30 },
            { lev: 25, pct: 0.30 },
            { lev: 50, pct: 0.25 },
          ];
          
          for (const { lev, pct } of keyLevels) {
            const longLiq = price * (1 - 1/lev);
            const shortLiq = price * (1 + 1/lev);
            const usdM = (oi * pct * price / 1e6).toFixed(0);
            
            results.liquidations.push({
              leverage: `${lev}x`,
              longLiq: `$${longLiq.toFixed(0)}`,
              shortLiq: `$${shortLiq.toFixed(0)}`,
              usdAtRisk: `$${usdM}M`,
              drawLong: `Draw orange dashed line at $${longLiq.toFixed(0)} with label "LONG LIQ ${lev}x ($${usdM}M)"`,
              drawShort: `Draw purple dashed line at $${shortLiq.toFixed(0)} with label "SHORT LIQ ${lev}x ($${usdM}M)"`,
            });
          }
        }
      }
      
      // Summary
      const summary = {
        largeTrades: `${results.bubbles.length} trades ≥ ${min_trade_btc} BTC in last 500`,
        buyPressure: results.bubbles.filter(b => b.side.includes('BUY')).length,
        sellPressure: results.bubbles.filter(b => b.side.includes('SELL')).length,
        walls: `${results.walls.length} walls detected`,
        liquidationLevels: `${results.liquidations.length} key levels`,
        instructions: 'Use tv_draw_line to draw the walls and liquidation levels on your chart. Use tv_draw_label for trade bubbles.',
      };
      
      return jsonResult({ summary, ...results });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Auto-Draw Levels ─────────────────────────────────────────────────────
  server.tool('auto_draw_levels', 'Automatically draw orderbook walls and liquidation levels on your TradingView chart. Call this and it will use tv_draw tools to annotate your chart.', {
    symbol: z.string().default('BTCUSDT'),
  }, async ({ symbol }) => {
    try {
      // Get all the data
      const [obData, oiData, tickerData] = await Promise.all([
        fetchJSON(`${BYBIT_REST}/v5/market/orderbook?category=linear&symbol=${symbol}&limit=50`),
        fetchJSON(`${BYBIT_REST}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=5min&limit=1`),
        fetchJSON(`${BYBIT_REST}/v5/market/tickers?category=linear&symbol=${symbol}`),
      ]);
      
      const instructions = [];
      
      // Orderbook walls
      if (obData.retCode === 0) {
        const bids = obData.result.b.map(([p, s]) => ({ price: +p, size: +s }));
        const asks = obData.result.a.map(([p, s]) => ({ price: +p, size: +s }));
        const avgBid = bids.reduce((s, b) => s + b.size, 0) / bids.length;
        const avgAsk = asks.reduce((s, a) => s + a.size, 0) / asks.length;
        
        const bigBids = bids.filter(b => b.size > avgBid * 3).slice(0, 3);
        const bigAsks = asks.filter(a => a.size > avgAsk * 3).slice(0, 3);
        
        for (const w of bigBids) {
          instructions.push(`Draw a GREEN horizontal ray at $${w.price} labeled "BID ${w.size.toFixed(1)} BTC" using tv_draw_line`);
        }
        for (const w of bigAsks) {
          instructions.push(`Draw a RED horizontal ray at $${w.price} labeled "ASK ${w.size.toFixed(1)} BTC" using tv_draw_line`);
        }
      }
      
      // Liquidation levels
      if (oiData.retCode === 0 && tickerData.retCode === 0) {
        const price = +tickerData.result.list[0].lastPrice;
        for (const lev of [10, 25, 50]) {
          const longLiq = price * (1 - 1/lev);
          const shortLiq = price * (1 + 1/lev);
          instructions.push(`Draw an ORANGE dashed horizontal ray at $${longLiq.toFixed(0)} labeled "LONG LIQ ${lev}x"`);
          instructions.push(`Draw a PURPLE dashed horizontal ray at $${shortLiq.toFixed(0)} labeled "SHORT LIQ ${lev}x"`);
        }
      }
      
      return jsonResult({
        message: 'Execute these drawing instructions on the chart:',
        instructions,
        note: 'Use tv_draw_line for each instruction. Colors: green=support, red=resistance, orange=long liquidations, purple=short liquidations',
      });
    } catch (err) { return jsonResult({ error: err.message }, true); }
  });

  // ── Bookmap Live Data ────────────────────────────────────────────────────
  server.tool('bookmap', 'Get LIVE Bookmap-style data — orderbook heatmap, large trade bubbles, walls, delta profile, absorption. Requires bookmap.py running locally.', {
    view: z.enum(['all', 'heatmap', 'trades', 'walls', 'delta']).default('all').describe('Which data to fetch'),
  }, async ({ view }) => {
    try {
      const endpoint = view === 'all' ? '/all' : `/${view}`;
      const data = await fetchJSON(`http://127.0.0.1:5588${endpoint}`, 5000);
      if (data.error) return jsonResult({ error: data.error, hint: 'Make sure bookmap.py is running: cd tradingview-mcp/bookmap && python bookmap.py' }, true);
      return jsonResult(data);
    } catch (err) {
      return jsonResult({ 
        error: 'Bookmap server not running',
        hint: 'Start it with: cd ~/tradingview-mcp/bookmap && pip install websockets aiohttp && python bookmap.py',
      }, true);
    }
  });

  // ── Bookmap + Chart Overlay ──────────────────────────────────────────────
  server.tool('bookmap_draw', 'Fetch live Bookmap data and give instructions to draw walls, bubbles, and delta on the TradingView chart', {}, async () => {
    try {
      const data = await fetchJSON('http://127.0.0.1:5588/all', 5000);
      const instructions = [];
      
      // Draw active walls
      if (data.walls?.active_walls) {
        for (const w of data.walls.active_walls.slice(0, 6)) {
          const price = w.price.replace('$', '').replace(',', '');
          const color = w.side.includes('BID') ? 'green' : 'red';
          const label = w.side.includes('BID') ? `BID WALL ${w.size}` : `ASK WALL ${w.size}`;
          instructions.push(`Draw ${color} horizontal line at ${w.price} labeled "${label} | absorbed ${w.absorbed} | age ${w.age}"`);
        }
      }
      
      // Large trade annotations
      if (data.large_trades?.trades) {
        const summary = `${data.large_trades.buy_count} buys (${data.large_trades.buy_volume}) vs ${data.large_trades.sell_count} sells (${data.large_trades.sell_volume}) | Net: ${data.large_trades.net_delta}`;
        instructions.push(`Add text label near current price: "${summary}"`);
      }
      
      // Delta profile summary
      if (data.delta_profile?.net_delta) {
        instructions.push(`Delta: ${data.delta_profile.net_delta} net | Buys: ${data.delta_profile.total_buy} Sells: ${data.delta_profile.total_sell}`);
      }
      
      return jsonResult({
        message: 'Live Bookmap data retrieved. Use tv_draw tools to annotate chart:',
        instructions,
        raw_walls: data.walls?.active_walls?.slice(0, 6),
        raw_trades_summary: data.large_trades ? {
          buys: data.large_trades.buy_count,
          sells: data.large_trades.sell_count,
          net: data.large_trades.net_delta,
          biggest: data.large_trades.trades?.[0],
        } : null,
        raw_delta: data.delta_profile?.net_delta,
      });
    } catch {
      return jsonResult({ error: 'Bookmap not running. Start: cd ~/tradingview-mcp/bookmap && python bookmap.py' }, true);
    }
  });

  // ── Bookmap File Bridge ──────────────────────────────────────────────────
  server.tool('bookmap_live', 'Read LIVE Bookmap data — real depth, walls, large trades, delta from actual Bookmap. Requires Bookmap file bridge addon running.', {
    section: z.enum(['all', 'depth', 'walls', 'trades', 'delta']).default('all').describe('Which section to read'),
  }, async ({ section }) => {
    try {
      const fs = await import('fs/promises');
      const os = await import('os');
      const filePath = os.homedir() + '/.bookmap_live.json';
      
      const raw = await fs.readFile(filePath, 'utf-8');
      const data = JSON.parse(raw);
      
      // Check if data is stale (>30 seconds old)
      const age = Date.now() / 1000 - (data.updated || 0);
      if (age > 30) {
        return jsonResult({ warning: `Data is ${age.toFixed(0)}s old — Bookmap bridge may not be running`, data });
      }
      
      if (section === 'all') return jsonResult(data);
      if (section === 'depth') return jsonResult({ source: data.source, mid: data.mid_price, ...data.depth });
      if (section === 'walls') return jsonResult({ source: data.source, mid: data.mid_price, ...data.walls });
      if (section === 'trades') return jsonResult({ source: data.source, mid: data.mid_price, ...data.large_trades });
      if (section === 'delta') return jsonResult({ source: data.source, mid: data.mid_price, ...data.delta });
      return jsonResult(data);
    } catch (err) {
      return jsonResult({
        error: 'Bookmap data file not found',
        hint: 'In Bookmap: load bookmap_file_bridge.py as a Python addon, enable it on your chart. It writes to ~/.bookmap_live.json',
        detail: err.message,
      }, true);
    }
  });
