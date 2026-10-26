'use strict';

// [wd-scheduler] 智能调度：账号熔断器 + 临期积分优先选号。
// WorkDaddy 二改新增模块。职责：
//   1) 熔断：某账号出现积分耗尽 / 限流 / 登录过期 / 切换失败时进入冷却，
//      冷却期内不再被选为切换目标；限流与切换失败按次数指数退避。
//   2) 选号：在熔断未打开的账号里，按「最早到期的积分段」优先（临期优先），
//      同到期时间比较剩余额度，剩余额度高的优先。
//   3) 状态落盘：DATA_DIR/smart-scheduler-state.json，重启不丢失。

const fs = require('node:fs');
const path = require('node:path');
const { replaceFileWithRetry } = require('./atomic-file-write');
const { nearestExpiringSegment } = require('./credit-rotation.js');

// 各类失败的初始冷却时长（毫秒）
const COOLDOWNS = {
  insufficient: 30 * 60 * 1000,  // 积分耗尽：冷却 30 分钟（等官方用量结算刷新）
  ratelimit: 5 * 60 * 1000,      // 模型限流：首次 5 分钟，随后指数退避
  expired: 2 * 60 * 60 * 1000,   // 登录身份过期：冷却 2 小时（等用户重新登录）
  network: 60 * 1000,            // 网络/未知错误：冷却 1 分钟
  switchfail: 10 * 60 * 1000,    // 切换失败：首次 10 分钟，随后指数退避
};
const MAX_BACKOFF_MS = 2 * 60 * 60 * 1000;
const MAX_FAIL_HISTORY = 20;

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeType(type) {
  return Object.prototype.hasOwnProperty.call(COOLDOWNS, type) ? type : 'network';
}

function createSmartScheduler(dataDir) {
  const file = path.join(dataDir, 'smart-scheduler-state.json');
  let entries = new Map();
  try {
    const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
    for (const [uid, value] of Object.entries(raw && typeof raw === 'object' ? raw : {})) {
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(uid) || !value || typeof value !== 'object') continue;
      entries.set(uid, {
        failCount: finiteNumber(value.failCount) || 0,
        openUntil: finiteNumber(value.openUntil) || 0,
        lastErrorType: typeof value.lastErrorType === 'string' ? value.lastErrorType : '',
        lastErrorAt: finiteNumber(value.lastErrorAt) || 0,
        history: Array.isArray(value.history) ? value.history.slice(-MAX_FAIL_HISTORY) : [],
      });
    }
  } catch (_) { /* 缺失或损坏的调度状态可以安全丢弃 */ }

  function save() {
    try {
      fs.mkdirSync(dataDir, { recursive: true });
      replaceFileWithRetry(file, JSON.stringify(Object.fromEntries(entries)), 0o600);
    } catch (_) { /* 状态写入失败不阻塞主流程 */ }
  }

  function entry(uid) {
    if (!entries.has(uid)) {
      entries.set(uid, { failCount: 0, openUntil: 0, lastErrorType: '', lastErrorAt: 0, history: [] });
    }
    return entries.get(uid);
  }

  // ok=true 清空熔断；ok=false 按类型进入冷却（ratelimit/switchfail 指数退避）
  function report(uid, type, ok) {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(String(uid || ''))) return null;
    const record = entry(String(uid));
    if (ok) {
      entries.delete(String(uid));
      save();
      return { uid: String(uid), open: false, reset: true };
    }
    const kind = normalizeType(type);
    const now = Date.now();
    record.failCount += 1;
    record.lastErrorType = kind;
    record.lastErrorAt = now;
    record.history.push({ at: now, type: kind });
    if (record.history.length > MAX_FAIL_HISTORY) record.history = record.history.slice(-MAX_FAIL_HISTORY);
    let cooldown = COOLDOWNS[kind];
    if (kind === 'ratelimit' || kind === 'switchfail') {
      const backoff = cooldown * Math.pow(2, Math.min(record.failCount - 1, 5));
      cooldown = Math.min(backoff, MAX_BACKOFF_MS);
    }
    record.openUntil = Math.max(record.openUntil, now + cooldown);
    save();
    return {
      uid: String(uid),
      open: true,
      openUntil: record.openUntil,
      failCount: record.failCount,
      type: kind,
      cooldownMs: cooldown,
    };
  }

  function snapshot(uid, now) {
    const record = entries.get(String(uid));
    if (!record) return null;
    const open = Number(record.openUntil) > now;
    return {
      uid: String(uid),
      open,
      openUntil: open ? Number(record.openUntil) : 0,
      remainingMs: open ? Math.max(0, Number(record.openUntil) - now) : 0,
      failCount: Number(record.failCount) || 0,
      lastErrorType: record.lastErrorType || '',
      lastErrorAt: Number(record.lastErrorAt) || 0,
    };
  }

  function breakerList(now = Date.now()) {
    const current = Number.isFinite(Number(now)) ? Number(now) : Date.now();
    return Array.from(entries.keys())
      .map((uid) => snapshot(uid, current))
      .filter(Boolean);
  }

  function isOpen(uid, now = Date.now()) {
    const record = entries.get(String(uid));
    return !!record && Number(record.openUntil) > now;
  }

  // 在候选账号中选号（临期优先）。accounts 元素形如 { uid, nickname, creditSegments|segments }。
  // force=true 时忽略「当前账号临期并不晚于候选」的保守检查（用于积分耗尽/限流后的强制切换）。
  // 返回 { account, segment } 或 null。
  function pickCandidate(accounts, currentUid, options) {
    const opts = options && typeof options === 'object' ? options : {};
    const force = opts.force === true;
    const minRemaining = Math.max(0, finiteNumber(opts.minRemaining) || 0);
    const now = Number.isFinite(Number(opts.now)) ? Number(opts.now) : Date.now();
    const list = Array.isArray(accounts) ? accounts : [];
    const currentAccount = list.find((account) => account && String(account.uid) === String(currentUid || ''));
    const currentSegment = currentAccount && nearestExpiringSegment(currentAccount.creditSegments || currentAccount.segments, now);
    const candidates = list
      .filter((account) => account && String(account.uid || '') && String(account.uid) !== String(currentUid || ''))
      .filter((account) => !isOpen(account.uid, now))
      .map((account) => ({
        account,
        segment: nearestExpiringSegment(account.creditSegments || account.segments, now),
        totalRemaining: (Array.isArray(account.creditSegments || account.segments) ? (account.creditSegments || account.segments) : [])
          .reduce((sum, segment) => sum + (finiteNumber(segment && segment.remaining) || 0), 0),
      }))
      .filter((item) => item.segment && item.segment.remaining > minRemaining)
      .sort((a, b) => {
        const ae = a.segment.expiresAt === null ? Number.MAX_SAFE_INTEGER : a.segment.expiresAt;
        const be = b.segment.expiresAt === null ? Number.MAX_SAFE_INTEGER : b.segment.expiresAt;
        if (ae !== be) return ae - be; // 临期优先：最早到期在前，无到期时间排最后
        if (b.totalRemaining !== a.totalRemaining) return b.totalRemaining - a.totalRemaining;
        return b.segment.remaining - a.segment.remaining;
      });
    const best = candidates[0];
    if (!best) return null;
    // 非强制模式下保持原版保守策略：当前账号临期不晚于候选时不切换，避免无意义打扰
    if (!force && currentSegment && currentSegment.expiresAt !== null &&
        (best.segment.expiresAt === null || best.segment.expiresAt >= currentSegment.expiresAt)) {
      return null;
    }
    return best;
  }

  function reset() {
    entries.clear();
    save();
  }

  return { report, breakerList, isOpen, pickCandidate, reset, file };
}

module.exports = { createSmartScheduler, COOLDOWNS };
