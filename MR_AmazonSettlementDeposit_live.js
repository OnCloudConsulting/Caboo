/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * Amazon settlement file -> NetSuite deposit(s)
 * --------------------------------------------------------------------------
 * - Picks the oldest file from the Pending folder (one file per run).
 * - Parses the Amazon V2 settlement flat file (tab delimited).
 * - Matches every Amazon order ID to FarApp cash sales / cash refunds in NetSuite
 *   (by order ID, NOT by FarApp's settlement tag).
 * - Builds one or more deposits (split by max payment lines) whose combined total
 *   equals the settlement total-amount to the cent. Memo: "AMZ STL <id> (n/k)".
 * - Differences are posted as Other Deposits (+) / Cash Back (-) to mapped accounts.
 * - Writes an exception CSV, moves the file to Processed (or Failed), emails result.
 *
 * Deposit treatment per order
 *   MATCH                 NS undeposited txns = Amazon paid            -> payments only
 *   FEES_OVERPOSTED       duplicated fee lines on cash sale            -> fees account
 *   REFUND_COMM_SIGN      FarApp nets RefundCommission the wrong way   -> fees account
 *   PARTIAL               cash sale booked > Amazon paid so far        -> clearing (memo = order)
 *   RELEASE_CLEARING      later shipment of cash sale deposited by
 *                         this script ("AMZ STL" memo)                 -> clearing (memo = order)
 *   PRE_CUTOVER           later shipment of manually deposited sale    -> unmatched account
 *   NO_NS_CASH_SALE       Amazon paid, no cash sale in NetSuite        -> unmatched account
 *   DUPLICATE_CASH_SALE   more than one undeposited cash sale          -> unmatched account
 *
 * Configuration
 *   All ids are hardcoded in CONFIG below. Set dryRun to false for live deposits.
 */
define(['N/file', 'N/search', 'N/query', 'N/record', 'N/runtime', 'N/email', 'N/log'],
    (file, search, query, record, runtime, email, log) => {

        const ORDER_RE = /^\d{3}-\d{7}-\d{7}$/;
        const ORDMOD_RE = /^FA-AmzStl-[^-]+-(\d+)-/;
        const MEMO_PREFIX = 'AMZ STL ';

        // ------------------------------------------------------------------ utils
        const toCents = (s) => {
            const n = parseFloat(String(s || '0').replace(/,/g, ''));
            return isNaN(n) ? 0 : Math.round(n * 100);
        };
        const toMoney = (c) => Math.round(c) / 100;
        const fmt = (c) => (toMoney(c)).toFixed(2);
        const csvCell = (v) => {
            const s = v === null || v === undefined ? '' : String(v);
            return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
        };

        // ================================================================ CONFIG
        // Hardcoded settings. Replace each null (TODO) with the internal id.
        const CONFIG = {
            dryRun: false,            // LIVE: creates deposits. Set true to build and report without saving
            folderPending: 23183,     // File Cabinet folder: Pending
            folderProcessed: 23184,   // File Cabinet folder: Processed
            folderFailed: 23185,      // File Cabinet folder: Failed
            subsidiary: 13,           // US subsidiary
            bankAcct: 556,            // operating bank account (USD, sub 13)
            accounts: {
                fees: 276,            // 5524 Freight / Storage Cost : Amazon FBA fees-Shipping
                promo: 394,           // 6050 Marketing : Amazon Promotion - rebates
                reimb: 276,           // reimbursements -> 5524 until client gives a separate account
                liquid: 276,          // liquidations   -> 5524 until client gives a separate account
                clearing: 590,        // Amazon Settlement Clearing (Other Current Asset)
                unmatched: 590        // same as clearing (pre-cutover / not in NS)
            },
            feeItems: [1380, 1327, 1309], // FarApp fee items: Overpaid Fees Adjustment, X001IJUCWT, X001IK43CD
            currency: 'USD',
            maxLines: 8000,           // max payment lines per deposit (21,277 lines -> 3 deposits, ~7,093 each)
            customForm: 220,          // deposit form used for initialization
            bucketSize: 200,          // orders per map bucket (one SuiteQL each)
            classId: 2,               // D2C : Amazon Seller Central (other / cash back lines)
            locationId: null,         // optional
            departmentId: null,       // optional
            notify: 384968            // employee internal id (email author + recipient)
        };

        const getParams = () => CONFIG;

        const validateParams = (p) => {
            const missing = [];
            ['folderPending', 'folderProcessed', 'folderFailed'].forEach((k) => { if (!p[k]) missing.push(k); });
            if (!p.dryRun) {
                if (!p.bankAcct) missing.push('bankAcct');
                Object.keys(p.accounts).forEach((k) => { if (!p.accounts[k]) missing.push(`accounts.${k}`); });
            }
            if (missing.length) throw new Error(`Missing configuration: ${missing.join(', ')}`);
        };

        // ------------------------------------------------------------- file helpers
        const findOldestPendingFile = (folderId) => {
            let found = null;
            search.create({
                type: 'file',
                filters: [['folder', 'anyof', folderId]],
                columns: [search.createColumn({ name: 'internalid', sort: search.Sort.ASC }), 'name']
            }).run().each((r) => {
                found = { id: Number(r.getValue('internalid')), name: r.getValue('name') };
                return false;
            });
            return found;
        };

        const moveFile = (fileId, folderId) => {
            const f = file.load({ id: fileId });
            f.folder = folderId;
            return f.save();
        };

        const writeTextFile = (name, contents, folderId, type) => file.create({
            name, contents, folder: folderId, fileType: type || file.Type.PLAINTEXT
        }).save();

        const sendEmail = (p, subject, body, attachmentIds) => {
            if (!p.notify) return;
            try {
                email.send({
                    author: p.notify,
                    recipients: [p.notify],
                    subject,
                    body,
                    attachments: (attachmentIds || []).map((id) => file.load({ id }))
                });
            } catch (e) {
                log.error('Email failed', e);
            }
        };

        // ---------------------------------------------------------- settlement parse
        const nonOrderKey = (tt, at) => {
            if (/Reimbursement/i.test(at)) return 'reimb';
            if (tt === 'Liquidations') return 'liquid';
            if (/Coupon|Deal/i.test(at)) return 'promo';
            return 'fees';
        };

        const parseSettlementFile = (fileId) => {
            const f = file.load({ id: fileId });
            let cols = null;
            let header = null;
            const orders = {};
            const nonOrder = {};
            let lineCount = 0;

            f.lines.iterator().each((l) => {
                const line = l.value;
                if (!line || !line.trim()) return true;
                const v = line.split('\t');
                if (!cols) {
                    cols = {};
                    v.forEach((name, i) => { cols[name.trim()] = i; });
                    ['settlement-id', 'total-amount', 'order-id', 'amount', 'transaction-type', 'amount-type', 'amount-description']
                        .forEach((c) => { if (cols[c] === undefined) throw new Error(`Settlement file missing column "${c}"`); });
                    return true;
                }
                const g = (name) => (cols[name] === undefined ? '' : (v[cols[name]] || '').trim());

                if (g('total-amount')) {
                    header = {
                        settlementId: g('settlement-id'),
                        start: g('settlement-start-date'),
                        end: g('settlement-end-date'),
                        depositDate: g('deposit-date'),
                        totalCents: toCents(g('total-amount')),
                        currency: g('currency')
                    };
                    return true;
                }

                lineCount++;
                const amt = toCents(g('amount'));
                if (!amt) return true;

                const oid = g('order-id');
                const tt = g('transaction-type');
                const at = g('amount-type');
                const ad = g('amount-description');

                if (ORDER_RE.test(oid)) {
                    const o = orders[oid] || (orders[oid] = { id: oid, net: 0, fba: 0, comm: 0, refComm: 0 });
                    o.net += amt;
                    if (ad === 'FBAPerUnitFulfillmentFee') o.fba += amt;
                    if (ad === 'Commission' || ad === 'RefundCommission') o.comm += amt;
                    if (ad === 'RefundCommission') o.refComm += amt;
                } else {
                    const k = `${nonOrderKey(tt, at)}|${amt > 0 ? 'P' : 'N'}`;
                    const n = nonOrder[k] || (nonOrder[k] = { key: nonOrderKey(tt, at), cents: 0, count: 0, desc: {} });
                    n.cents += amt;
                    n.count++;
                    n.desc[`${at} - ${ad}`] = true;
                }
                return true;
            });

            if (!header) throw new Error('Settlement header row (total-amount) not found');
            return {
                header,
                lineCount,
                orders: Object.keys(orders).map((k) => orders[k]),
                nonOrder: Object.keys(nonOrder).map((k) => ({
                    key: nonOrder[k].key,
                    cents: nonOrder[k].cents,
                    count: nonOrder[k].count,
                    desc: Object.keys(nonOrder[k].desc).join('; ')
                }))
            };
        };

        const depositExists = (settlementId) => {
            const rs = query.runSuiteQL({
                query: `SELECT id, tranid FROM transaction WHERE type = 'Deposit' AND memo LIKE ?`,
                params: [`${MEMO_PREFIX}${settlementId}%`]
            }).asMappedResults();
            return rs.length ? rs : null;
        };

        // ------------------------------------------------------------- NS lookup
        const queryNsForOrders = (orderIds, feeItems) => {
            const out = {};
            if (!orderIds.length) return out;
            const ph = orderIds.map(() => '?').join(',');
            const fees = feeItems.map((x) => parseInt(x, 10)).join(',');
            const sql = `
                SELECT t.id, t.type, t.tranid, t.externalid, t.custbody_nsc_amz_settlement_id AS stl,
                       t.custbody_fa_channel_order AS ord,
                       SUM(CASE WHEN tl.mainline = 'T' THEN tl.foreignamount ELSE 0 END) AS uf_amt,
                       SUM(CASE WHEN tl.mainline = 'F' AND tl.item IN (${fees}) THEN tl.foreignamount ELSE 0 END) AS fee_amt,
                       dep.dep_id, dep.dep_memo
                FROM transaction t
                JOIN transactionline tl ON tl.transaction = t.id
                LEFT JOIN (
                    SELECT nl.previousdoc AS prev, MAX(d.id) AS dep_id, MAX(d.memo) AS dep_memo
                    FROM NextTransactionLineLink nl
                    JOIN transaction d ON d.id = nl.nextdoc
                    WHERE d.type = 'Deposit'
                    GROUP BY nl.previousdoc
                ) dep ON dep.prev = t.id
                WHERE t.type IN ('CashSale', 'CashRfnd')
                  AND t.custbody_fa_channel = 'Amazon'
                  AND t.custbody_fa_channel_order IN (${ph})
                  AND tl.taxline = 'F'
                GROUP BY t.id, t.type, t.tranid, t.externalid, t.custbody_nsc_amz_settlement_id, t.custbody_fa_channel_order, dep.dep_id, dep.dep_memo`;

            query.runSuiteQL({ query: sql, params: orderIds }).asMappedResults().forEach((r) => {
                const k = r.ord;
                (out[k] || (out[k] = [])).push({
                    id: Number(r.id),
                    type: r.type,
                    tranid: r.tranid,
                    extid: r.externalid || '',
                    uf: toCents(r.uf_amt),
                    fee: toCents(r.fee_amt),
                    depId: r.dep_id ? Number(r.dep_id) : null,
                    depMemo: r.dep_memo || '',
                    stl: r.stl ? String(r.stl) : ''
                });
            });
            return out;
        };

        const classifyOrder = (o, txns, settlementId) => {
            const forThisSettlement = (t) => {
                const m = ORDMOD_RE.exec(t.extid);
                return !m || m[1] === settlementId;
            };
            const undep = txns.filter((t) => !t.depId && forThisSettlement(t));
            const undepCs = undep.filter((t) => t.type === 'CashSale');
            const nsNet = undep.reduce((a, t) => a + t.uf, 0);
            const nsFee = undep.reduce((a, t) => a + t.fee, 0);
            const diff = o.net - nsNet;
            const near = (a, b) => Math.abs(a - b) <= 1;

            const res = {
                order: o.id,
                amazon: o.net,
                ns: nsNet,
                diff,
                pay: undep.map((t) => ({ id: t.id, uf: t.uf, tranid: t.tranid, stl: t.stl })),
                docs: txns.map((t) => `${t.tranid}${t.depId ? '[dep]' : ''}:${fmt(t.uf)}`).join(' '),
                code: 'MATCH',
                acct: null
            };
            if (diff === 0) return res;

            if (undepCs.length > 1) {
                res.code = 'DUPLICATE_CASH_SALE'; res.acct = 'unmatched';
            } else if (undepCs.length === 1) {
                const feeExcess = nsFee + o.fba + o.comm;
                if (feeExcess > 0 && near(diff, feeExcess)) { res.code = 'FEES_OVERPOSTED'; res.acct = 'fees'; }
                else if (o.refComm !== 0 && near(diff, 2 * o.refComm)) { res.code = 'REFUND_COMM_SIGN'; res.acct = 'fees'; }
                else { res.code = 'PARTIAL'; res.acct = 'clearing'; }
            } else {
                const prevCs = txns.find((t) => t.type === 'CashSale' && t.depId);
                if (o.refComm !== 0 && near(diff, 2 * o.refComm) && undep.length) { res.code = 'REFUND_COMM_SIGN'; res.acct = 'fees'; }
                else if (prevCs && prevCs.depMemo.indexOf(MEMO_PREFIX) === 0) { res.code = 'RELEASE_CLEARING'; res.acct = 'clearing'; }
                else if (prevCs) { res.code = 'PRE_CUTOVER'; res.acct = 'unmatched'; }
                else { res.code = 'NO_NS_CASH_SALE'; res.acct = 'unmatched'; }
            }
            return res;
        };

        // ------------------------------------------------------------- stages
        const getInputData = () => {
            const p = getParams();
            let pending = null;
            try {
                validateParams(p);
                pending = findOldestPendingFile(p.folderPending);
                if (!pending) {
                    log.debug('No settlement file in Pending');
                    return [];
                }
                log.audit('Processing file', pending);

                const parsed = parseSettlementFile(pending.id);
                const h = parsed.header;
                if (h.currency && h.currency !== p.currency) {
                    throw new Error(`Currency ${h.currency} does not match expected ${p.currency}`);
                }
                const existing = depositExists(h.settlementId);
                if (existing) {
                    throw new Error(`Deposit already exists for settlement ${h.settlementId}: ${existing.map((r) => r.tranid || r.id).join(', ')}`);
                }

                const items = [{ kind: 'header', fileId: pending.id, fileName: pending.name, header: h, lineCount: parsed.lineCount }];
                items.push({ kind: 'nonorder', lines: parsed.nonOrder });
                for (let i = 0; i < parsed.orders.length; i += p.bucketSize) {
                    items.push({ kind: 'bucket', settlementId: h.settlementId, orders: parsed.orders.slice(i, i + p.bucketSize) });
                }
                log.audit('Parsed', `settlement ${h.settlementId}, orders ${parsed.orders.length}, buckets ${items.length - 2}`);
                return items;
            } catch (e) {
                log.error('getInputData failed', e);
                if (pending && p.folderFailed) {
                    try {
                        moveFile(pending.id, p.folderFailed);
                        writeTextFile(`${pending.name}.error.txt`, String(e.message || e), p.folderFailed);
                    } catch (e2) { log.error('Move to Failed failed', e2); }
                }
                sendEmail(p, `Amazon settlement FAILED: ${pending ? pending.name : 'n/a'}`, String(e.message || e));
                return [];
            }
        };

        const map = (context) => {
            const v = JSON.parse(context.value);
            if (v.kind === 'header' || v.kind === 'nonorder') {
                context.write({ key: v.kind, value: context.value });
                return;
            }
            const p = getParams();
            const ns = queryNsForOrders(v.orders.map((o) => o.id), p.feeItems);
            const results = v.orders.map((o) => classifyOrder(o, ns[o.id] || [], v.settlementId));
            context.write({ key: 'orders', value: JSON.stringify(results) });
        };

        // ------------------------------------------------------------- deposit build
        const parseAmazonDate = (s) => {
            const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s || '');
            return m ? new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])) : new Date();
        };

        const addAdjLine = (rec, p, sublistId, acctId, cents, memo) => {
            rec.selectNewLine({ sublistId });
            rec.setCurrentSublistValue({ sublistId, fieldId: 'account', value: acctId });
            rec.setCurrentSublistValue({ sublistId, fieldId: 'amount', value: toMoney(Math.abs(cents)) });
            rec.setCurrentSublistValue({ sublistId, fieldId: 'memo', value: String(memo).substring(0, 300) });
            if (p.classId) rec.setCurrentSublistValue({ sublistId, fieldId: 'class', value: p.classId });
            if (p.locationId) rec.setCurrentSublistValue({ sublistId, fieldId: 'location', value: p.locationId });
            if (p.departmentId) rec.setCurrentSublistValue({ sublistId, fieldId: 'department', value: p.departmentId });
            rec.commitLine({ sublistId });
        };

        // Initializes a deposit with exactly the given payment ids loaded in the Payments list.
        // defaultValues.deposits + disablepaymentfilters bypasses the 10,000-line Undeposited Funds window.
        const initDeposit = (p, h, ids, memo) => {
            const rec = record.create({
                type: record.Type.DEPOSIT,
                isDynamic: true,
                defaultValues: {
                    account: p.bankAcct,
                    customform: p.customForm,
                    disablepaymentfilters: true,
                    deposits: ids.join(',')
                }
            });
            rec.setValue({ fieldId: 'trandate', value: parseAmazonDate(h.depositDate) });
            rec.setValue({ fieldId: 'memo', value: memo });
            const lineCount = rec.getLineCount({ sublistId: 'payment' });
            const index = {};
            for (let i = 0; i < lineCount; i++) {
                index[String(rec.getSublistValue({ sublistId: 'payment', fieldId: 'id', line: i }))] = i;
            }
            const missing = ids.map(String).filter((id) => index[id] === undefined);
            return { rec, lineCount, index, missing };
        };

        const markPayments = (rec, index, ids) => {
            ids.forEach((id) => {
                const line = index[String(id)];
                rec.selectLine({ sublistId: 'payment', line });
                if (!rec.getCurrentSublistValue({ sublistId: 'payment', fieldId: 'deposit' })) {
                    rec.setCurrentSublistValue({ sublistId: 'payment', fieldId: 'deposit', value: true });
                }
                rec.commitLine({ sublistId: 'payment' });
            });
        };

        // Splits payments round-robin into parts of <= maxLines (balanced totals), puts all
        // adjustment lines on the largest part, builds and (unless dry run) saves each deposit.
        const buildDeposits = (p, h, payMap, payStl, adjustments, adjCents, created) => {
            const payIds = Object.keys(payMap);
            const partCount = Math.max(1, Math.ceil(payIds.length / p.maxLines));
            const chunks = Array.from({ length: partCount }, () => []);
            payIds.sort((x, y) => payMap[y] - payMap[x]).forEach((id, i) => chunks[i % partCount].push(id));
            const chunkCents = chunks.map((ids) => ids.reduce((a, id) => a + payMap[id], 0));
            const adjPart = chunkCents.indexOf(Math.max.apply(null, chunkCents));
            const expectedParts = chunkCents.map((c, i) => c + (i === adjPart ? adjCents : 0));
            const bad = expectedParts.map((c, i) => ({ part: i + 1, c })).filter((x) => x.c <= 0);
            if (bad.length) {
                throw new Error(`Deposit parts with zero or negative total: ${bad.map((x) => `${x.part}: ${fmt(x.c)}`).join(', ')}`);
            }

            const parts = [];
            chunks.forEach((ids, i) => {
                const partNo = i + 1;
                const memo = `${MEMO_PREFIX}${h.settlementId} (${partNo}/${partCount})`;
                const { rec, lineCount, index, missing } = initDeposit(p, h, ids, memo);
                if (missing.length && !p.dryRun) {
                    throw new Error(`Part ${partNo}: ${missing.length} of ${ids.length} payments not loaded, e.g. ${missing.slice(0, 10).join(', ')}`);
                }
                markPayments(rec, index, ids.filter((id) => index[String(id)] !== undefined));
                if (i === adjPart) {
                    adjustments.forEach((a) => addAdjLine(rec, p, a.cents > 0 ? 'other' : 'cashback', a.acctId, a.cents, a.memo));
                }
                const expected = expectedParts[i];

                let c;
                if (p.dryRun) {
                    c = { id: null, tranid: `DRY RUN ${partNo}/${partCount}`, totalCents: toCents(rec.getValue({ fieldId: 'total' })) };
                } else {
                    const id = rec.save({ ignoreMandatoryFields: false });
                    const saved = record.load({ type: record.Type.DEPOSIT, id });
                    c = { id, tranid: saved.getValue({ fieldId: 'tranid' }), totalCents: toCents(saved.getValue({ fieldId: 'total' })) };
                    created.push(c);
                }
                parts.push({
                    part: `${partNo}/${partCount}`, deposit: c.tranid, paymentLines: ids.length, loadedLines: lineCount,
                    missing: missing.length, withAdjustments: i === adjPart,
                    netsuiteTotal: toMoney(c.totalCents), expected: toMoney(expected), ok: c.totalCents === expected && !missing.length
                });
                log.audit(`Deposit ${partNo}/${partCount}${p.dryRun ? ' (dry run)' : ''}`, `${c.tranid}, loaded ${lineCount}/${ids.length}, missing ${missing.length}, NS ${fmt(c.totalCents)}, expected ${fmt(expected)}, units ${runtime.getCurrentScript().getRemainingUsage()}`);
                if (!p.dryRun && c.totalCents !== expected) {
                    throw new Error(`Deposit ${c.tranid} total ${fmt(c.totalCents)} does not equal expected ${fmt(expected)}`);
                }
            });
            return parts;
        };

        const summarize = (summary) => {
            const p = getParams();
            let header = null;
            let nonOrder = [];
            const results = [];

            summary.output.iterator().each((key, value) => {
                const v = JSON.parse(value);
                if (key === 'header') header = v;
                else if (key === 'nonorder') nonOrder = v.lines;
                else if (key === 'orders') Array.prototype.push.apply(results, v);
                return true;
            });

            if (!header) {
                log.debug('Nothing processed this run');
                return;
            }
            const h = header.header;
            const created = [];

            const fail = (msg) => {
                log.error('Settlement failed', msg);
                created.forEach((c) => {
                    try { record.delete({ type: record.Type.DEPOSIT, id: c.id }); } catch (e) { log.error(`Could not delete deposit ${c.id}`, e); }
                });
                try {
                    moveFile(header.fileId, p.folderFailed);
                    writeTextFile(`${header.fileName}.error.txt`, msg, p.folderFailed);
                } catch (e) { log.error('Move to Failed failed', e); }
                sendEmail(p, `Amazon settlement ${h.settlementId} FAILED`, msg);
            };

            try {
                const mapErrors = [];
                summary.mapSummary.errors.iterator().each((k, e) => { mapErrors.push(`${k}: ${e}`); return true; });
                if (summary.inputSummary.error) mapErrors.push(`input: ${summary.inputSummary.error}`);
                if (mapErrors.length) throw new Error(`Map errors:\n${mapErrors.slice(0, 20).join('\n')}`);

                // ---- payments
                const payMap = {};
                const payStl = {};
                results.forEach((r) => r.pay.forEach((t) => { payMap[t.id] = t.uf; payStl[t.id] = t.stl || ''; }));
                const payIds = Object.keys(payMap).map(Number);
                const payCents = payIds.reduce((a, id) => a + payMap[id], 0);

                // ---- adjustments
                const adj = {};
                const addAdj = (acctKey, memo, cents, groupKey) => {
                    if (!cents) return;
                    const acctId = p.accounts[acctKey];
                    const k = groupKey || `${acctKey}|${memo}|${cents > 0 ? 'P' : 'N'}`;
                    const a = adj[k] || (adj[k] = { acctKey, acctId, memo, cents: 0 });
                    a.cents += cents;
                };
                results.forEach((r) => {
                    if (r.code === 'MATCH') return;
                    if (r.code === 'FEES_OVERPOSTED') addAdj('fees', 'Fee over-posting reversal (FarApp duplicate fee lines)', r.diff, 'FEES_OVERPOSTED');
                    else if (r.code === 'REFUND_COMM_SIGN') addAdj('fees', 'Refund commission sign correction', r.diff, 'REFUND_COMM_SIGN');
                    else addAdj(r.acct, `${r.order} ${r.code}`, r.diff, `ORDER|${r.order}`);
                });
                nonOrder.forEach((n) => addAdj(n.key, `${n.desc} (${n.count} lines)`, n.cents));
                const adjustments = Object.keys(adj).map((k) => adj[k]).filter((a) => a.cents !== 0);
                const adjCents = adjustments.reduce((a, x) => a + x.cents, 0);

                if (payCents + adjCents !== h.totalCents) {
                    throw new Error(`Pre-check failed: payments ${fmt(payCents)} + adjustments ${fmt(adjCents)} = ${fmt(payCents + adjCents)} vs settlement ${fmt(h.totalCents)}`);
                }

                // ---- deposit array (same layout as the Excel Deposit tab)
                const acctLabel = (a) => `${a.acctKey}${a.acctId ? ` (${a.acctId})` : ' (NOT SET)'}`;
                const depositArray = [{
                    section: 'Payments',
                    account: 'Undeposited Funds',
                    accountId: null,
                    memo: `${payIds.length} FarApp cash sales / refunds`,
                    amount: toMoney(payCents),
                    signed: toMoney(payCents)
                }].concat(adjustments.map((a) => ({
                    section: a.cents > 0 ? 'Other Deposit' : 'Cash Back',
                    account: acctLabel(a),
                    accountId: a.acctId,
                    memo: a.memo,
                    amount: toMoney(Math.abs(a.cents)),
                    signed: toMoney(a.cents)
                })));

                const byAccount = {};
                adjustments.forEach((a) => {
                    const k = `${a.cents > 0 ? 'Other Deposit' : 'Cash Back'}|${acctLabel(a)}`;
                    const x = byAccount[k] || (byAccount[k] = { section: k.split('|')[0], account: acctLabel(a), lines: 0, cents: 0 });
                    x.lines++; x.cents += a.cents;
                });
                const accountSummary = [{ section: 'Payments', account: 'Undeposited Funds', lines: payIds.length, amount: toMoney(payCents) }]
                    .concat(Object.keys(byAccount).sort().map((k) => ({
                        section: byAccount[k].section, account: byAccount[k].account,
                        lines: byAccount[k].lines, amount: toMoney(Math.abs(byAccount[k].cents))
                    })));
                const otherCents = adjustments.filter((a) => a.cents > 0).reduce((x, a) => x + a.cents, 0);
                const cashBackCents = -adjustments.filter((a) => a.cents < 0).reduce((x, a) => x + a.cents, 0);
                const totals = {
                    payments: toMoney(payCents),
                    otherDeposits: toMoney(otherCents),
                    cashBack: toMoney(cashBackCents),
                    depositTotal: toMoney(payCents + otherCents - cashBackCents),
                    settlementTotal: toMoney(h.totalCents),
                    difference: toMoney(payCents + otherCents - cashBackCents - h.totalCents)
                };

                // ---- build deposits (filtered by FarApp settlement tag, passes of maxLines)
                const probe = [];
                const parts = buildDeposits(p, h, payMap, payStl, adjustments, adjCents, created);

                if (!p.dryRun) {
                    const depTotal = created.reduce((a, c) => a + c.totalCents, 0);
                    if (depTotal !== h.totalCents) {
                        throw new Error(`Deposits total ${fmt(depTotal)} does not equal settlement ${fmt(h.totalCents)}`);
                    }
                }

                // ---- exception CSV
                const tag = p.dryRun ? '_DRYRUN' : '';
                const counts = {};
                results.forEach((r) => { counts[r.code] = (counts[r.code] || 0) + 1; });
                const csv = [['amazon_order', 'result', 'amazon_paid', 'ns_undeposited', 'difference', 'deposit_account', 'ns_documents'].join(',')];
                results.filter((r) => r.code !== 'MATCH').forEach((r) => {
                    csv.push([r.order, r.code, fmt(r.amazon), fmt(r.ns), fmt(r.diff), r.acct || '', r.docs].map(csvCell).join(','));
                });
                const csvId = writeTextFile(`${h.settlementId}${tag}_exceptions.csv`, csv.join('\n'), p.folderProcessed, file.Type.CSV);

                // ---- deposit array files
                const depCsv = [['section', 'account', 'account_id', 'memo', 'amount', 'signed_amount'].join(',')]
                    .concat(depositArray.map((d) => [d.section, d.account, d.accountId || '', d.memo, d.amount.toFixed(2), d.signed.toFixed(2)].map(csvCell).join(',')))
                    .concat(['', `,,,Payments,${totals.payments.toFixed(2)},`, `,,,Other deposits,${totals.otherDeposits.toFixed(2)},`,
                        `,,,Cash back,${totals.cashBack.toFixed(2)},`, `,,,Deposit total,${totals.depositTotal.toFixed(2)},`,
                        `,,,Settlement total,${totals.settlementTotal.toFixed(2)},`, `,,,Difference,${totals.difference.toFixed(2)},`]);
                const depCsvId = writeTextFile(`${h.settlementId}${tag}_deposit.csv`, depCsv.join('\n'), p.folderProcessed, file.Type.CSV);
                const output = {
                    settlementId: h.settlementId, period: `${h.start} to ${h.end}`, depositDate: h.depositDate,
                    dryRun: p.dryRun, totals, accountSummary, parts, probe, ordersByResult: counts, depositLines: depositArray
                };
                const jsonId = writeTextFile(`${h.settlementId}${tag}_deposit.json`, JSON.stringify(output, null, 2), p.folderProcessed, file.Type.JSON);

                log.audit('Deposit totals', JSON.stringify(totals));
                log.audit('Deposit by account', JSON.stringify(accountSummary));
                log.audit('Deposit parts', JSON.stringify(parts));

                moveFile(header.fileId, p.folderProcessed);

                const body = [
                    `${p.dryRun ? 'DRY RUN - nothing saved. ' : ''}Settlement ${h.settlementId} (${h.start} to ${h.end}), deposit date ${h.depositDate}`,
                    '',
                    `Payments:        ${totals.payments.toFixed(2)}  (${payIds.length} transactions)`,
                    `Other deposits:  ${totals.otherDeposits.toFixed(2)}`,
                    `Cash back:       ${totals.cashBack.toFixed(2)}`,
                    `Deposit total:   ${totals.depositTotal.toFixed(2)}`,
                    `Settlement:      ${totals.settlementTotal.toFixed(2)}`,
                    `Difference:      ${totals.difference.toFixed(2)}`,
                    '',
                    'By account:',
                    ...accountSummary.map((a) => `  ${a.section} | ${a.account} | ${a.lines} lines | ${a.amount.toFixed(2)}`),
                    '',
                    'Deposit parts:',
                    ...parts.map((x) => `  ${JSON.stringify(x)}`),
                    '',
                    'Orders by result:',
                    ...Object.keys(counts).sort().map((k) => `  ${k}: ${counts[k]}`),
                    ...(probe.length ? ['', 'Deposit sublist probe:', ...probe.map((x) => `  ${JSON.stringify(x)}`)] : [])
                ].join('\n');
                sendEmail(p, `Amazon settlement ${h.settlementId} ${p.dryRun ? 'DRY RUN' : 'deposited'}: ${fmt(h.totalCents)}`, body, [depCsvId, csvId, jsonId]);
                log.audit(p.dryRun ? 'Dry run complete' : 'Settlement complete', body.substring(0, 3900));
            } catch (e) {
                fail(String(e.message || e));
            }
        };

        return { getInputData, map, summarize };
    });