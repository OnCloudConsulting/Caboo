/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * Amazon settlement file -> NetSuite deposit(s)
 * Stages: getInputData parses/matches/reconciles and plans all batches;
 * map routes prepared jobs; reduce builds one deposit per batch;
 * summarize verifies totals, writes reports, moves the file and sends email.
 * Use one active deployment so settlements sharing an order run sequentially.
 * --------------------------------------------------------------------------
 * - Picks the oldest file from the Pending folder (one file per run).
 * - Parses the Amazon V2 settlement flat file (tab delimited).
 * - Matches every Amazon order ID to FarApp cash sales / cash refunds in NetSuite
 *   (by order ID, NOT by FarApp's settlement tag).
 * - Builds one or more deposits (split by max payment lines) whose combined total
 *   equals the settlement total-amount to the cent. Memo: "AMZ STL <id> (n)".
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
            dryRun: false,            // live processing of the oldest Pending file
            auditFileId: 620558,       // inspect this source in place only during dry run
            folderPending: 23183,     // File Cabinet folder: Pending
            folderProcessed: 23184,   // File Cabinet folder: Processed
            folderFailed: 23185,      // File Cabinet folder: Failed
            subsidiary: 13,           // US subsidiary
            bankAcct: 556,            // 1091 JPMorgan Checking - Caboo USA (USD, sub 13)
            customForm: 220,          // Caboo Paper Pro - Bank Deposit
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
            maxLines: 4000,           // max payment lines per deposit
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
            if (!Number.isInteger(p.maxLines) || p.maxLines < 1 || p.maxLines > 4000) missing.push('maxLines (integer 1-4000)');
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


                const oid = g('order-id');
                const tt = g('transaction-type');
                const at = g('amount-type');
                const ad = g('amount-description');

                if (ORDER_RE.test(oid)) {
                    const o = orders[oid] || (orders[oid] = { id: oid, net: 0, fba: 0, comm: 0, refComm: 0,
                        reimbOnly: true, shippingOnly: true, paidUnits: 0, hasAmount: false });
                    o.hasAmount = o.hasAmount || amt !== 0;
                    o.reimbOnly = o.reimbOnly && tt === 'other-transaction' && at === 'FBA Inventory Reimbursement';
                    const shipping = (at === 'ItemPrice' && ['Shipping', 'ShippingTax'].includes(ad)) ||
                        (at === 'ItemFees' && ad === 'ShippingChargeback') ||
                        (at === 'ItemWithheldTax' && /^MarketplaceFacilitator(Tax|VAT)-Shipping$/.test(ad));
                    o.shippingOnly = o.shippingOnly && (tt === 'Order' || tt === 'Refund') && shipping;
                    if (tt === 'Order' && at === 'ItemPrice' && ad === 'Principal') {
                        const qty = Number(g('quantity-purchased'));
                        o.paidUnits += Number.isInteger(qty) && qty > 0 ? qty : NaN;
                    }
                    o.net += amt;
                    if (ad === 'FBAPerUnitFulfillmentFee') o.fba += amt;
                    if (ad === 'Commission' || ad === 'RefundCommission') o.comm += amt;
                    if (ad === 'RefundCommission') o.refComm += amt;
                } else {
                    if (!amt) return true;
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
                orders: Object.keys(orders).map((k) => orders[k]).filter((o) => o.hasAmount),
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

        const bookedUnits = (sale) => {
            const rec = record.load({ type: record.Type.CASH_SALE, id: sale.id });
            let units = 0;
            const count = rec.getLineCount({ sublistId: 'item' });
            if (count >= 10000) return NaN;
            for (let line = 0; line < count; line++) {
                const read = (fieldId) => rec.getSublistValue({ sublistId: 'item', fieldId, line });
                if (['InvtPart', 'Assembly', 'Kit'].includes(read('itemtype'))) {
                    const qty = Number(read('quantity'));
                    if (!Number.isInteger(qty) || qty <= 0) return NaN;
                    units += qty;
                }
            }
            return units;
        };

        const classifyOrder = (o, txns, settlementId, readUnits = bookedUnits) => {
            const forThisSettlement = (t) => {
                const m = ORDMOD_RE.exec(t.extid);
                return !m || m[1] === settlementId;
            };
            let undep = txns.filter((t) => !t.depId && forThisSettlement(t));
            // A reimbursement is a settlement-generated order modification.
            // Match the complete external ID so a merchandise sale cannot satisfy it.
            if (o.reimbOnly) {
                undep = undep.filter((t) => t.stl === settlementId &&
                    /^FA-AmzStl-[^-]+-/.test(t.extid) &&
                    t.extid.replace(/^FA-AmzStl-[^-]+-/, '') === settlementId + '-OrdMod-' + o.id);
            }
            if (o.shippingOnly && o.net === 0) undep = [];
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
            const review = (reason) => Object.assign(res, { code: 'REVIEW_REQUIRED', reason,
                pay: [], ns: 0, acct: null, diff: 0 });
            if (o.shippingOnly && o.net === 0) { res.code = 'SHIPPING_OFFSET'; return res; }
            if (o.reimbOnly) {
                if (undep.length !== 1 || diff !== 0) return review('Expected one available settlement reimbursement matching the file amount');
                res.code = 'REIMBURSEMENT_MATCH';
                return res;
            }
            if (diff === 0) return res;

            if (undepCs.length > 1) {
                res.code = 'DUPLICATE_CASH_SALE'; res.acct = 'unmatched';
            } else if (undepCs.length === 1) {
                const feeExcess = nsFee + o.fba + o.comm;
                if (feeExcess > 0 && near(diff, feeExcess)) { res.code = 'FEES_OVERPOSTED'; res.acct = 'fees'; }
                else if (o.refComm !== 0 && near(diff, 2 * o.refComm)) { res.code = 'REFUND_COMM_SIGN'; res.acct = 'fees'; }
                else {
                    // Approved temporary clearing entry for CS779620 on this settlement only.
                    if (settlementId === '27756550371' && o.id === '114-5893750-7421869' &&
                        undep.length === 1 && String(undepCs[0].id) === '7072035' &&
                        undepCs[0].tranid === 'CS779620' && o.net === 1222 && nsNet === 983) {
                        res.code = 'APPROVED_CLEARING_VARIANCE';
                        res.acct = 'clearing';
                        return res;
                    }
                    if (!(diff < 0 && o.paidUnits > 0 && readUnits(undepCs[0]) > o.paidUnits)) {
                        return review('Unexplained cash-sale difference ' + fmt(diff) + ' or insufficient evidence of unpaid merchandise units');
                    }
                    res.code = 'PARTIAL'; res.acct = 'clearing';
                }
            } else {
                const prevCs = txns.find((t) => t.type === 'CashSale' && t.depId);
                if (o.refComm !== 0 && near(diff, 2 * o.refComm) && undep.length) { res.code = 'REFUND_COMM_SIGN'; res.acct = 'fees'; }
                else if (prevCs && prevCs.depMemo.indexOf(MEMO_PREFIX) === 0) { res.code = 'RELEASE_CLEARING'; res.acct = 'clearing'; }
                else if (prevCs) { res.code = 'PRE_CUTOVER'; res.acct = 'unmatched'; }
                else { res.code = 'NO_NS_CASH_SALE'; res.acct = 'unmatched'; }
            }
            return res;
        };

        // Adjustments can be on a different deposit part from the cash sale.
        // Locate all matching historical deposits, then read their actual sublist
        // amounts: Cash Back opens clearing; Other Deposits consumes it.
        const validateClearingReleases = (p, results) => {
            const releases = results.filter((r) => r.code === 'RELEASE_CLEARING');
            if (!releases.length) return;
            const balances = new Map(releases.map((r) => [r.order, 0]));
            const depositIds = new Set();
            for (let i = 0; i < releases.length; i += p.bucketSize) {
                const memos = [];
                releases.slice(i, i + p.bucketSize).forEach((r) => {
                    memos.push(r.order + ' PARTIAL', r.order + ' RELEASE_CLEARING');
                });
                const rows = query.runSuiteQL({
                    query: `SELECT DISTINCT d.id FROM transaction d
                        JOIN transactionline l ON l.transaction = d.id
                        WHERE d.type = 'Deposit' AND NVL(d.voided, 'F') = 'F'
                        AND d.memo LIKE ? AND l.memo IN (${memos.map(() => '?').join(',')})`,
                    params: [MEMO_PREFIX + '%', ...memos]
                }).asMappedResults();
                if (rows.length >= 5000) throw new Error('Clearing history exceeds query limit; review settlement before posting');
                rows.forEach((row) => depositIds.add(Number(row.id)));
            }
            depositIds.forEach((id) => {
                const dep = record.load({ type: record.Type.DEPOSIT, id });
                if (String(dep.getValue({ fieldId: 'account' })) !== String(p.bankAcct)) {
                    throw new Error(`Clearing history on deposit ${id} uses another bank account; review before posting`);
                }
                ['cashback', 'other'].forEach((sublistId) => {
                    const count = dep.getLineCount({ sublistId });
                    if (count >= 10000) throw new Error(`Cannot verify all clearing lines on deposit ${id}`);
                    for (let line = 0; line < count; line++) {
                        const read = (fieldId) => dep.getSublistValue({ sublistId, fieldId, line });
                        const match = /^(\d{3}-\d{7}-\d{7}) (PARTIAL|RELEASE_CLEARING)$/.exec(read('memo') || '');
                        if (!match || !balances.has(match[1])) continue;
                        if (String(read('account')) !== String(p.accounts.clearing)) {
                            throw new Error(`Clearing account mismatch on deposit ${id}, order ${match[1]}`);
                        }
                        const cents = toCents(read('amount'));
                        if (cents <= 0) throw new Error(`Invalid clearing amount on deposit ${id}, order ${match[1]}`);
                        const signed = sublistId === 'cashback' ? cents : -cents;
                        balances.set(match[1], balances.get(match[1]) + signed);
                    }
                });
            });
            releases.forEach((r) => {
                const available = balances.get(r.order);
                if (r.diff <= 0 || r.diff > available) {
                    throw new Error(`Clearing review for order ${r.order}: requested ${fmt(r.diff)}, outstanding ${fmt(available)}. No deposits created.`);
                }
                r.docs += ` | Clearing before ${fmt(available)}, released ${fmt(r.diff)}, remaining ${fmt(available - r.diff)}`;
            });
        };

        // ------------------------------------------------------------- stages
        const getInputData = () => {
            const p = getParams();
            let pending = null;
            try {
                validateParams(p);
                const auditSource = p.dryRun && p.auditFileId;
                pending = auditSource ? { id: auditSource, name: file.load({ id: auditSource }).name } : findOldestPendingFile(p.folderPending);
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
                const existing = !auditSource && depositExists(h.settlementId);
                if (existing) {
                    throw new Error(`Deposit already exists for settlement ${h.settlementId}: ${existing.map((r) => r.tranid || r.id).join(', ')}`);
                }

                const results = [];
                for (let i = 0; i < parsed.orders.length; i += p.bucketSize) {
                    const orders = parsed.orders.slice(i, i + p.bucketSize);
                    const ns = queryNsForOrders(orders.map((o) => o.id), p.feeItems);
                    orders.forEach((o) => results.push(classifyOrder(o, ns[o.id] || [], h.settlementId)));
                }
                const header = { fileId: pending.id, fileName: pending.name, header: h, lineCount: parsed.lineCount };
                const held = results.filter((r) => r.code === 'REVIEW_REQUIRED');
                if (held.length || auditSource) {
                    const report = [['Order','Result','Amazon cents','Payment IDs','Selected cents','Adjustment cents','Reason','Candidates']];
                    results.forEach((r) => report.push([r.order,r.code,r.amazon,r.pay.map((t) => t.id).join('|'),
                        r.ns,r.diff,r.reason || '',r.docs]));
                    const reportId = writeTextFile(h.settlementId + '_order_audit_' + Date.now() + '.csv',
                        report.map((row) => row.map(csvCell).join(',')).join('\n'),
                        p.dryRun ? p.folderProcessed : p.folderFailed, file.Type.CSV);
                    log.audit('Settlement audit', { settlement: h.settlementId, orders: results.length,
                        reviewRequired: held.length, reportId });
                    if (held.length) throw new Error(held.length + ' orders require review; see report ' + reportId);
                    if (auditSource) {
                        validateClearingReleases(p, results);
                        const preview = prepareSettlement(p, h, results, parsed.nonOrder);
                        log.audit('Audit plan', { parts: planBatches(p,h,preview).length, totals: preview.totals });
                        return [];
                    }
                }
                validateClearingReleases(p, results);
                const plan = prepareSettlement(p, h, results, parsed.nonOrder);
                const batches = planBatches(p, h, plan);
                const meta = { header, totals: plan.totals, accountSummary: plan.accountSummary,
                    depositArray: plan.depositArray, payCount: plan.payIds.length,
                    expectedParts: batches.length, expectedOrders: results.length };
                const items = [{ kind: 'meta', key: 'meta', data: meta }];
                for (let i = 0; i < results.length; i += p.bucketSize) {
                    items.push({ kind: 'orders', key: `orders:${i}`, data: results.slice(i, i + p.bucketSize) });
                }
                batches.forEach((batch) => items.push({ kind: 'deposit', key: `deposit:${batch.part}`, header, batch }));
                log.audit('Prepared settlement', JSON.stringify({ settlementId: h.settlementId,
                    orders: results.length, deposits: batches.length }));
                return items;
            } catch (e) {
                log.error('getInputData failed', e);
                if (p.dryRun && p.auditFileId) return [];
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

        // Batch planning must finish before independent deposit reducers start.
        const map = (context) => {
            const item = JSON.parse(context.value);
            context.write({ key: item.key, value: JSON.stringify(item) });
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

        // Match orders first, then initialize each settlement-tag group with its exact
        // payment IDs. UI custom filter fields are not relied on to refresh the list.
        // Oracle documents defaultValues.deposits for selecting payment IDs.
        const openDeposit = (p, h, ids) => {
            const rec = record.create({
                type: record.Type.DEPOSIT,
                isDynamic: true,
                defaultValues: {
                    account: p.bankAcct,
                    customform: p.customForm,
                    disablepaymentfilters: true,
                    ...(ids.length ? { deposits: ids.join(',') } : {})
                }
            });
            rec.setValue({ fieldId: 'trandate', value: parseAmazonDate(h.depositDate) });
            const lineCount = rec.getLineCount({ sublistId: 'payment' });
            const index = {};
            const wanted = new Set(ids);
            for (let i = 0; i < lineCount; i++) {
                const id = String(rec.getSublistValue({ sublistId: 'payment', fieldId: 'id', line: i }));
                if (index[id] !== undefined && wanted.has(id)) {
                    throw new Error(`Payment ${id} appears more than once on the deposit list`);
                }
                index[id] = i;
            }
            const missing = ids.filter((id) => index[id] === undefined);
            log.audit('Payment sublist initialization', JSON.stringify({
                settlementId: h.settlementId, requestedPayments: ids.length,
                paymentLineCount: lineCount, missingPayments: missing.length
            }));
            if (missing.length) {
                throw new Error(`${missing.length} requested payments unavailable on deposit list: ${missing.slice(0, 20).join(', ')}`);
            }
            return { rec, lineCount, index };
        };

        const markPayments = (rec, ids) => {
            const wanted = new Set(ids);
            const n = rec.getLineCount({ sublistId: 'payment' });
            let selected = 0;
            // NetSuite may return extra available rows. Select only the requested IDs,
            // and clear any other selections supplied by defaults or account scripts.
            for (let i = 0; i < n; i++) {
                const id = String(rec.getSublistValue({ sublistId: 'payment', fieldId: 'id', line: i }));
                const shouldSelect = wanted.has(id);
                const current = rec.getSublistValue({ sublistId: 'payment', fieldId: 'deposit', line: i });
                if ((current === true || current === 'T') !== shouldSelect) {
                    rec.selectLine({ sublistId: 'payment', line: i });
                    rec.setCurrentSublistValue({ sublistId: 'payment', fieldId: 'deposit', value: shouldSelect });
                    rec.commitLine({ sublistId: 'payment' });
                }
                const checked = rec.getSublistValue({ sublistId: 'payment', fieldId: 'deposit', line: i });
                if (checked === true || checked === 'T') {
                    if (!shouldSelect) throw new Error(`Unexpected selected payment ${id}`);
                    selected++;
                }
            }
            if (selected !== ids.length) throw new Error(`Selected ${selected} payments; expected ${ids.length}`);
            return selected;
        };

        const prepareSettlement = (p, h, results, nonOrder) => {
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

            return { payMap, payStl, payIds, adjustments, adjCents, depositArray, accountSummary, totals };
        };

        const planBatches = (p, h, plan) => {
            const { payMap, payStl, adjustments, adjCents } = plan;
            const ids = Object.keys(payMap);
            const groups = {};
            ids.forEach((id) => {
                const tag = payStl[id] || '';
                (groups[tag] || (groups[tag] = [])).push(id);
            });
            const order = Object.keys(groups).sort((x, y) =>
                x === y ? 0 : x === h.settlementId ? -1 : y === h.settlementId ? 1 : groups[y].length - groups[x].length);
            const sum = (list) => list.reduce((total, id) => total + payMap[id], 0);
            const limit = p.maxLines;
            if (!Number.isInteger(limit) || limit < 1 || limit > 4000) throw new Error('Invalid maxLines');
            if (!ids.length) {
                if (!adjustments.length) return [];
                if (adjCents <= 0 || adjCents !== h.totalCents) {
                    throw new Error('An adjustment-only deposit requires a positive total equal to the settlement');
                }
                return [{ part: 1, tag: h.settlementId, sourceTags: [], groupLines: 0,
                    ids: [], adjustments, withAdjustments: true, expectedCents: adjCents }];
            }

            // Keep the old grouping/order whenever it already produces positive deposits.
            const original = [];
            let adjDone = adjustments.length === 0;
            order.forEach((tag) => {
                for (let offset = 0; offset < groups[tag].length; offset += limit) {
                    const take = groups[tag].slice(offset, offset + limit);
                    const payCents = sum(take);
                    const withAdj = !adjDone && payCents + adjCents > 0;
                    original.push({ ids: take, withAdjustments: withAdj,
                        expectedCents: payCents + (withAdj ? adjCents : 0) });
                    if (withAdj) adjDone = true;
                }
            });
            let planned = adjDone && original.every((b) => b.expectedCents > 0) ? original : null;

            // Place the largest magnitudes first. Positive payments go to the
            // lowest-total batch; negative payments go to the highest-total batch.
            // All original adjustments stay together on one designated batch.
            const balance = (list, includeAdjustments) => {
                const count = Math.ceil(list.length / limit);
                const bins = Array.from({ length: count }, (_, i) => ({
                    ids: [], withAdjustments: includeAdjustments && i === 0,
                    expectedCents: includeAdjustments && i === 0 ? adjCents : 0
                }));
                const sorted = list.slice().sort((x, y) =>
                    Math.abs(payMap[y]) - Math.abs(payMap[x]) || Number(x) - Number(y));
                sorted.forEach((id) => {
                    const amount = payMap[id];
                    let target = null;
                    bins.forEach((bin) => {
                        if (bin.ids.length >= limit) return;
                        if (!target || (amount >= 0
                            ? bin.expectedCents < target.expectedCents
                            : bin.expectedCents > target.expectedCents) ||
                            (bin.expectedCents === target.expectedCents && bin.ids.length < target.ids.length)) target = bin;
                    });
                    target.ids.push(id);
                    target.expectedCents += amount;
                });
                // Repair a non-positive bin by exchanging a low-value payment
                // with a higher-value payment from a bin that remains positive.
                let guard = 0;
                while (bins.some((bin) => bin.expectedCents <= 0) && guard++ < list.length) {
                    const bad = bins.filter((bin) => bin.expectedCents <= 0)
                        .sort((x, y) => x.expectedCents - y.expectedCents)[0];
                    let swap = null;
                    bins.forEach((donor) => {
                        if (donor === bad || donor.expectedCents <= 1) return;
                        // Lowest payment in the deficient bin offers the largest
                        // possible improvement for each candidate donor payment.
                        const low = bad.ids.reduce((x, y) => payMap[x] < payMap[y] ? x : y);
                        donor.ids.forEach((high) => {
                            const gain = payMap[high] - payMap[low];
                            if (gain > 0 && donor.expectedCents - gain > 0 && (!swap || gain > swap.gain)) {
                                swap = { donor, low, high, gain };
                            }
                        });
                    });
                    if (!swap) break;
                    bad.ids[bad.ids.indexOf(swap.low)] = swap.high;
                    swap.donor.ids[swap.donor.ids.indexOf(swap.high)] = swap.low;
                    bad.expectedCents += swap.gain;
                    swap.donor.expectedCents -= swap.gain;
                }
                return bins.every((bin) => bin.ids.length && bin.expectedCents > 0) ? bins : null;
            };

            if (!planned) {
                // Prefer retaining settlement-tag boundaries. Try the group with
                // the strongest net balance for the unchanged adjustment bundle.
                const candidates = adjustments.length
                    ? order.slice().sort((x, y) => sum(groups[y]) - sum(groups[x]))
                    : [null];
                for (const adjustmentTag of candidates) {
                    const trial = [];
                    let valid = true;
                    for (const tag of order) {
                        const bins = balance(groups[tag], adjustments.length > 0 && tag === adjustmentTag);
                        if (!bins) { valid = false; break; }
                        trial.push(...bins);
                    }
                    if (valid) { planned = trial; break; }
                }
                if (!planned) {
                    // Some tags contain only refunds. Exact-ID initialization
                    // permits pairing those with positive payments from another
                    // tag within THIS SAME Amazon settlement file.
                    planned = balance(ids, adjustments.length > 0);
                }
                if (!planned) throw new Error('Unable to form positive deposits within the 4000-payment limit. No deposits created; review payment amounts and the adjustment total.');
                log.audit('Rebalanced deposit batches', JSON.stringify({
                    settlementId: h.settlementId, payments: ids.length, batches: planned.length
                }));
            }

            // Enforce completeness and cent-level equality before any reducer saves.
            const remaining = new Set(ids);
            let expectedTotal = 0, adjustmentBatches = 0;
            const batches = planned.map((bin, i) => {
                if (!bin.ids.length || bin.ids.length > limit) throw new Error('Invalid deposit batch size');
                bin.ids.forEach((id) => {
                    if (!remaining.delete(id)) throw new Error(`Duplicate or unexpected payment ${id}`);
                });
                if (bin.withAdjustments) adjustmentBatches++;
                const expected = sum(bin.ids) + (bin.withAdjustments ? adjCents : 0);
                if (expected <= 0 || expected !== bin.expectedCents) throw new Error('Invalid balanced deposit total');
                expectedTotal += expected;
                const sourceTags = [...new Set(bin.ids.map((id) => payStl[id] || ''))];
                const tag = sourceTags.length === 1 ? sourceTags[0] : 'MIXED';
                return { part: i + 1, tag, sourceTags,
                    groupLines: sourceTags.reduce((n, t) => n + groups[t].length, 0), ids: bin.ids,
                    adjustments: bin.withAdjustments ? adjustments : [],
                    withAdjustments: bin.withAdjustments, expectedCents: expected };
            });
            if (remaining.size || adjustmentBatches !== (adjustments.length ? 1 : 0) || expectedTotal !== h.totalCents) {
                throw new Error('Balanced batches failed payment/adjustment/settlement reconciliation');
            }
            return batches;
        };

        const reduce = (context) => {
            const item = JSON.parse(context.values[0]);
            if (item.kind !== 'deposit') {
                context.write({ key: context.key, value: JSON.stringify(item) });
                return;
            }
            const p = getParams();
            const h = item.header.header;
            const b = item.batch;
            const memo = `${MEMO_PREFIX}${h.settlementId} (${b.part})`;
            let savedId = null;
            const startedAt = Date.now();
            const trace = (event, details) => log.audit(event, JSON.stringify({
                settlementId: h.settlementId, part: b.part, dryRun: p.dryRun,
                elapsedSeconds: Math.round((Date.now() - startedAt) / 1000), ...details
            }));
            trace('DEPOSIT START', {
                memo, bankAccount: p.bankAcct, customForm: p.customForm,
                depositDate: h.depositDate, sourceTags: b.sourceTags,
                requestedPayments: b.ids.length,
                adjustmentLines: b.adjustments.length,
                adjustmentTotal: toMoney(b.adjustments.reduce((n, a) => n + a.cents, 0)),
                expectedTotal: toMoney(b.expectedCents), restarted: !!context.isRestarted
            });
            try {
                // Recover a deposit saved before a reducer interruption. An exact
                // memo lookup avoids creating the same part twice on a retry.
                let existing = [];
                if (!p.dryRun) existing = query.runSuiteQL({
                    query: "SELECT id FROM transaction WHERE type = 'Deposit' AND memo = ?",
                    params: [memo]
                }).asMappedResults();
                if (existing.length > 1) throw new Error(`Multiple deposits already use memo ${memo}`);
                let rec, lineCount, selected;
                if (existing.length) {
                    if (!context.isRestarted) throw new Error(`Deposit already exists for ${memo}; refusing concurrent/duplicate run`);
                    savedId = Number(existing[0].id);
                    trace('DEPOSIT RECOVERED', { depositId: savedId, memo });
                } else {
                    const opened = openDeposit(p, h, b.ids);
                    rec = opened.rec;
                    lineCount = opened.lineCount;
                    selected = markPayments(rec, b.ids);
                    rec.setValue({ fieldId: 'memo', value: memo });
                    b.adjustments.forEach((a) => addAdjLine(rec, p, a.cents > 0 ? 'other' : 'cashback', a.acctId, a.cents, a.memo));
                    trace('DEPOSIT READY TO SAVE', {
                        memo, paymentListLines: lineCount, requestedPayments: b.ids.length,
                        selectedPayments: selected, adjustmentLines: b.adjustments.length,
                        expectedTotal: toMoney(b.expectedCents)
                    });
                    if (!p.dryRun) {
                        savedId = rec.save({ ignoreMandatoryFields: false });
                        trace('DEPOSIT SAVED - VALIDATION PENDING', { depositId: savedId, memo });
                    }
                }
                const part = { part: b.part, tag: b.tag || '(no tag)', sourceTags: b.sourceTags, groupLines: b.groupLines,
                    listLines: lineCount, requestedPayments: b.ids.length,
                    availableRequestedPayments: b.ids.length, selectedPayments: selected,
                    paymentLines: b.ids.length, withAdjustments: b.withAdjustments,
                    expected: toMoney(b.expectedCents) };
                let created = null;
                if (!p.dryRun) {
                    const saved = record.load({ type: record.Type.DEPOSIT, id: savedId,
                        defaultValues: { disablepaymentfilters: true } });
                    const actual = toCents(saved.getValue({ fieldId: 'total' }));
                    trace('DEPOSIT SAVED TOTAL CHECK', {
                        depositId: savedId, documentNumber: saved.getValue({ fieldId: 'tranid' }),
                        bankAccount: saved.getValue({ fieldId: 'account' }),
                        expectedBankAccount: p.bankAcct,
                        actualTotal: toMoney(actual), expectedTotal: toMoney(b.expectedCents),
                        difference: toMoney(actual - b.expectedCents)
                    });
                    if (actual !== b.expectedCents) throw new Error(`Deposit ${savedId} total ${fmt(actual)} does not equal expected ${fmt(b.expectedCents)}`);
                    if (String(saved.getValue({ fieldId: 'account' })) !== String(p.bankAcct)) throw new Error(`Deposit ${savedId} bank account mismatch`);
                    // Verify saved payment identities too, including recovered parts.
                    const wanted = new Set(b.ids);
                    let selectedSaved = 0;
                    for (let line = 0; line < saved.getLineCount({ sublistId: 'payment' }); line++) {
                        const checked = saved.getSublistValue({ sublistId: 'payment', fieldId: 'deposit', line });
                        if (checked !== true && checked !== 'T') continue;
                        const id = String(saved.getSublistValue({ sublistId: 'payment', fieldId: 'id', line }));
                        if (!wanted.delete(id)) throw new Error(`Unexpected saved payment ${id} on deposit ${savedId}`);
                        selectedSaved++;
                    }
                    if (wanted.size || selectedSaved !== b.ids.length) throw new Error(`Saved payment mismatch on deposit ${savedId}`);
                    created = { id: savedId, tranid: saved.getValue({ fieldId: 'tranid' }), totalCents: actual, expectedCents: b.expectedCents };
                    trace('DEPOSIT VALIDATED', {
                        depositId: savedId, documentNumber: created.tranid, memo,
                        requestedPayments: b.ids.length, savedSelectedPayments: selectedSaved,
                        missingPayments: wanted.size, actualTotal: toMoney(actual),
                        expectedTotal: toMoney(b.expectedCents), difference: toMoney(actual - b.expectedCents)
                    });
                    part.depositId = savedId;
                    part.deposit = created.tranid;
                    part.netsuiteTotal = toMoney(actual);
                    part.selectedPayments = selectedSaved;
                }
                context.write({ key: context.key, value: JSON.stringify({ kind: 'deposit', part, created }) });
                log.audit(`${p.dryRun ? 'Dry run' : 'Deposit'} part ${b.part}`, JSON.stringify(part));
            } catch (e) {
                log.error('DEPOSIT FAILED', JSON.stringify({
                    settlementId: h.settlementId, part: b.part, depositId: savedId,
                    memo, elapsedSeconds: Math.round((Date.now() - startedAt) / 1000),
                    name: e.name, message: String(e.message || e), stack: e.stack || ''
                }));
                // Emit known IDs so summarize can roll back every completed part.
                context.write({ key: context.key, value: JSON.stringify({ kind: 'failure',
                    part: b.part, created: savedId ? { id: savedId } : null,
                    duplicateConflict: /refusing concurrent\/duplicate run|Multiple deposits already use memo/.test(String(e.message || e)),
                    error: String(e.message || e) }) });
            }
        };

        const summarize = (summary) => {
            const p = getParams();
            let meta = null;
            let duplicateConflict = false;
            const results = [], parts = [], created = [], errors = [];
            summary.output.iterator().each((key, value) => {
                const v = JSON.parse(value);
                if (v.kind === 'meta') meta = v.data;
                else if (v.kind === 'orders') results.push(...v.data);
                else {
                    if (v.created) created.push(v.created);
                    if (v.kind === 'failure') {
                        duplicateConflict = duplicateConflict || v.duplicateConflict;
                        errors.push(`Part ${v.part}: ${v.error}`);
                    }
                    else if (v.kind === 'deposit') parts.push(v.part);
                }
                return true;
            });
            if (summary.inputSummary.error) errors.push(String(summary.inputSummary.error));
            [summary.mapSummary, summary.reduceSummary].forEach((stage) => {
                stage.errors.iterator().each((key, error) => { errors.push(`${key}: ${error}`); return true; });
            });
            if (!meta) {
                if (errors.length || created.length) {
                    log.error('Settlement metadata missing', JSON.stringify({ errors, created }));
                    created.forEach((c) => { try { record.delete({ type: record.Type.DEPOSIT, id: c.id }); log.audit('ROLLBACK DEPOSIT DELETED', JSON.stringify({ depositId: c.id })); }
                        catch (e) { log.error(`Could not delete deposit ${c.id}`, e); } });
                    sendEmail(p, 'Amazon settlement FAILED: metadata unavailable', errors.join('\n') + '\nReview Pending files and deposit IDs in execution log.');
                }
                return;
            }
            const { header, totals, accountSummary, depositArray, payCount } = meta;
            const h = header.header;
            parts.sort((a, b) => a.part - b.part);
            const fail = (msg) => {
                log.error('Settlement failed', msg);
                // A reducer can be terminated after save but before context.write.
                // Discover those parts as well, using exact planned memos.
                if (!p.dryRun && !duplicateConflict) {
                    const known = new Set(created.map((c) => String(c.id)));
                    for (let part = 1; part <= meta.expectedParts; part++) {
                        try {
                            query.runSuiteQL({
                                query: "SELECT id FROM transaction WHERE type = 'Deposit' AND memo = ?",
                                params: [`${MEMO_PREFIX}${h.settlementId} (${part})`]
                            }).asMappedResults().forEach((row) => {
                                if (!known.has(String(row.id))) {
                                    known.add(String(row.id));
                                    created.push({ id: Number(row.id) });
                                }
                            });
                        } catch (e) { log.error(`Could not locate part ${part} for rollback`, e); }
                    }
                }
                created.forEach((c) => {
                    try { record.delete({ type: record.Type.DEPOSIT, id: c.id }); log.audit('ROLLBACK DEPOSIT DELETED', JSON.stringify({ depositId: c.id })); }
                    catch (e) { log.error(`Could not delete deposit ${c.id}`, e); }
                });
                try {
                    moveFile(header.fileId, p.folderFailed);
                    writeTextFile(`${header.fileName}.error.txt`, msg, p.folderFailed);
                } catch (e) { log.error('Move to Failed failed', e); }
                sendEmail(p, `Amazon settlement ${h.settlementId} FAILED`, msg);
            };
            try {
                if (errors.length) throw new Error(errors.slice(0, 20).join('\n'));
                if (results.length !== meta.expectedOrders) throw new Error(`Expected ${meta.expectedOrders} order results, received ${results.length}`);
                if (parts.length !== meta.expectedParts) throw new Error(`Expected ${meta.expectedParts} deposit results, received ${parts.length}`);
                if (!p.dryRun) {
                    const total = created.reduce((sum, c) => sum + c.totalCents, 0);
                    log.audit('LIVE SETTLEMENT TOTAL CHECK', JSON.stringify({
                        settlementId: h.settlementId, expectedParts: meta.expectedParts,
                        completedParts: parts.length, requestedPayments: payCount,
                        savedSelectedPayments: parts.reduce((n, part) => n + part.selectedPayments, 0),
                        actualDepositTotal: toMoney(total), settlementTotal: toMoney(h.totalCents),
                        difference: toMoney(total - h.totalCents), depositIds: created.map((c) => c.id)
                    }));
                    if (total !== h.totalCents) throw new Error(`Deposits total ${fmt(total)} does not equal settlement ${fmt(h.totalCents)}`);
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
                    dryRun: p.dryRun, createdDeposits: created, totals, accountSummary, parts, ordersByResult: counts, depositLines: depositArray
                };
                const jsonId = writeTextFile(`${h.settlementId}${tag}_deposit.json`, JSON.stringify(output, null, 2), p.folderProcessed, file.Type.JSON);

                log.audit('Deposit totals', JSON.stringify(totals));
                log.audit('Deposit by account', JSON.stringify(accountSummary));
                log.audit('Deposit parts', JSON.stringify(parts));

                moveFile(header.fileId, p.folderProcessed);

                const body = [
                    `${p.dryRun ? 'DRY RUN - no deposits saved. Reports and file moves still performed. ' : ''}Settlement ${h.settlementId} (${h.start} to ${h.end}), deposit date ${h.depositDate}`,
                    '',
                    `Payments:        ${totals.payments.toFixed(2)}  (${payCount} transactions)`,
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
                    ...Object.keys(counts).sort().map((k) => `  ${k}: ${counts[k]}`)
                ].join('\n');
                sendEmail(p, `Amazon settlement ${h.settlementId} ${p.dryRun ? 'DRY RUN' : 'deposited'}: ${fmt(h.totalCents)}`, body, [depCsvId, csvId, jsonId]);
                log.audit(p.dryRun ? 'Dry run complete' : 'Settlement complete', body.substring(0, 3900));
                if (!p.dryRun) {
                    // Separate compact entries avoid truncating the deposit-ID list.
                    for (let i = 0; i < created.length; i += 20) {
                        log.audit('LIVE RUN COMPLETED - DEPOSIT IDS', JSON.stringify({
                            settlementId: h.settlementId, fileId: header.fileId,
                            status: 'SUCCESS', totalDeposits: created.length,
                            deposits: created.slice(i, i + 20).map((c) => ({
                                id: c.id, documentNumber: c.tranid, total: toMoney(c.totalCents)
                            }))
                        }));
                    }
                }
            } catch (e) {
                fail(String(e.message || e));
            }
        };

        return { getInputData, map, reduce, summarize };
    });
