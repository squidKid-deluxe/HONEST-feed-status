// export default GrapheneRPC;
// export default GrapheneRPCPool;

/*
 * Provides a hook to a bitshares node and returns human-readable data
 */
class GrapheneRPC {
    /**
     * @param {string} url - WebSocket URL
     * @param {number} [timeout=10000] - Timeout for the connection handshake in ms
     */
    constructor(url, timeout = 10000) {
        this.url = url;
        this.ws = null;
        this.connected = false;
        this.requestId = 1;
        this.queue = [];
        this.timeout = timeout;
        this.connectionPromise = this.connect();
        // A failed handshake rejects this promise forever; consume it so dead
        // nodes don't surface as unhandled rejections (the browser already
        // logs the underlying WebSocket error).
        this.connectionPromise.catch(() => {});
    }

    /**
     * Attempt to connect and return a Promise that resolves on open, rejects on timeout
     * @returns {Promise<void>}
     */
    connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(this.url);

            const timer = setTimeout(() => {
                if (!this.connected) {
                    this.ws.close();
                    reject(new Error(`GrapheneRPC: Connection timeout after ${this.timeout} ms`));
                }
            }, this.timeout);

            this.ws.onopen = () => {
                clearTimeout(timer);
                this.connected = true;

                // Send queued messages
                while (this.queue.length) {
                    this.ws.send(this.queue.shift());
                }
                resolve();
            };

            this.ws.onclose = () => {
                this.connected = false;
            };

            this.ws.onerror = (err) => {
                clearTimeout(timer);
                reject();//new Error(`GrapheneRPC: WebSocket error: ${err.message || err}`));
            };
        });
    }

    query(api, params) {
        return new Promise((resolve, reject) => {
            const requestId = this.requestId++;
            const payload = JSON.stringify({
                method: "call",
                params: [api, ...params],
                jsonrpc: "2.0",
                id: requestId,
            });

            const listener = (event) => {
                const response = JSON.parse(event.data);
                if (response.id === requestId) {
                    this.ws.removeEventListener("message", listener);
                    if ("result" in response) {
                        resolve(response.result);
                    } else {
                        reject(response.error);
                    }
                }
            };

            this.ws.addEventListener("message", listener);

            if (this.connected) {
                this.ws.send(payload);
            } else {
                this.queue.push(payload);
            }
        });
    }

    precision(number, places) {
        return parseFloat(number).toFixed(places);
    }

    /**
     * Return data about objects in 1.7.x, 2.4.x, 1.3.x, etc. format.
     */

    /**
     * Batch get_objects with automatic chunking for >90 ids
     * @param {string[]} objectIds 
     * @returns {Promise<Object>} map of objectId -> object
     */
    async getObjects(objectIds) {
        const results = [];

        for (let i = 0; i < objectIds.length; i += 90) {
            const chunk = objectIds.slice(i, i + 90);
            const chunkResult = await this.query("database", ["get_objects", [chunk]]);
            results.push(...chunkResult);
        }

        // Return as array matching input order
        return results;
    }


    /**
     * Fetches account balances for specified asset IDs.
     * Uses: cache.account_name
     */
    async rpcAccountBalances(cache, assetIds, assetPrecisions) {
        if (!assetIds.includes("1.3.0")) {
            assetIds.push("1.3.0");
            assetPrecisions.push(5);
        }
        const ret = await this.query("database", ["get_named_account_balances", [cache.account_name, assetIds]]);
        const balances = Object.fromEntries(assetIds.map(id => [id, 0]));
        for (let i = 0; i < assetIds.length; i++) {
            for (const balance of ret) {
                if (balance.asset_id === assetIds[i]) {
                    balances[assetIds[i]] += parseFloat(balance.amount) / Math.pow(10, assetPrecisions[i]);
                }
            }
        }
        return balances;
    }

    /**
     * Retrieves recent trade history between 'now' and 'then'.
     * Uses: cache.currency, cache.asset, cache.asset_precision
     */
    async rpcMarketHistory(cache, now, then, depth = 100) {
        const tradeHistory = await this.query("database", ["get_trade_history", [cache.currency, cache.asset, now, then, depth]]);
        const history = tradeHistory.map(value => {
            const unix = Math.floor(new Date(value.date).getTime() / 1000);
            const price = this.precision(value.price, 16);
            if (parseFloat(price) === 0) throw new Error("zero price in history");
            const amount = this.precision(value.amount, cache.asset_precision);
            return [unix, price, amount];
        });
        if (history.length === 0) throw new Error("no history");
        return history;
    }

    /**
     * Looks up asset symbols and precisions.
     * Uses: cache.asset, cache.currency
     */
    async rpcLookupAssetSymbols(cache) {
        const ret = await this.query("database", ["lookup_asset_symbols", [
            [cache.asset, cache.currency]
        ]]);
        return [ret[0].id, ret[0].precision, ret[1].id, ret[1].precision];
    }

    /**
     * Checks recent blocks' timestamp to compute latency.
     * Uses: storage.mean_ping
     */
    async rpcBlockLatency(storage) {
        const dgp = await this.query("database", ["get_dynamic_global_properties", []]);
        const blocktime = new Date(dgp.time).getTime() / 1000;
        const latency = Math.min(9.999, (Date.now() / 1000) - blocktime);
        const max = Math.min(9.999, 3 + 3 * storage.mean_ping);
        if (latency > max) throw new Error("stale blocktime", latency);
        return [latency, max, Math.floor(blocktime)];
    }

    /**
     * Looks up account info by name.
     * Uses: cache.account_name
     */
    async rpcLookupAccounts(cache) {
        const ret = await this.query("database", ["lookup_accounts", [cache.account_name, 1]]);
        return ret[0][1];
    }

    /**
     * Pings chain and checks response time and ID.
     * Uses: storage.mean_ping
     */
    async rpcPingLatency(storage, expectedChainId) {
        const start = Date.now() / 1000;
        const chainId = await this.query("database", ["get_chain_id", []]);
        const latency = Math.min(9.999, (Date.now() / 1000) - start);
        const max = Math.min(2, 2 * storage.mean_ping);
        if (chainId !== expectedChainId) throw new Error("chain_id != ID");
        if (latency > max) throw new Error("slow ping", latency);
        return [latency, max];
    }

    /**
     * Retrieves current order book data up to specified depth.
     * Uses: cache.currency, cache.asset, cache.asset_precision
     */
    async rpcBook(cache, depth = 3) {
        const orderBook = await this.query("database", ["get_order_book", [cache.currency, cache.asset, depth]]);
        const askp = [],
            bidp = [],
            askv = [],
            bidv = [];
        for (const ask of orderBook.asks) {
            const price = this.precision(ask.price, 16);
            if (parseFloat(price) === 0) throw new Error("zero price in asks");
            const volume = this.precision(ask.quote, cache.asset_precision);
            askp.push(price);
            askv.push(volume);
        }
        for (const bid of orderBook.bids) {
            const price = this.precision(bid.price, 16);
            if (parseFloat(price) === 0) throw new Error("zero price in bids");
            const volume = this.precision(bid.quote, cache.asset_precision);
            bidp.push(price);
            bidv.push(volume);
        }
        if (parseFloat(bidp[0]) >= parseFloat(askp[0])) throw new Error("mismatched orderbook");
        return [askp, bidp, askv, bidv];
    }

    /**
     * Retrieves and processes open limit orders.
     * Uses: cache.account_name, cache.currency_id, cache.asset_id, 
     *       cache.currency_precision, cache.asset_precision, cache.pair
     */
    async rpcOpenOrders(cache) {
        const ret = await this.query("database", ["get_full_accounts", [
            [cache.account_name], false
        ]]);
        const limitOrders = (ret[0][1] && ret[0][1].limit_orders) || [];
        const orders = [];

        for (const order of limitOrders) {
            const baseId = order.sell_price.base.asset_id;
            const quoteId = order.sell_price.quote.asset_id;
            if ([cache.currency_id, cache.asset_id].includes(baseId) && [cache.currency_id, cache.asset_id].includes(quoteId)) {
                let amount = parseFloat(order.for_sale);
                let baseAmount = parseFloat(order.sell_price.base.amount);
                let quoteAmount = parseFloat(order.sell_price.quote.amount);

                const basePrecision = baseId === cache.currency_id ? cache.currency_precision : cache.asset_precision;
                const quotePrecision = baseId === cache.currency_id ? cache.asset_precision : cache.currency_precision;

                baseAmount /= Math.pow(10, basePrecision);
                quoteAmount /= Math.pow(10, quotePrecision);

                let orderType, price;
                if (baseId === cache.asset_id) {
                    orderType = "sell";
                    price = quoteAmount / baseAmount;
                    amount = amount / Math.pow(10, basePrecision);
                } else {
                    orderType = "buy";
                    price = baseAmount / quoteAmount;
                    amount = (amount / Math.pow(10, basePrecision)) / price;
                }

                orders.push({
                    orderNumber: order.id,
                    orderType,
                    market: cache.pair,
                    amount: this.precision(amount, cache.asset_precision),
                    price: this.precision(price, 16),
                });
            }
        }
        return orders.sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
    }

    /**
     * Retrieves the latest market price.
     * Uses: cache.currency, cache.asset
     */
    async rpcLast(cache) {
        const ticker = await this.query("database", ["get_ticker", [cache.currency, cache.asset, false]]);
        const last = this.precision(ticker.latest, 16);
        if (parseFloat(last) === 0) throw new Error("zero price last");
        return last;
    }
}


/*
 * Manages multiple instances of GrapheneRPC:
 * - Exposes the same API as a single GrapheneRPC instance
 * - Routes calls to a randomly selected child instance
 * - On failure or timeout, retries a different instance
 * - Includes a configurable maximum retry count and timeout per call
 */
class GrapheneRPCPool {
    /**
     * @param {GrapheneRPC[]} instances - Array of GrapheneRPC instances
     * @param {number} [maxRetries=3] - Maximum number of retries per method call
     * @param {number} [timeoutMs=3000] - Timeout per individual request in milliseconds
     */
    constructor(instances, maxRetries = 3, timeoutMs = 3000) {
        if (!Array.isArray(instances) || instances.length === 0) {
            throw new Error("GrapheneRPCPool requires a non-empty array of GrapheneRPC instances.");
        }

        this.instances = instances;
        this.maxRetries = maxRetries;
        this.timeoutMs = timeoutMs;

        return new Proxy(this, {
            get: (target, prop, receiver) => {
                if (typeof target[prop] !== "undefined") return Reflect.get(target, prop, receiver);

                // Create a wrapper for instance method calls
                return async(...args) => {
                    return await target._callWithFailover(prop, args);
                };
            }
        });
    }

    /**
     * Internal method to call a random instance with retries and timeout
     * @param {string} method - Method name to call
     * @param {Array} args - Arguments to pass to the method
     */
    async _callWithFailover(method, args) {
        const tried = new Set();
        let lastError;

        for (let i = 0; i < this.maxRetries; i++) {
            let candidates = this.instances.filter((_, index) => !tried.has(index));
            if (candidates.length === 0) break;

            // Prefer instances whose WebSocket handshake completed, so
            // requests aren't queued on nodes that never connected.
            const healthy = candidates.filter((instance) => instance.connected);
            if (healthy.length > 0) candidates = healthy;

            const index = Math.floor(Math.random() * candidates.length);
            const instance = candidates[index];
            tried.add(this.instances.indexOf(instance));

            try {
                return await this._withTimeout(instance[method](...args));
            } catch (err) {
                lastError = err;
            }
        }

        throw new Error(`All RPC instances failed: ${lastError?.message || lastError}`);
    }

    /**
     * Wraps a promise with a timeout
     * @param {Promise} promise
     * @returns {Promise}
     */
    _withTimeout(promise) {
        return Promise.race([
            promise,
            new Promise((_, reject) => setTimeout(() => reject(new Error("RPC call timeout")), this.timeoutMs))
        ]);
    }
}
