/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Orders;
using QuantConnect.Scheduling;
using QuantConnect.Brokerages;
using QuantConnect.Securities;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Live performance benchmark for the Tastytrade brokerage order API.
    ///
    /// The algorithm runs one benchmark "cell" at a time. A cell is a single
    /// (SecurityType, OrderType) pair (for example Equity + Market, or Equity Option + Limit).
    /// For each selected cell it resolves a tradable instrument and fires a wave of orders on it,
    /// timing the round trip of every order from the Tastytrade node in QuantConnect Cloud.
    ///
    /// Orders inside a wave are placed ONE AT A TIME, alternating BUY/SELL: the next order is not
    /// placed until the previous one reaches a terminal status. This keeps the net position within a
    /// single unit at all times (no transient naked exposure), and it matches how the plugin already
    /// serializes submissions (each order blocks on the websocket ack; there is no client rate gate).
    ///
    /// Per order it records:
    ///   * submit latency:        local place call -> Submitted event (broker accepted it);
    ///   * fill latency:          local place call -> Filled event (full round trip);
    ///   * market exec time:      Submitted -> Filled;
    ///   * limit resolution time: Submitted -> terminal (Filled or Canceled);
    ///   * slippage:              signed per side, in basis points, against the quote at that order's place time.
    /// Latency is summarized as percentiles (p50/p95/p99), never an average. Every order is also written
    /// to a CSV file in the Object Store for offline analysis.
    ///
    /// This benchmark is Tastytrade-only by design; it is not meant to be reused for other brokerages.
    ///
    /// Parameters (set them in the cloud project, all optional):
    ///   security-types          comma list or "All": Equity, EquityOption, IndexOption, Future, FutureOption   (default Equity)
    ///   order-types             comma list or "All": Market, Limit, StopMarket, StopLimit, ComboLimit           (default Market)
    ///   orders-per-wave         orders placed per cell wave                                                     (default 10)
    ///   waves-per-cell          how many waves to run per cell                                                  (default 1)
    ///   order-quantity          shares/contracts per order                                                      (default 1)
    ///   marketable-buffer-pct   percent past the touch for a marketable limit (1 = 1%)                          (default 1)
    ///   stop-offset-pct         percent away from the touch for a resting stop trigger (2 = 2%)                 (default 2)
    ///   resting-order-ttl-sec   cancel an order still open after this many seconds (resting stops)             (default 4)
    ///   max-order-wait-sec      hard watchdog: force an order to resolve after this many seconds               (default 15)
    ///   seconds-between-waves   cooldown between waves                                                          (default 10)
    ///   flatten-after-wave      liquidate after each wave                                                       (default true)
    ///   equity-ticker           equity cell instrument                                                          (default AAPL)
    ///   option-underlying       equity-option underlying                                                        (default SPY)
    ///   index-ticker            index-option underlying index                                                   (default VIX)
    ///   index-option-target     index-option target root (weekly alias, e.g. VIXW / SPXW)                       (default VIXW)
    ///   future-ticker           future / future-option root                                                    (default MES)
    ///   resolve-timeout-sec     skip a cell if its instrument is not ready within this many seconds            (default 120)
    ///   csv-object-store-key    Object Store key for the per-order CSV                                          (default tastytrade-benchmark-orders.csv)
    /// </summary>
    public class TastytradePerformanceBenchmarkAlgorithm : QCAlgorithm
    {
        private const string CsvHeader =
            "session,security_type,order_type,wave,order_id,combo_group,symbol,side," +
            "t_place_ms,t_submitted_ms,t_filled_ms,t_terminal_ms," +
            "submit_latency_ms,fill_latency_ms,market_exec_ms,resolution_ms," +
            "anchor_bid,anchor_ask,arrival_price,fill_price,slippage_price,slippage_bps,final_status";

        // High resolution monotonic clock shared by the placing thread and the order-event thread.
        private readonly Stopwatch _clock = Stopwatch.StartNew();

        // Guards the timing state below, which is touched from the state machine (place) and OnOrderEvent (events).
        private readonly object _sync = new();

        private readonly List<BenchmarkCell> _cells = new();
        private readonly HashSet<SecurityType> _selectedSecurityTypes = new();

        // Resolved option/future contracts, keyed by SecurityType. Only ever written from OnData (single thread).
        private readonly Dictionary<SecurityType, ResolvedInstrument> _resolved = new();

        // Per-order timing keyed by Lean order id.
        private readonly Dictionary<int, OrderTiming> _orders = new();

        // Order ids of the single order (or combo legs) currently in flight; the wave waits for these to resolve.
        private readonly List<int> _pendingOrderIds = new();

        // Completed waves, kept for the grand summary at the end.
        private readonly List<WaveResult> _results = new();

        // One CSV line per order, flushed to the Object Store at the end of the run.
        private readonly List<string> _csvRows = new();

        // Configuration.
        private int _ordersPerWave;
        private int _wavesPerCell;
        private int _orderQuantity;
        private decimal _marketableBuffer;
        private decimal _stopOffset;
        private TimeSpan _restingTtl;
        private TimeSpan _maxOrderWait;
        private TimeSpan _resolveTimeout;
        private TimeSpan _waveCooldown;
        private bool _flattenAfterWave;
        private string _csvKey;
        private string _equityTicker;
        private string _optionUnderlying;
        private string _indexTicker;
        private string _indexOptionTarget;
        private string _futureTicker;

        private Symbol _equitySymbol;

        // Run state.
        private WaveState _currentWave;
        private bool _waveActive;
        private bool _flattenRequested;
        private bool _finalFlattenRequested;
        private TimeSpan _flattenDeadlineElapsed;
        private int _ordersPlacedInWave;
        private bool _pendingCancelIssued;
        private TimeSpan _pendingCancelDueElapsed;
        private TimeSpan _pendingHardDeadlineElapsed;
        private int _cellIndex;
        private int _cellWavesDone;
        private int _wavesDone;
        private DateTime _nextWaveTimeUtc = DateTime.MinValue;
        private DateTime _cellDeadlineUtc = DateTime.MinValue;
        private bool _finalReported;

        public override void Initialize()
        {
            // Dates only matter for a local backtest dry run; a live deploy ignores them.
            SetStartDate(2024, 1, 1);
            SetCash(100000);

            // Force the Tastytrade fee and fill model so a local dry run behaves like the live broker.
            SetBrokerageModel(BrokerageName.Tastytrade, AccountType.Margin);

            // Orders placed per cell wave.
            _ordersPerWave = GetParameter("orders-per-wave", 50;
            // How many waves to run per cell.
            _wavesPerCell = GetParameter("waves-per-cell", 1);
            // Shares or contracts per order.
            _orderQuantity = GetParameter("order-quantity", 1);
            // Percent past the touch price for a marketable limit (1 = 1%).
            _marketableBuffer = ParsePercentParameter("marketable-buffer-pct", 1m);
            // Percent away from the touch price for a resting stop trigger (2 = 2%).
            _stopOffset = ParsePercentParameter("stop-offset-pct", 2m);
            // Cancel an order still open after this many seconds (resting stops).
            _restingTtl = TimeSpan.FromSeconds(GetParameter("resting-order-ttl-sec", 4));
            // Hard watchdog: force an order to resolve after this many seconds.
            _maxOrderWait = TimeSpan.FromSeconds(GetParameter("max-order-wait-sec", 15));
            // Skip a cell if its instrument is not ready within this many seconds.
            _resolveTimeout = TimeSpan.FromSeconds(GetParameter("resolve-timeout-sec", 120));
            // Cooldown between waves.
            _waveCooldown = TimeSpan.FromSeconds(GetParameter("seconds-between-waves", 10));
            // Liquidate after each wave.
            _flattenAfterWave = GetParameter("flatten-after-wave", "true")
                .Equals("true", StringComparison.OrdinalIgnoreCase);
            // Object Store key for the per-order CSV.
            _csvKey = GetParameter("csv-object-store-key", "tastytrade-benchmark-orders.csv");
            // Equity cell instrument.
            _equityTicker = GetParameter("equity-ticker", "AAPL");
            // Equity-option underlying.
            _optionUnderlying = GetParameter("option-underlying", "SPY");
            // Index-option underlying index.
            _indexTicker = GetParameter("index-ticker", "VIX");
            // Index-option target root (weekly alias, e.g. VIXW / SPXW).
            _indexOptionTarget = GetParameter("index-option-target", "VIXW");
            // Future and future-option root.
            _futureTicker = GetParameter("future-ticker", Futures.Indices.MicroSP500EMini);

            // Security types to benchmark (comma list or "All").
            var securityTypes = ParseSecurityTypes(GetParameter("security-types", "ALL"));
            // Order types to benchmark (comma list or "All").
            var orderTypes = ParseOrderTypes(GetParameter("order-types", "Market"));
            BuildCells(securityTypes, orderTypes);
            SubscribeSecurities();

            // A steady heartbeat drives the state machine even if a thin instrument stops delivering data,
            // so the run can never stall waiting for the next slice.
            Schedule.On(DateRules.EveryDay(), TimeRules.Every(TimeSpan.FromSeconds(1)), Step);

            Log($"[Benchmark] Tastytrade order API benchmark. cells={_cells.Count} " +
                $"ordersPerWave={_ordersPerWave} wavesPerCell={_wavesPerCell} orderQuantity={_orderQuantity} " +
                $"marketableBuffer={_marketableBuffer:P2} stopOffset={_stopOffset:P2} restingTtl={_restingTtl.TotalSeconds}s " +
                $"maxOrderWait={_maxOrderWait.TotalSeconds}s cooldown={_waveCooldown.TotalSeconds}s " +
                $"flattenAfterWave={_flattenAfterWave} liveMode={LiveMode}");
            Log($"[Benchmark] cell plan: {string.Join(", ", _cells.Select(c => c.ToString()))}");

            if (!LiveMode)
            {
                Log("[Benchmark] WARNING: latency numbers are only meaningful in live mode; " +
                    "a backtest fills synchronously and will report near-zero times.");
            }
        }

        public override void OnData(Slice slice)
        {
            // Resolve option/future contracts from the chains as soon as data arrives, then step the state machine.
            TryResolveInstruments(slice);
            Step();
        }

        private void Step()
        {
            lock (_sync)
            {
                if (_finalReported)
                {
                    return;
                }

                // Everything done: flatten once, print the grand summary, export the CSV, stop.
                if (_cellIndex >= _cells.Count)
                {
                    if (!TryFlatten(ref _finalFlattenRequested))
                    {
                        return;
                    }
                    ReportGrandTotal();
                    ExportCsv();
                    _finalReported = true;
                    Quit("Benchmark complete");
                    return;
                }

                // A wave is in flight: place the next order once the previous one resolves, or finish the wave.
                if (_waveActive)
                {
                    StepActiveWave();
                    return;
                }

                // Between waves: flatten the position from the previous wave, then honor the cooldown.
                if (_wavesDone > 0)
                {
                    if (!TryFlatten(ref _flattenRequested))
                    {
                        return;
                    }

                    if (UtcTime < _nextWaveTimeUtc)
                    {
                        return;
                    }
                }

                var cell = _cells[_cellIndex];

                if (_cellDeadlineUtc == DateTime.MinValue)
                {
                    _cellDeadlineUtc = UtcTime.Add(_resolveTimeout);
                }

                // A combo needs a second option leg; if we resolved the instrument but could not find one, skip the cell.
                if (cell.OrderType == OrderType.ComboLimit && IsResolved(cell.SecurityType) && GetSecondaryLeg(cell.SecurityType) == null)
                {
                    Log($"[Benchmark] {cell}: no second option leg available, skipping this cell.");
                    AdvanceCell();
                    return;
                }

                if (!IsCellReady(cell))
                {
                    if (UtcTime > _cellDeadlineUtc)
                    {
                        Log($"[Benchmark] {cell}: instrument not ready within {_resolveTimeout.TotalSeconds:0}s, skipping this cell.");
                        AdvanceCell();
                    }
                    return;
                }

                StartCellWave(cell);
            }
        }

        private void StepActiveWave()
        {
            var now = _clock.Elapsed;

            if (HasPendingOpenOrders())
            {
                // Cancel a still-open order after its TTL so resting stops (and any stuck order) resolve.
                if (!_pendingCancelIssued && now >= _pendingCancelDueElapsed)
                {
                    CancelPendingOrders();
                    _pendingCancelIssued = true;
                }

                // Hard watchdog: if an order still has not resolved, force it so the run always advances.
                if (now >= _pendingHardDeadlineElapsed)
                {
                    ForceResolvePendingOrders();
                }
                else
                {
                    return;
                }
            }

            // The pending order resolved: place the next one, or finish the wave.
            if (_ordersPlacedInWave < _ordersPerWave)
            {
                PlaceNextInWave();
                return;
            }

            CompleteWave(now);
        }

        private void StartCellWave(BenchmarkCell cell)
        {
            _currentWave = new WaveState
            {
                Number = _wavesDone + 1,
                Cell = cell,
                Session = SessionBucket(),
                StartElapsed = _clock.Elapsed
            };
            _ordersPlacedInWave = 0;
            _pendingOrderIds.Clear();
            _flattenRequested = false;
            _waveActive = true;

            Log($"[Benchmark] Wave {_currentWave.Number} ({cell}) [{_currentWave.Session}]: starting " +
                $"{_ordersPerWave} orders on {GetPrimarySymbol(cell.SecurityType).Value}.");

            if (_ordersPerWave <= 0)
            {
                CompleteWave(_clock.Elapsed);
                return;
            }

            PlaceNextInWave();
        }

        private void PlaceNextInWave()
        {
            var index = _ordersPlacedInWave;

            // Alternate BUY/SELL so the net position never leaves the range [-1, +1] unit.
            var side = (index % 2 == 0) ? 1 : -1;
            var cell = _currentWave.Cell;
            var primary = GetPrimarySymbol(cell.SecurityType);

            _pendingOrderIds.Clear();
            if (cell.OrderType == OrderType.ComboLimit)
            {
                PlaceComboOrder(cell, primary, GetSecondaryLeg(cell.SecurityType), side, index + 1);
            }
            else
            {
                PlaceSingleOrder(cell, primary, Securities[primary], side, index + 1);
            }

            if (_pendingOrderIds.Count == 0)
            {
                Log($"[Benchmark] {cell} order {index + 1}: placement recorded no order, skipping.");
            }

            _ordersPlacedInWave++;
            _pendingCancelIssued = false;
            _pendingCancelDueElapsed = _clock.Elapsed + _restingTtl;
            _pendingHardDeadlineElapsed = _clock.Elapsed + _maxOrderWait;
        }

        private void PlaceSingleOrder(BenchmarkCell cell, Symbol symbol, Security security, int side, int index)
        {
            var signedQuantity = side * _orderQuantity;
            var bid = security.BidPrice;
            var ask = security.AskPrice;
            var arrivalPrice = ArrivalPrice(side, bid, ask, security);
            var placeElapsed = _clock.Elapsed;

            OrderTicket ticket;
            switch (cell.OrderType)
            {
                case OrderType.Market:
                    ticket = MarketOrder(symbol, signedQuantity, asynchronous: true, tag: Tag(cell, index));
                    break;

                case OrderType.Limit:
                    var limitPrice = MarketableLimitPrice(side, bid, ask, security);
                    ticket = LimitOrder(symbol, signedQuantity, limitPrice, asynchronous: true, tag: Tag(cell, index));
                    break;

                case OrderType.StopMarket:
                    var stopPrice = RestingStopPrice(side, bid, ask, security);
                    ticket = StopMarketOrder(symbol, signedQuantity, stopPrice, asynchronous: true, tag: Tag(cell, index));
                    break;

                case OrderType.StopLimit:
                    var stopTrigger = RestingStopPrice(side, bid, ask, security);
                    var stopLimit = side > 0
                        ? RoundToTick(stopTrigger * (1 + _marketableBuffer), security)
                        : RoundToTick(stopTrigger * (1 - _marketableBuffer), security);
                    ticket = StopLimitOrder(symbol, signedQuantity, stopTrigger, stopLimit, asynchronous: true, tag: Tag(cell, index));
                    break;

                default:
                    return;
            }

            RecordOrder(ticket.OrderId, cell, symbol, side, 1, placeElapsed, bid, ask, arrivalPrice, 0);
        }

        private void PlaceComboOrder(BenchmarkCell cell, Symbol nearLeg, Symbol farLeg, int side, int index)
        {
            // Vertical spread: long the near leg, short the far leg (ratio 1 / -1). The group quantity carries the
            // direction. Leg ratios stay 1/-1 for both buy and sell so the spread structure is the same either way.
            if (!Securities.TryGetValue(nearLeg, out var nearSecurity) || !Securities.TryGetValue(farLeg, out var farSecurity))
            {
                Log($"[Benchmark] {cell}: combo legs not subscribed, skipping this order.");
                return;
            }

            var nearAsk = nearSecurity.AskPrice > 0 ? nearSecurity.AskPrice : nearSecurity.Price;
            var nearBid = nearSecurity.BidPrice > 0 ? nearSecurity.BidPrice : nearSecurity.Price;
            var farAsk = farSecurity.AskPrice > 0 ? farSecurity.AskPrice : farSecurity.Price;
            var farBid = farSecurity.BidPrice > 0 ? farSecurity.BidPrice : farSecurity.Price;

            // Side-correct natural net price of the spread: pay near-ask / receive far-bid to buy, the reverse to sell.
            // This is the slippage anchor and the basis for a marketable net limit; it can be a debit or a credit.
            var netArrival = side > 0 ? (nearAsk - farBid) : (nearBid - farAsk);
            var tick = nearSecurity.SymbolProperties.MinimumPriceVariation;
            var pad = _marketableBuffer * Math.Max(Math.Abs(netArrival), tick);
            var netLimit = RoundToTick(netArrival + side * pad, nearSecurity);
            if (Math.Abs(netLimit) < tick)
            {
                // ComboLimitOrder rejects a zero limit price; keep it on the marketable side.
                netLimit = side * tick;
            }

            var legs = new List<Leg>
            {
                Leg.Create(nearLeg, 1),
                Leg.Create(farLeg, -1)
            };

            var placeElapsed = _clock.Elapsed;
            var tickets = ComboLimitOrder(legs, side * _orderQuantity, netLimit, asynchronous: true, tag: Tag(cell, index));
            if (tickets == null || tickets.Count == 0)
            {
                return;
            }

            var groupId = tickets[0].OrderId;
            foreach (var ticket in tickets)
            {
                var legRatio = ticket.Symbol == nearLeg ? 1 : -1;
                var legBid = ticket.Symbol == nearLeg ? nearBid : farBid;
                var legAsk = ticket.Symbol == nearLeg ? nearAsk : farAsk;
                RecordOrder(ticket.OrderId, cell, ticket.Symbol, side, legRatio, placeElapsed, legBid, legAsk, netArrival, groupId);
            }
        }

        private void RecordOrder(int orderId, BenchmarkCell cell, Symbol symbol, int side, int legRatio,
            TimeSpan placeElapsed, decimal anchorBid, decimal anchorAsk, decimal arrivalPrice, int comboGroupId)
        {
            _orders[orderId] = new OrderTiming
            {
                OrderId = orderId,
                WaveNumber = _currentWave.Number,
                Cell = cell,
                Session = _currentWave.Session,
                Symbol = symbol,
                Side = side,
                LegRatio = legRatio,
                SubmitCall = placeElapsed,
                AnchorBid = anchorBid,
                AnchorAsk = anchorAsk,
                ArrivalPrice = arrivalPrice,
                ComboGroupId = comboGroupId
            };
            _currentWave.OrderIds.Add(orderId);
            _pendingOrderIds.Add(orderId);
        }

        private bool HasPendingOpenOrders()
        {
            return _pendingOrderIds.Any(id => _orders.TryGetValue(id, out var timing) && !timing.IsTerminal);
        }

        private void CancelPendingOrders()
        {
            foreach (var id in _pendingOrderIds)
            {
                if (_orders.TryGetValue(id, out var timing) && !timing.IsTerminal)
                {
                    Transactions.CancelOrder(id, "benchmark ttl");
                }
            }
        }

        private void ForceResolvePendingOrders()
        {
            var now = _clock.Elapsed;
            foreach (var id in _pendingOrderIds)
            {
                if (_orders.TryGetValue(id, out var timing) && !timing.IsTerminal)
                {
                    // Best-effort cancel; the between-wave flatten cleans up any position a stuck order may have opened.
                    Transactions.CancelOrder(id, "benchmark watchdog");
                    timing.IsTerminal = true;
                    timing.TerminalAt = now;
                    timing.FinalStatus = OrderStatus.Canceled;
                }
            }
            Log($"[Benchmark] watchdog: forced {_currentWave.Cell} order to resolve after {_maxOrderWait.TotalSeconds:0}s.");
        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            // Capture the timestamp before taking the lock so lock contention never inflates the measurement.
            var now = _clock.Elapsed;

            lock (_sync)
            {
                // Orders we did not place (for example liquidation) are not part of the benchmark.
                if (!_orders.TryGetValue(orderEvent.OrderId, out var timing))
                {
                    return;
                }

                switch (orderEvent.Status)
                {
                    case OrderStatus.Submitted:
                        timing.SubmittedAt ??= now;
                        break;

                    case OrderStatus.Filled:
                        if (timing.FilledAt == null)
                        {
                            timing.FilledAt = now;
                            timing.FillPrice = orderEvent.FillPrice;
                        }
                        break;
                }

                if (IsTerminal(orderEvent.Status) && !timing.IsTerminal)
                {
                    timing.IsTerminal = true;
                    timing.TerminalAt = now;
                    timing.FinalStatus = orderEvent.Status;

                    // A fill can arrive as the terminal event without a prior Filled branch; capture it here too.
                    if (orderEvent.Status == OrderStatus.Filled && timing.FilledAt == null)
                    {
                        timing.FilledAt = now;
                        timing.FillPrice = orderEvent.FillPrice;
                    }
                }
            }
        }

        private void CompleteWave(TimeSpan completedElapsed)
        {
            var cell = _currentWave.Cell;
            var timings = _currentWave.OrderIds.Select(id => _orders[id]).ToList();

            var submitLatencies = timings
                .Where(t => t.SubmittedAt != null)
                .Select(t => (t.SubmittedAt.Value - t.SubmitCall).TotalMilliseconds)
                .ToList();

            var fillLatencies = timings
                .Where(t => t.FilledAt != null)
                .Select(t => (t.FilledAt.Value - t.SubmitCall).TotalMilliseconds)
                .ToList();

            var slippages = ComputeSlippageBps(cell, timings);

            var filled = timings.Count(t => t.FinalStatus == OrderStatus.Filled);
            var canceled = timings.Count(t => t.FinalStatus == OrderStatus.Canceled);
            var invalid = timings.Count(t => t.FinalStatus == OrderStatus.Invalid);

            var submitWallMs = (timings.Where(t => t.SubmittedAt != null)
                .Select(t => t.SubmittedAt.Value).DefaultIfEmpty(_currentWave.StartElapsed).Max()
                - _currentWave.StartElapsed).TotalMilliseconds;
            var fillWallMs = (completedElapsed - _currentWave.StartElapsed).TotalMilliseconds;

            var result = new WaveResult
            {
                Number = _currentWave.Number,
                Cell = cell,
                Session = _currentWave.Session,
                OrderCount = _currentWave.OrderIds.Count,
                Filled = filled,
                Canceled = canceled,
                Invalid = invalid,
                SubmitWallMs = submitWallMs,
                FillWallMs = fillWallMs,
                SubmitLatencies = submitLatencies,
                FillLatencies = fillLatencies,
                SlippagesBps = slippages
            };
            _results.Add(result);

            AppendCsvRows(timings);
            LogWaveReport(result);

            _wavesDone++;
            _cellWavesDone++;
            if (_cellWavesDone >= _wavesPerCell)
            {
                _cellIndex++;
                _cellWavesDone = 0;
            }
            _waveActive = false;
            _pendingOrderIds.Clear();
            _ordersPlacedInWave = 0;
            _cellDeadlineUtc = DateTime.MinValue;
            _nextWaveTimeUtc = UtcTime.Add(_waveCooldown);
            _currentWave = null;
        }

        private List<double> ComputeSlippageBps(BenchmarkCell cell, List<OrderTiming> timings)
        {
            // Single-leg orders: slippage per order against that order's own arrival quote.
            if (cell.OrderType != OrderType.ComboLimit)
            {
                return timings
                    .Where(t => t.FillPrice != null && t.ArrivalPrice != 0)
                    .Select(t => (double)(t.Side * (t.FillPrice.Value - t.ArrivalPrice) / t.ArrivalPrice) * 10000.0)
                    .ToList();
            }

            // Combos: slippage on the net filled spread price vs the side-correct net arrival, one value per group.
            var comboSlippages = new List<double>();
            foreach (var group in timings.GroupBy(t => t.ComboGroupId))
            {
                var legs = group.ToList();
                if (legs.Any(t => t.FillPrice == null))
                {
                    continue;
                }

                var netFill = legs.Sum(t => t.LegRatio * (double)t.FillPrice.Value);
                var netArrival = (double)legs.First().ArrivalPrice;
                if (netArrival == 0)
                {
                    continue;
                }

                // Divide by the magnitude so "worse than arrival" stays positive for a debit or a credit spread.
                var side = legs.First().Side;
                comboSlippages.Add(side * (netFill - netArrival) / Math.Abs(netArrival) * 10000.0);
            }
            return comboSlippages;
        }

        private void AppendCsvRows(List<OrderTiming> timings)
        {
            foreach (var t in timings)
            {
                var submitMs = t.SubmittedAt != null ? (t.SubmittedAt.Value - t.SubmitCall).TotalMilliseconds : (double?)null;
                var fillMs = t.FilledAt != null ? (t.FilledAt.Value - t.SubmitCall).TotalMilliseconds : (double?)null;
                var execMs = (t.SubmittedAt != null && t.FilledAt != null)
                    ? (t.FilledAt.Value - t.SubmittedAt.Value).TotalMilliseconds : (double?)null;
                var resolutionMs = (t.SubmittedAt != null && t.TerminalAt != null)
                    ? (t.TerminalAt.Value - t.SubmittedAt.Value).TotalMilliseconds : (double?)null;

                decimal? slippagePrice = null;
                decimal? slippageBps = null;
                if (t.ComboGroupId == 0 && t.FillPrice != null && t.ArrivalPrice != 0)
                {
                    slippagePrice = t.Side * (t.FillPrice.Value - t.ArrivalPrice);
                    slippageBps = slippagePrice.Value / t.ArrivalPrice * 10000m;
                }

                _csvRows.Add(string.Join(",",
                    t.Session,
                    t.Cell.SecurityType,
                    t.Cell.OrderType,
                    t.WaveNumber.ToString(CultureInfo.InvariantCulture),
                    t.OrderId.ToString(CultureInfo.InvariantCulture),
                    t.ComboGroupId.ToString(CultureInfo.InvariantCulture),
                    t.Symbol.Value,
                    t.Side.ToString(CultureInfo.InvariantCulture),
                    Csv(t.SubmitCall.TotalMilliseconds),
                    Csv(t.SubmittedAt?.TotalMilliseconds),
                    Csv(t.FilledAt?.TotalMilliseconds),
                    Csv(t.TerminalAt?.TotalMilliseconds),
                    Csv(submitMs),
                    Csv(fillMs),
                    Csv(execMs),
                    Csv(resolutionMs),
                    Csv(t.AnchorBid),
                    Csv(t.AnchorAsk),
                    Csv(t.ArrivalPrice),
                    Csv(t.FillPrice),
                    Csv(slippagePrice),
                    Csv(slippageBps),
                    t.FinalStatus.ToString()));
            }
        }

        private void LogWaveReport(WaveResult r)
        {
            var throughput = r.FillWallMs > 0 ? r.OrderCount / (r.FillWallMs / 1000.0) : 0;

            Log($"[Benchmark] ===== Wave {r.Number} ({r.Cell}) [{r.Session}] results =====");
            Log($"[Benchmark] orders={r.OrderCount} filled={r.Filled} canceled={r.Canceled} invalid={r.Invalid}");
            Log($"[Benchmark] wall time: all submitted in {r.SubmitWallMs:0} ms, all resolved in {r.FillWallMs:0} ms " +
                $"({throughput:0.0} orders/sec, end-to-end serialized)");
            Log($"[Benchmark] submit latency ms: {FormatStats(r.SubmitLatencies)}");
            Log($"[Benchmark] fill latency ms:   {FormatStats(r.FillLatencies)}");
            if (r.SlippagesBps.Count > 0)
            {
                Log($"[Benchmark] slippage bps:      {FormatStats(r.SlippagesBps)}");
            }
        }

        private void ReportGrandTotal()
        {
            if (_results.Count == 0)
            {
                Log("[Benchmark] No waves completed; nothing to summarize.");
                return;
            }

            Log("[Benchmark] ========== GRAND SUMMARY ==========");
            var totalOrders = _results.Sum(r => r.OrderCount);
            var totalFilled = _results.Sum(r => r.Filled);
            var totalCanceled = _results.Sum(r => r.Canceled);
            var totalInvalid = _results.Sum(r => r.Invalid);
            Log($"[Benchmark] waves={_results.Count} orders={totalOrders} filled={totalFilled} " +
                $"canceled={totalCanceled} invalid={totalInvalid}");

            // Per (SecurityType, OrderType) cell breakdown.
            foreach (var group in _results.GroupBy(r => r.Cell.ToString()).OrderBy(g => g.Key))
            {
                var submit = group.SelectMany(r => r.SubmitLatencies).ToList();
                var fill = group.SelectMany(r => r.FillLatencies).ToList();
                var slip = group.SelectMany(r => r.SlippagesBps).ToList();
                Log($"[Benchmark] [{group.Key}] submit ms: {FormatStats(submit)}");
                Log($"[Benchmark] [{group.Key}] fill ms:   {FormatStats(fill)}");
                if (slip.Count > 0)
                {
                    Log($"[Benchmark] [{group.Key}] slip bps:  {FormatStats(slip)}");
                }
            }

            // Volume summary for the renewal framing.
            Log($"[Benchmark] volume: {totalOrders} orders, {totalFilled} filled across " +
                $"{_results.Select(r => r.Cell.SecurityType).Distinct().Count()} security types.");
        }

        private void ExportCsv()
        {
            var builder = new StringBuilder();
            builder.AppendLine(CsvHeader);
            foreach (var row in _csvRows)
            {
                builder.AppendLine(row);
            }

            try
            {
                ObjectStore.Save(_csvKey, builder.ToString());
                Log($"[Benchmark] wrote {_csvRows.Count} order rows to Object Store key '{_csvKey}'.");
            }
            catch (Exception exception)
            {
                Log($"[Benchmark] failed to save CSV to Object Store: {exception.Message}");
            }
        }

        private void TryResolveInstruments(Slice slice)
        {
            foreach (var kvp in slice.OptionChains)
            {
                var securityType = kvp.Key.SecurityType;
                if (!_selectedSecurityTypes.Contains(securityType) || IsResolved(securityType))
                {
                    continue;
                }
                ResolveOptionCell(securityType, kvp.Value);
            }

            if (_selectedSecurityTypes.Contains(SecurityType.Future) && !IsResolved(SecurityType.Future))
            {
                foreach (var kvp in slice.FutureChains)
                {
                    ResolveFutureCell(kvp.Value);
                    if (IsResolved(SecurityType.Future))
                    {
                        break;
                    }
                }
            }
        }

        private void ResolveOptionCell(SecurityType securityType, Data.Market.OptionChain chain)
        {
            if (chain.Underlying == null)
            {
                return;
            }
            var underlyingPrice = chain.Underlying.Price;

            // Only contracts that already have a live two-sided quote can be priced and closed.
            var quoted = chain.Where(c => c.AskPrice > 0 && c.BidPrice > 0).ToList();
            if (quoted.Count == 0)
            {
                return;
            }

            var nearestExpiry = quoted.Min(c => c.Expiry);
            var sameExpiry = quoted
                .Where(c => c.Expiry == nearestExpiry)
                .OrderBy(c => Math.Abs(underlyingPrice - c.Strike))
                .ToList();

            var atm = sameExpiry.First();

            // Second leg for a vertical combo: the next strike up in the same right and expiry.
            Symbol secondary = null;
            var sameRight = sameExpiry
                .Where(c => c.Right == atm.Right)
                .OrderBy(c => c.Strike)
                .ToList();
            var atmPosition = sameRight.FindIndex(c => c.Symbol == atm.Symbol);
            if (atmPosition >= 0 && atmPosition + 1 < sameRight.Count)
            {
                secondary = sameRight[atmPosition + 1].Symbol;
            }

            _resolved[securityType] = new ResolvedInstrument
            {
                Primary = atm.Symbol,
                Secondary = secondary,
                IsResolved = true
            };
            Log($"[Benchmark] resolved {securityType}: {atm.Symbol.Value}" +
                (secondary != null ? $" (+ combo leg {secondary.Value})" : " (no combo leg)"));
        }

        private void ResolveFutureCell(Data.Market.FuturesChain chain)
        {
            var front = chain
                .Where(c => c.Expiry > Time.Date)
                .OrderBy(c => c.Expiry)
                .FirstOrDefault();
            if (front == null)
            {
                return;
            }

            _resolved[SecurityType.Future] = new ResolvedInstrument
            {
                Primary = front.Symbol,
                Secondary = null,
                IsResolved = true
            };
            Log($"[Benchmark] resolved Future front month: {front.Symbol.Value}");
        }

        private bool IsCellReady(BenchmarkCell cell)
        {
            var primary = GetPrimarySymbol(cell.SecurityType);
            if (primary == null)
            {
                return false;
            }
            if (!Securities.ContainsKey(primary))
            {
                return false;
            }
            if (!IsMarketOpen(primary))
            {
                return false;
            }

            var security = Securities[primary];
            if (security.Price == 0 && security.BidPrice == 0 && security.AskPrice == 0)
            {
                return false;
            }
            return true;
        }

        private void AdvanceCell()
        {
            _cellIndex++;
            _cellWavesDone = 0;
            _cellDeadlineUtc = DateTime.MinValue;
        }

        private bool TryFlatten(ref bool requested)
        {
            if (!_flattenAfterWave || !Portfolio.Invested)
            {
                return true;
            }
            if (!requested)
            {
                Liquidate(asynchronous: true, tag: "benchmark flatten");
                requested = true;
                _flattenDeadlineElapsed = _clock.Elapsed + _maxOrderWait;
                return false;
            }
            if (_clock.Elapsed >= _flattenDeadlineElapsed)
            {
                Log($"[Benchmark] flatten did not complete within {_maxOrderWait.TotalSeconds:0}s; " +
                    "canceling open orders and proceeding with a residual position.");
                Transactions.CancelOpenOrders();
                return true;
            }
            return false;
        }

        private void BuildCells(IEnumerable<SecurityType> securityTypes, IEnumerable<OrderType> orderTypes)
        {
            var types = orderTypes.ToList();
            foreach (var securityType in securityTypes)
            {
                foreach (var orderType in types)
                {
                    // ComboLimit is multi-leg and only valid for option security types.
                    if (orderType == OrderType.ComboLimit && !IsOptionType(securityType))
                    {
                        continue;
                    }
                    _cells.Add(new BenchmarkCell(securityType, orderType));
                    _selectedSecurityTypes.Add(securityType);
                }
            }
        }

        private void SubscribeSecurities()
        {
            if (_selectedSecurityTypes.Contains(SecurityType.Equity))
            {
                _equitySymbol = AddEquity(_equityTicker, Resolution.Second).Symbol;
            }

            if (_selectedSecurityTypes.Contains(SecurityType.Option))
            {
                // Raw normalization keeps the underlying price aligned with option strikes.
                var underlying = AddEquity(_optionUnderlying, Resolution.Second);
                underlying.SetDataNormalizationMode(DataNormalizationMode.Raw);
                var option = AddOption(underlying.Symbol, Resolution.Second);
                option.SetFilter(u => u.Strikes(-2, 2).Expiration(0, 7));
            }

            if (_selectedSecurityTypes.Contains(SecurityType.IndexOption))
            {
                var index = AddIndex(_indexTicker, Resolution.Second);
                var indexOption = AddIndexOption(index.Symbol, _indexOptionTarget, Resolution.Second);
                indexOption.SetFilter(u => u.Strikes(-2, 2).Expiration(0, 7));
            }

            var needsFuture = _selectedSecurityTypes.Contains(SecurityType.Future)
                || _selectedSecurityTypes.Contains(SecurityType.FutureOption);
            if (needsFuture)
            {
                var future = AddFuture(_futureTicker, Resolution.Second);
                future.SetFilter(0, 182);

                if (_selectedSecurityTypes.Contains(SecurityType.FutureOption))
                {
                    AddFutureOption(future.Symbol, u => u.Strikes(-2, 2).OnlyApplyFilterAtMarketOpen());
                }
            }
        }

        private decimal ArrivalPrice(int side, decimal bid, decimal ask, Security security)
        {
            // Touch anchor: buys pay the ask, sells hit the bid; fall back to last price if a side is missing.
            var touch = side > 0 ? ask : bid;
            if (touch <= 0)
            {
                touch = security.Price;
            }
            return touch;
        }

        private decimal MarketableLimitPrice(int side, decimal bid, decimal ask, Security security)
        {
            var touch = ArrivalPrice(side, bid, ask, security);
            var buffered = side > 0 ? touch * (1 + _marketableBuffer) : touch * (1 - _marketableBuffer);
            return RoundToTick(Math.Max(security.SymbolProperties.MinimumPriceVariation, buffered), security);
        }

        private decimal RestingStopPrice(int side, decimal bid, decimal ask, Security security)
        {
            // A stop must sit on the far side of the market so it rests instead of triggering: buy-stop above, sell-stop below.
            var reference = side > 0 ? ask : bid;
            if (reference <= 0)
            {
                reference = security.Price;
            }
            var stop = side > 0 ? reference * (1 + _stopOffset) : reference * (1 - _stopOffset);
            return RoundToTick(stop, security);
        }

        private decimal RoundToTick(decimal price, Security security)
        {
            var tick = security.SymbolProperties.MinimumPriceVariation;
            if (tick <= 0)
            {
                return Math.Round(price, 2);
            }
            return Math.Round(price / tick, MidpointRounding.AwayFromZero) * tick;
        }

        private string SessionBucket()
        {
            var timeOfDay = Time.TimeOfDay;
            if (timeOfDay >= new TimeSpan(9, 30, 0) && timeOfDay < new TimeSpan(10, 0, 0))
            {
                return "open";
            }
            if (timeOfDay >= new TimeSpan(12, 0, 0) && timeOfDay < new TimeSpan(13, 0, 0))
            {
                return "midday";
            }
            if (timeOfDay >= new TimeSpan(15, 30, 0) && timeOfDay <= new TimeSpan(16, 0, 0))
            {
                return "close";
            }
            return "other";
        }

        private string Tag(BenchmarkCell cell, int index)
        {
            return $"bench-{cell.SecurityType}-{cell.OrderType}-w{_currentWave.Number}-{index}";
        }

        private bool IsResolved(SecurityType securityType)
        {
            return _resolved.TryGetValue(securityType, out var resolved) && resolved.IsResolved;
        }

        private Symbol GetPrimarySymbol(SecurityType securityType)
        {
            if (securityType == SecurityType.Equity)
            {
                return _equitySymbol;
            }
            if (_resolved.TryGetValue(securityType, out var resolved) && resolved.IsResolved)
            {
                return resolved.Primary;
            }
            return null;
        }

        private Symbol GetSecondaryLeg(SecurityType securityType)
        {
            if (_resolved.TryGetValue(securityType, out var resolved))
            {
                return resolved.Secondary;
            }
            return null;
        }

        private static bool IsOptionType(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Option:
                case SecurityType.IndexOption:
                case SecurityType.FutureOption:
                    return true;
                default:
                    return false;
            }
        }

        private static bool IsTerminal(OrderStatus status)
        {
            return status == OrderStatus.Filled
                || status == OrderStatus.Canceled
                || status == OrderStatus.Invalid;
        }

        private List<SecurityType> ParseSecurityTypes(string raw)
        {
            var all = new[]
            {
                SecurityType.Equity,
                SecurityType.Option,
                SecurityType.IndexOption,
                SecurityType.Future,
                SecurityType.FutureOption
            };
            if (string.IsNullOrWhiteSpace(raw) || raw.Trim().Equals("All", StringComparison.OrdinalIgnoreCase))
            {
                return all.ToList();
            }

            var result = new List<SecurityType>();
            foreach (var token in raw.Split(','))
            {
                switch (token.Trim().ToLowerInvariant())
                {
                    case "equity":
                        result.Add(SecurityType.Equity);
                        break;
                    case "equityoption":
                    case "option":
                        result.Add(SecurityType.Option);
                        break;
                    case "indexoption":
                        result.Add(SecurityType.IndexOption);
                        break;
                    case "future":
                        result.Add(SecurityType.Future);
                        break;
                    case "futureoption":
                        result.Add(SecurityType.FutureOption);
                        break;
                    default:
                        Log($"[Benchmark] unknown security-type '{token}', ignored.");
                        break;
                }
            }
            return result;
        }

        private List<OrderType> ParseOrderTypes(string raw)
        {
            var all = new[]
            {
                OrderType.Market,
                OrderType.Limit,
                OrderType.StopMarket,
                OrderType.StopLimit,
                OrderType.ComboLimit
            };
            if (string.IsNullOrWhiteSpace(raw) || raw.Trim().Equals("All", StringComparison.OrdinalIgnoreCase))
            {
                return all.ToList();
            }

            var result = new List<OrderType>();
            foreach (var token in raw.Split(','))
            {
                switch (token.Trim().ToLowerInvariant())
                {
                    case "market":
                        result.Add(OrderType.Market);
                        break;
                    case "limit":
                        result.Add(OrderType.Limit);
                        break;
                    case "stopmarket":
                    case "stop":
                        result.Add(OrderType.StopMarket);
                        break;
                    case "stoplimit":
                        result.Add(OrderType.StopLimit);
                        break;
                    case "combolimit":
                    case "combo":
                        result.Add(OrderType.ComboLimit);
                        break;
                    default:
                        Log($"[Benchmark] unknown order-type '{token}', ignored.");
                        break;
                }
            }
            return result;
        }

        private decimal ParsePercentParameter(string name, decimal defaultPercent)
        {
            var raw = GetParameter(name, defaultPercent.ToString(CultureInfo.InvariantCulture));
            if (!decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var percent))
            {
                percent = defaultPercent;
            }
            return percent / 100m;
        }

        private static string Csv(double? value)
        {
            if (value == null)
            {
                return string.Empty;
            }
            return value.Value.ToString("0.####", CultureInfo.InvariantCulture);
        }

        private static string Csv(decimal? value)
        {
            if (value == null)
            {
                return string.Empty;
            }
            return value.Value.ToString("0.########", CultureInfo.InvariantCulture);
        }

        private static string FormatStats(List<double> values)
        {
            if (values == null || values.Count == 0)
            {
                return "n=0";
            }

            var sorted = values.OrderBy(v => v).ToList();
            return string.Format(CultureInfo.InvariantCulture,
                "n={0} p50={1:0.##} p95={2:0.##} p99={3:0.##} min={4:0.##} max={5:0.##}",
                sorted.Count, Percentile(sorted, 50), Percentile(sorted, 95), Percentile(sorted, 99),
                sorted.First(), sorted.Last());
        }

        private static double Percentile(List<double> sorted, double percentile)
        {
            if (sorted.Count == 0)
            {
                return 0;
            }
            if (sorted.Count == 1)
            {
                return sorted[0];
            }

            var rank = percentile / 100.0 * (sorted.Count - 1);
            var low = (int)Math.Floor(rank);
            var high = (int)Math.Ceiling(rank);
            var weight = rank - low;
            return sorted[low] * (1 - weight) + sorted[high] * weight;
        }

        private class BenchmarkCell
        {
            public SecurityType SecurityType { get; }
            public OrderType OrderType { get; }

            public BenchmarkCell(SecurityType securityType, OrderType orderType)
            {
                SecurityType = securityType;
                OrderType = orderType;
            }

            public override string ToString()
            {
                return $"{SecurityType}/{OrderType}";
            }
        }

        private class ResolvedInstrument
        {
            public Symbol Primary { get; init; }
            public Symbol Secondary { get; init; }
            public bool IsResolved { get; init; }
        }

        private class OrderTiming
        {
            public int OrderId { get; set; }
            public int WaveNumber;
            public BenchmarkCell Cell;
            public string Session;
            public Symbol Symbol;
            public int Side;
            public int LegRatio;
            public TimeSpan SubmitCall;
            public TimeSpan? SubmittedAt;
            public TimeSpan? FilledAt;
            public TimeSpan? TerminalAt;
            public decimal AnchorBid;
            public decimal AnchorAsk;
            public decimal ArrivalPrice;
            public decimal? FillPrice;
            public int ComboGroupId;
            public bool IsTerminal;
            public OrderStatus FinalStatus;
        }

        private class WaveState
        {
            public int Number;
            public BenchmarkCell Cell;
            public string Session;
            public readonly List<int> OrderIds = new();
            public TimeSpan StartElapsed;
        }

        private class WaveResult
        {
            public int Number;
            public BenchmarkCell Cell;
            public string Session;
            public int OrderCount;
            public int Filled;
            public int Canceled;
            public int Invalid;
            public double SubmitWallMs;
            public double FillWallMs;
            public List<double> SubmitLatencies;
            public List<double> FillLatencies;
            public List<double> SlippagesBps;
        }
    }
}
