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
using System.Linq;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Orders;
using QuantConnect.Brokerages;
using QuantConnect.Securities;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Live performance benchmark for the Tastytrade brokerage order API, run from QuantConnect Cloud.
    ///
    /// This is the cloud re-run of the Lean.Brokerages.Tastytrade PR #23 order-execution test. It places real
    /// orders through the live Tastytrade node and, for each order, measures the six metrics from the approved
    /// test plan:
    ///   * submit latency        place -> Submitted
    ///   * fill latency          place -> Filled
    ///   * market exec time      Submitted -> Filled           (market cells)
    ///   * limit resolution time Submitted -> Filled|Canceled  (limit and stop cells)
    ///   * slippage              signed, side-aware, in bps, anchored to the quote at each order's place time
    ///   * throughput            a 100-order alternating wave: orders/sec plus fail/throttle count
    ///
    /// Numbers are reported per SecurityType (Equity, Equity Option, Index Option, Future, Future Option) and per
    /// OrderType (Market, Limit, StopMarket, StopLimit, ComboLimit). Speed is shown as percentiles (p50 = typical,
    /// p95 = slow 1-in-20, p99 = worst 1-in-100), never as a headline average. A traded-volume summary (orders,
    /// contracts, notional, fill rate) frames the run as production-equivalent order flow. Each order is timed on
    /// one monotonic Stopwatch (C1 at place, C2 at the top of OnOrderEvent) so the number is immune to clock skew
    /// and captures the full round trip a cloud customer sees.
    ///
    /// The run is selectable: pick a subset of SecurityTypes and OrderTypes so a single cell can be exercised on
    /// demand. Samples are tagged with the session bucket (open / midday / close) and the full matrix runs once
    /// per requested bucket per day. Per-order rows are exported to the ObjectStore as CSV for offline percentile
    /// work and the renewal appendix.
    ///
    /// Real-money note: QuantConnect Cloud is live-only. Every order here is real and capital-at-risk. Risk is
    /// bounded by 1-unit sizing, alternating open/close (net stays flat, never shorts), marketable-limit price
    /// caps, a per-order notional ceiling, immediate flatten, and a daily loss / order-count kill-switch.
    ///
    /// This is a standalone alternative to <see cref="TastytradePerformanceBenchmarkAlgorithm"/>; both are
    /// Tastytrade-only by design and not meant to be reused for other brokerages.
    ///
    /// Parameters (all optional, set them in the cloud project):
    ///   security-types          comma list: Equity,Option,IndexOption,Future,FutureOption or "All"   (default Equity)
    ///   order-types             comma list: Market,Limit,StopMarket,StopLimit,ComboLimit or "All"     (default Market,Limit)
    ///   run-sessions            comma list: Open,Midday,Close or "Any"                                 (default Open,Midday,Close)
    ///   trials-per-cell         measured orders per cell                                               (default 20)
    ///   order-quantity          shares/contracts per order                                             (default 1)
    ///   equity-ticker           equity + heartbeat/session clock                                       (default AAPL)
    ///   option-underlying       underlying for the equity-option cells                                 (default SPY)
    ///   index-option-ticker     index for the index-option cells (SPX -> SPXW weeklys)                 (default SPX)
    ///   future-ticker           future root for future / future-option cells                           (default MES)
    ///   throughput-waves        number of throughput waves (0 = skip)                                  (default 0)
    ///   throughput-wave-size    orders per throughput wave                                             (default 100)
    ///   marketable-buffer-bps   how far a marketable limit crosses the touch, in bps                   (default 25)
    ///   stop-offset-bps         how far a resting stop is parked from the touch, in bps                (default 200)
    ///   resting-ttl-seconds     cancel a resting stop/warm-up order after this many seconds            (default 3)
    ///   seconds-between-trials  cooldown between measured orders                                        (default 2)
    ///   warmup-orders           throwaway equity orders per session (excluded from stats)              (default 5)
    ///   notional-cap-per-order  skip a cell if one unit's quote*qty*multiplier exceeds this            (default 50000)
    ///   daily-loss-cap          kill-switch: stop if the day's drawdown exceeds this (account ccy)     (default 300)
    ///   daily-order-cap         kill-switch: stop after this many orders placed in a day               (default 1000)
    ///   csv-key                 ObjectStore key for the per-order CSV export                           (default tastytrade-cloud-benchmark.csv)
    /// </summary>
    public class TastytradeCloudOrderBenchmarkAlgorithm : QCAlgorithm
    {
        // ----- configuration -----
        private List<SecurityType> _securityTypes;
        private List<OrderType> _orderTypes;
        private HashSet<string> _runSessions;
        private int _trialsPerCell;
        private decimal _orderQuantity;
        private string _equityTicker;
        private string _optionUnderlyingTicker;
        private string _indexOptionTicker;
        private string _futureTicker;
        private int _throughputWaves;
        private int _throughputWaveSize;
        private decimal _marketableBufferBps;
        private decimal _stopOffsetBps;
        private TimeSpan _restingTtl;
        private TimeSpan _trialCooldown;
        private int _warmupOrders;
        private decimal _notionalCapPerOrder;
        private decimal _dailyLossCap;
        private int _dailyOrderCap;
        private string _csvKey;

        // ----- subscriptions resolved in Initialize -----
        private Symbol _equitySymbol;               // heartbeat + session clock (always subscribed)
        private Symbol _optionUnderlyingSymbol;     // equity underlying for the equity-option cells
        private Symbol _indexSymbol;                // index underlying for the index-option cells
        private Symbol _futureCanonical;            // canonical continuous future
        private Symbol _frontFuture;                // resolved front-month future contract

        // ----- clocks and concurrency -----
        // One monotonic clock shared by the placing thread (OnData) and the event thread (OnOrderEvent).
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private readonly object _sync = new();

        // ----- run state machine -----
        private readonly List<TestCell> _cells = new();     // ordered plan for the current session run
        private int _cellIndex;
        private int _trialIndex;
        private string _currentSession;
        private bool _sessionActive;
        private int _warmupDone;

        private ResolveState _resolveState = ResolveState.None;
        private Symbol _leg1;                                // resolved tradable (single-leg cells and combo leg 1)
        private Symbol _leg2;                                // combo second leg
        private DateTime _resolveDeadlineUtc;

        private Trial _active;                               // order/combo/warm-up/wave currently in flight
        private bool _restingCancelSent;
        private bool _awaitingFlatten;
        private DateTime _nextTrialTimeUtc = DateTime.MinValue;

        // Maps a live order id to the trial + leg it belongs to. Orders we did not place (flatten) are ignored.
        private readonly Dictionary<int, (Trial trial, LegState leg)> _orderIndex = new();

        // ----- accumulated results -----
        private readonly List<TrialResult> _results = new();
        private readonly List<WaveResult> _waveResults = new();
        private readonly List<string> _csvRows = new();
        private readonly HashSet<string> _completedSessions = new();   // "yyyyMMdd:bucket" already run

        // ----- kill-switch bookkeeping -----
        private DateTime _currentDay = DateTime.MinValue;
        private decimal _dayStartValue;
        private int _dayOrderCount;
        private bool _killed;

        public override void Initialize()
        {
            // Dates only matter for a local backtest dry run; a live deploy ignores them.
            SetStartDate(2024, 1, 1);
            SetCash(100000);

            // Force the Tastytrade fee and fill model so a local dry run behaves like the live broker.
            SetBrokerageModel(BrokerageName.Tastytrade, AccountType.Margin);

            // Allow 1-share / 1-contract orders through without the default margin-buffer rejection.
            Settings.MinimumOrderMarginPortfolioPercentage = 0m;

            _securityTypes = ParseSecurityTypes(GetParameter("security-types", "Equity"));
            _orderTypes = ParseOrderTypes(GetParameter("order-types", "Market,Limit"));
            _runSessions = ParseSessions(GetParameter("run-sessions", "Open,Midday,Close"));
            _trialsPerCell = Math.Max(1, GetParameter("trials-per-cell", 20));
            _orderQuantity = Math.Max(1, GetParameter("order-quantity", 1));
            _equityTicker = GetParameter("equity-ticker", "AAPL");
            _optionUnderlyingTicker = GetParameter("option-underlying", "SPY");
            _indexOptionTicker = GetParameter("index-option-ticker", "SPX");
            _futureTicker = GetParameter("future-ticker", Futures.Indices.MicroSP500EMini);
            _throughputWaves = Math.Max(0, GetParameter("throughput-waves", 0));
            _throughputWaveSize = Math.Max(1, GetParameter("throughput-wave-size", 100));
            _marketableBufferBps = GetParameter("marketable-buffer-bps", 25m);
            _stopOffsetBps = GetParameter("stop-offset-bps", 200m);
            _restingTtl = TimeSpan.FromSeconds(Math.Max(1, GetParameter("resting-ttl-seconds", 3)));
            _trialCooldown = TimeSpan.FromSeconds(Math.Max(0, GetParameter("seconds-between-trials", 2)));
            _warmupOrders = Math.Max(0, GetParameter("warmup-orders", 5));
            _notionalCapPerOrder = GetParameter("notional-cap-per-order", 50000m);
            _dailyLossCap = GetParameter("daily-loss-cap", 300m);
            _dailyOrderCap = Math.Max(1, GetParameter("daily-order-cap", 1000));
            _csvKey = GetParameter("csv-key", "tastytrade-cloud-benchmark.csv");

            // Heartbeat + session clock: an equity at second resolution ticks OnData through the whole RTH session.
            _equitySymbol = AddEquity(_equityTicker, Resolution.Second).Symbol;

            if (_securityTypes.Contains(SecurityType.Option))
            {
                // AddOptionContract forces Raw on the underlying; set it here so the equity-option cell resolves cleanly.
                var underlying = AddEquity(_optionUnderlyingTicker, Resolution.Second);
                underlying.SetDataNormalizationMode(DataNormalizationMode.Raw);
                _optionUnderlyingSymbol = underlying.Symbol;
            }

            if (_securityTypes.Contains(SecurityType.IndexOption))
            {
                _indexSymbol = AddIndex(_indexOptionTicker, Resolution.Minute).Symbol;
            }

            if (_securityTypes.Contains(SecurityType.Future) || _securityTypes.Contains(SecurityType.FutureOption))
            {
                var future = AddFuture(_futureTicker, Resolution.Minute, Market.CME,
                    dataMappingMode: DataMappingMode.OpenInterest,
                    dataNormalizationMode: DataNormalizationMode.BackwardsRatio,
                    contractDepthOffset: 0);
                _futureCanonical = future.Symbol;
            }

            Log($"[Benchmark] Tastytrade cloud order-API benchmark. liveMode={LiveMode}");
            Log($"[Benchmark] securityTypes=[{string.Join(",", _securityTypes)}] orderTypes=[{string.Join(",", _orderTypes)}] " +
                $"sessions=[{string.Join(",", _runSessions)}] trialsPerCell={_trialsPerCell} qty={_orderQuantity}");
            Log($"[Benchmark] instruments: equity={_equityTicker} optionUnderlying={_optionUnderlyingTicker} " +
                $"indexOption={_indexOptionTicker} future={_futureTicker}");
            Log($"[Benchmark] throughputWaves={_throughputWaves}x{_throughputWaveSize} marketableBufferBps={_marketableBufferBps} " +
                $"notionalCap={_notionalCapPerOrder} dailyLossCap={_dailyLossCap} dailyOrderCap={_dailyOrderCap}");

            if (_indexOptionTicker.Equals("XSP", StringComparison.OrdinalIgnoreCase))
            {
                Log("[Benchmark] WARNING: XSP index options are not in Lean's supported/tested index-option set; " +
                    "chain data may be unavailable and the index-option cell may be skipped. SPX/SPXW is the safe path.");
            }
            if (!LiveMode)
            {
                Log("[Benchmark] WARNING: latency numbers are only meaningful in live mode; a backtest fills " +
                    "synchronously and reports near-zero times.");
            }

            SetCsvHeader();
        }

        public override void OnData(Slice slice)
        {
            // Drive everything off a valid, open equity heartbeat with a real price.
            if (_killed || !IsMarketOpen(_equitySymbol) || Securities[_equitySymbol].Price == 0)
            {
                return;
            }

            // OnData (placing) and OnOrderEvent (events) run on different threads in live; serialize the shared state.
            lock (_sync)
            {
                RollDay();
                if (CheckKillSwitch())
                {
                    return;
                }

                // Wait for the order/combo currently in flight; only nudge resting orders toward their cancel.
                if (_active != null)
                {
                    MaybeCancelResting();
                    return;
                }

                // Wait for a requested flatten to settle before the next measured order.
                if (_awaitingFlatten)
                {
                    if (IsFlat(_leg1) && IsFlat(_leg2))
                    {
                        _awaitingFlatten = false;
                        _nextTrialTimeUtc = UtcTime.Add(_trialCooldown);
                    }
                    return;
                }

                if (UtcTime < _nextTrialTimeUtc)
                {
                    return;
                }

                if (!_sessionActive)
                {
                    TryStartSession();
                    return;
                }

                // Warm-up first: throwaway orders that absorb cold start, excluded from all statistics.
                if (_warmupDone < _warmupOrders)
                {
                    PlaceWarmupOrder();
                    _warmupDone++;
                    return;
                }

                if (_cellIndex >= _cells.Count)
                {
                    FinishSession();
                    return;
                }

                var cell = _cells[_cellIndex];
                try
                {
                    StepCell(cell);
                }
                catch (Exception e)
                {
                    Log($"[Benchmark] ERROR in cell {cell.Label}: {e.Message}; skipping.");
                    _active = null;
                    AdvanceCell();
                }
            }
        }

        private void StepCell(TestCell cell)
        {
            // Throughput waves are their own kind of cell (fire-all-at-once), no per-trial instrument resolution.
            if (cell.Kind == CellKind.Wave)
            {
                PlaceThroughputWave(cell);
                AdvanceCell();
                return;
            }

            var status = EnsureInstrument(cell);
            if (status == ResolveState.Failed)
            {
                Log($"[Benchmark] SKIP cell {cell.Label}: could not resolve a tradable instrument with a live quote.");
                AdvanceCell();
                return;
            }
            if (status != ResolveState.Ready)
            {
                return; // still resolving / waiting for a quote
            }

            if (_trialIndex >= _trialsPerCell)
            {
                // Cell finished: flatten any residual (odd trial count) before the next cell.
                if (AnyInvested())
                {
                    FlattenLegs();
                    return;
                }
                AdvanceCell();
                return;
            }

            PlaceTrial(cell);
        }

        // ----------------------------------------------------------------------------------------------------
        // Session gating
        // ----------------------------------------------------------------------------------------------------

        private void TryStartSession()
        {
            var bucket = GetSessionBucket(Time);
            if (!_runSessions.Contains(bucket))
            {
                return;
            }

            var key = $"{Time:yyyyMMdd}:{bucket}";
            if (_completedSessions.Contains(key))
            {
                return;
            }

            _currentSession = bucket;
            _sessionActive = true;
            _warmupDone = 0;
            _cellIndex = 0;
            _trialIndex = 0;
            ResetInstrument();
            BuildCells();

            Log($"[Benchmark] ===== Session '{bucket}' {Time:yyyy-MM-dd HH:mm} ET: {_cells.Count} cells, " +
                $"{_warmupOrders} warm-up orders =====");
        }

        private void FinishSession()
        {
            _completedSessions.Add($"{Time:yyyyMMdd}:{_currentSession}");
            _sessionActive = false;

            ReportSession(_currentSession);
            SaveCsv();
        }

        /// <summary>
        /// Buckets the exchange-local (ET) time into the plan's three session windows. Anything else is "other".
        /// </summary>
        private static string GetSessionBucket(DateTime et)
        {
            var t = et.TimeOfDay;
            if (t >= new TimeSpan(9, 30, 0) && t < new TimeSpan(10, 0, 0)) return "open";
            if (t >= new TimeSpan(12, 0, 0) && t < new TimeSpan(13, 0, 0)) return "midday";
            if (t >= new TimeSpan(15, 30, 0) && t < new TimeSpan(16, 0, 0)) return "close";
            return "other";
        }

        /// <summary>
        /// Builds the ordered cell plan for the current session: valid SecurityType x OrderType combinations,
        /// fill-on-placement cells first, then resting stop cells, then combos, then any throughput waves.
        /// </summary>
        private void BuildCells()
        {
            _cells.Clear();
            foreach (var sec in _securityTypes)
            {
                foreach (var ord in _orderTypes)
                {
                    if (!IsValidCell(sec, ord))
                    {
                        continue;
                    }
                    _cells.Add(new TestCell(sec, ord));
                }
            }

            _cells.Sort((a, b) =>
            {
                var byOrder = CellPriority(a).CompareTo(CellPriority(b));
                return byOrder != 0 ? byOrder : SecurityPriority(a.Security).CompareTo(SecurityPriority(b.Security));
            });

            for (var i = 0; i < _throughputWaves; i++)
            {
                _cells.Add(new TestCell(SecurityType.Equity, OrderType.Market, CellKind.Wave));
            }
        }

        private static bool IsValidCell(SecurityType sec, OrderType ord)
        {
            // ComboLimit is multi-leg options only (equity option, index option, future option).
            if (ord == OrderType.ComboLimit)
            {
                return sec == SecurityType.Option || sec == SecurityType.IndexOption || sec == SecurityType.FutureOption;
            }
            return true;
        }

        // Fill-on-placement cells first (Market, Limit), then resting stops, then combos (plan section 3.2).
        private static int CellPriority(TestCell c)
        {
            switch (c.Order)
            {
                case OrderType.Market: return 0;
                case OrderType.Limit: return 1;
                case OrderType.StopMarket: return 2;
                case OrderType.StopLimit: return 3;
                case OrderType.ComboLimit: return 4;
                default: return 5;
            }
        }

        // Liquid underlyings before thinner, higher-notional ones.
        private static int SecurityPriority(SecurityType s)
        {
            switch (s)
            {
                case SecurityType.Equity: return 0;
                case SecurityType.Option: return 1;
                case SecurityType.Future: return 2;
                case SecurityType.IndexOption: return 3;
                case SecurityType.FutureOption: return 4;
                default: return 5;
            }
        }

        private void AdvanceCell()
        {
            _cellIndex++;
            _trialIndex = 0;
            ResetInstrument();
            _nextTrialTimeUtc = UtcTime.Add(_trialCooldown);
        }

        // ----------------------------------------------------------------------------------------------------
        // Instrument resolution (subscribe + wait for a live quote), incremental across OnData ticks
        // ----------------------------------------------------------------------------------------------------

        private void ResetInstrument()
        {
            _resolveState = ResolveState.None;
            _leg1 = null;
            _leg2 = null;
        }

        private ResolveState EnsureInstrument(TestCell cell)
        {
            if (_resolveState == ResolveState.Ready || _resolveState == ResolveState.Failed)
            {
                return _resolveState;
            }

            if (_resolveState == ResolveState.None)
            {
                _resolveDeadlineUtc = UtcTime.AddSeconds(90);
                _resolveState = ResolveState.Resolving;
            }

            if (UtcTime > _resolveDeadlineUtc)
            {
                return _resolveState = ResolveState.Failed;
            }

            var needCombo = cell.Order == OrderType.ComboLimit;

            switch (cell.Security)
            {
                case SecurityType.Equity:
                    _leg1 = _equitySymbol;
                    return _resolveState = QuoteReady(_leg1) ? ResolveState.Ready : ResolveState.Resolving;

                case SecurityType.Future:
                    if (!EnsureFrontFuture())
                    {
                        return _resolveState = ResolveState.Failed;
                    }
                    _leg1 = _frontFuture;
                    return _resolveState = QuoteReady(_leg1) ? ResolveState.Ready : ResolveState.Resolving;

                case SecurityType.Option:
                case SecurityType.IndexOption:
                case SecurityType.FutureOption:
                    return ResolveOptionCell(cell, needCombo);

                default:
                    return _resolveState = ResolveState.Failed;
            }
        }

        private ResolveState ResolveOptionCell(TestCell cell, bool needCombo)
        {
            // Determine the underlying whose chain we walk and whose price defines "ATM".
            Symbol underlying;
            if (cell.Security == SecurityType.Option)
            {
                underlying = _optionUnderlyingSymbol;
            }
            else if (cell.Security == SecurityType.IndexOption)
            {
                underlying = _indexSymbol;
            }
            else // FutureOption
            {
                if (!EnsureFrontFuture())
                {
                    return _resolveState = ResolveState.Failed;
                }
                underlying = _frontFuture;
            }

            if (underlying == null || !QuoteReady(underlying))
            {
                return _resolveState = ResolveState.Resolving; // wait for the underlying price to arrive
            }

            // Resolve the contract(s) once, then subscribe and wait for their quotes on subsequent ticks.
            if (_leg1 == null)
            {
                var underlyingPrice = Securities[underlying].Price;
                var calls = OptionChainProvider.GetOptionContractList(underlying, Time)
                    .Where(s => s.ID.OptionRight == OptionRight.Call && s.ID.Date.Date >= Time.Date)
                    .OrderBy(s => s.ID.Date)
                    .ThenBy(s => Math.Abs(s.ID.StrikePrice - underlyingPrice))
                    .ToList();

                if (calls.Count == 0)
                {
                    return _resolveState = ResolveState.Failed;
                }

                var atm = calls[0];
                _leg1 = SubscribeOptionContract(cell.Security, atm);

                if (needCombo)
                {
                    // Debit vertical: long the ATM call, short the next higher strike on the same expiry.
                    var higher = calls
                        .Where(s => s.ID.Date == atm.ID.Date && s.ID.StrikePrice > atm.ID.StrikePrice)
                        .OrderBy(s => s.ID.StrikePrice)
                        .FirstOrDefault();
                    if (higher == null)
                    {
                        return _resolveState = ResolveState.Failed;
                    }
                    _leg2 = SubscribeOptionContract(cell.Security, higher);
                }
            }

            var ready = QuoteReady(_leg1) && (!needCombo || QuoteReady(_leg2));
            return _resolveState = ready ? ResolveState.Ready : ResolveState.Resolving;
        }

        private bool EnsureFrontFuture()
        {
            if (_frontFuture != null)
            {
                return true;
            }
            if (_futureCanonical == null)
            {
                return false;
            }

            var front = FutureChainProvider.GetFutureContractList(_futureCanonical, Time)
                .Where(s => s.ID.Date.Date >= Time.Date)
                .OrderBy(s => s.ID.Date)
                .FirstOrDefault();
            if (front == null)
            {
                return false;
            }

            _frontFuture = AddFutureContract(front, Resolution.Minute).Symbol;
            return true;
        }

        private Symbol SubscribeOptionContract(SecurityType security, Symbol contract)
        {
            switch (security)
            {
                case SecurityType.Option: return AddOptionContract(contract, Resolution.Minute).Symbol;
                case SecurityType.IndexOption: return AddIndexOptionContract(contract, Resolution.Minute).Symbol;
                case SecurityType.FutureOption: return AddFutureOptionContract(contract, Resolution.Minute).Symbol;
                default: throw new ArgumentOutOfRangeException(nameof(security));
            }
        }

        private bool QuoteReady(Symbol symbol)
        {
            return symbol != null && Securities.ContainsKey(symbol) && Securities[symbol].Price != 0;
        }

        // ----------------------------------------------------------------------------------------------------
        // Order placement
        // ----------------------------------------------------------------------------------------------------

        private void PlaceTrial(TestCell cell)
        {
            if (cell.Kind == CellKind.Combo)
            {
                PlaceComboTrial(cell);
            }
            else if (cell.Kind == CellKind.Resting)
            {
                PlaceRestingTrial(cell);
            }
            else
            {
                PlaceFillTrial(cell);
            }
            _trialIndex++;
        }

        /// <summary>
        /// Market / Limit fill cell. Alternates open (buy) and close (sell) based on the current holding so the
        /// net position never leaves [0, +qty] and both sides are sampled without ever shorting.
        /// </summary>
        private void PlaceFillTrial(TestCell cell)
        {
            var security = Securities[_leg1];
            var (bid, ask) = Quote(security);
            var holding = Portfolio[_leg1].Quantity;
            var buy = holding <= 0;                 // flat -> open long; long -> close
            var qty = buy ? _orderQuantity : -_orderQuantity;
            var anchorTouch = buy ? ask : bid;
            var anchorMid = (bid + ask) / 2m;

            if (!PassesNotionalCap(security, anchorTouch))
            {
                Log($"[Benchmark] SKIP cell {cell.Label}: one unit notional exceeds the cap ({_notionalCapPerOrder}).");
                _resolveState = ResolveState.Failed;
                return;
            }

            var trial = NewTrial(cell, buy ? 1 : -1);
            var place = _clock.Elapsed;

            OrderTicket ticket;
            if (cell.Order == OrderType.Market)
            {
                ticket = MarketOrder(_leg1, qty, asynchronous: true, tag: trial.Tag);
            }
            else // Limit (marketable)
            {
                var limit = MarketableLimit(security, buy, ask, bid);
                ticket = LimitOrder(_leg1, qty, limit, asynchronous: true, tag: trial.Tag);
            }

            RegisterLeg(trial, ticket, place, 1, anchorTouch, anchorMid);
            _active = trial;
        }

        /// <summary>
        /// Stop cell. Parks a resting stop far enough from the touch that it will not trigger, then cancels it
        /// after the TTL. Measures submit latency and the Submitted -> Canceled resolution time (no fill).
        /// </summary>
        private void PlaceRestingTrial(TestCell cell)
        {
            var security = Securities[_leg1];
            var (bid, ask) = Quote(security);

            if (!PassesNotionalCap(security, ask))
            {
                Log($"[Benchmark] SKIP cell {cell.Label}: one unit notional exceeds the cap ({_notionalCapPerOrder}).");
                _resolveState = ResolveState.Failed;
                return;
            }

            // Buy stop parked above the market -> rests until cancelled.
            var offset = _stopOffsetBps / 10000m;
            var stop = RoundTick(security, ask * (1 + offset));
            var trial = NewTrial(cell, 1);
            trial.IsResting = true;
            var place = _clock.Elapsed;

            OrderTicket ticket;
            if (cell.Order == OrderType.StopMarket)
            {
                ticket = StopMarketOrder(_leg1, _orderQuantity, stop, asynchronous: true, tag: trial.Tag);
            }
            else // StopLimit
            {
                var limit = RoundTick(security, stop * (1 + offset));
                ticket = StopLimitOrder(_leg1, _orderQuantity, stop, limit, asynchronous: true, tag: trial.Tag);
            }

            RegisterLeg(trial, ticket, place, 1, ask, (bid + ask) / 2m);
            _active = trial;
            _restingCancelSent = false;
        }

        /// <summary>
        /// ComboLimit cell. Opens a marketable debit call vertical (long ATM, short next strike) with a single
        /// compound limit price, then flattens both legs. Latency is on the parent (all legs terminal); slippage
        /// is on the net combo price vs. the net touch anchor at order time.
        /// </summary>
        private void PlaceComboTrial(TestCell cell)
        {
            var s1 = Securities[_leg1];
            var s2 = Securities[_leg2];
            var (bid1, ask1) = Quote(s1);
            var (bid2, ask2) = Quote(s2);

            // Long leg1 at its ask, short leg2 at its bid -> net debit we would pay to cross.
            var netTouch = ask1 - bid2;

            if (!PassesNotionalCap(s1, ask1))
            {
                Log($"[Benchmark] SKIP cell {cell.Label}: one unit notional exceeds the cap ({_notionalCapPerOrder}).");
                _resolveState = ResolveState.Failed;
                return;
            }

            var buffer = _marketableBufferBps / 10000m;
            var limit = RoundTick(s1, netTouch * (1 + buffer));
            if (limit <= 0)
            {
                limit = RoundTick(s1, Math.Max(netTouch, s1.SymbolProperties.MinimumPriceVariation));
            }

            var legs = new List<Leg>
            {
                Leg.Create(_leg1, 1),
                Leg.Create(_leg2, -1)
            };

            var trial = NewTrial(cell, 1);   // opening a debit spread -> side +1
            var place = _clock.Elapsed;
            var tickets = ComboLimitOrder(legs, (int)_orderQuantity, limit, asynchronous: true, tag: trial.Tag);

            foreach (var ticket in tickets)
            {
                var isNear = ticket.Symbol == _leg1;
                var leg = new LegState
                {
                    OrderId = ticket.OrderId,
                    Ratio = isNear ? 1 : -1,
                    PlaceElapsed = place,
                    AnchorTouch = isNear ? ask1 : bid2,
                    AnchorMid = isNear ? (bid1 + ask1) / 2m : (bid2 + ask2) / 2m
                };
                trial.Legs.Add(leg);
                _orderIndex[ticket.OrderId] = (trial, leg);
                _dayOrderCount++;
            }

            _active = trial;
        }

        private void PlaceWarmupOrder()
        {
            var security = Securities[_equitySymbol];
            var (bid, _) = Quote(security);
            // Non-marketable buy limit well below the market: rests, never fills, then cancelled. Cold-start only.
            var limit = RoundTick(security, bid * 0.5m);
            var trial = NewTrial(new TestCell(SecurityType.Equity, OrderType.Limit, CellKind.Resting), 1);
            trial.IsResting = true;
            trial.IsWarmup = true;
            var place = _clock.Elapsed;
            var ticket = LimitOrder(_equitySymbol, _orderQuantity, limit, asynchronous: true, tag: trial.Tag);
            RegisterLeg(trial, ticket, place, 1, bid, bid);
            _active = trial;
            _restingCancelSent = false;
        }

        private void PlaceThroughputWave(TestCell cell)
        {
            var security = Securities[_equitySymbol];
            var (bid, ask) = Quote(security);
            var trial = NewTrial(cell, 1);
            trial.IsWave = true;
            trial.WaveStart = _clock.Elapsed;

            Log($"[Benchmark] Throughput wave: firing {_throughputWaveSize} alternating market orders on {_equityTicker}.");

            for (var i = 0; i < _throughputWaveSize; i++)
            {
                // Alternate buy/sell. Lean serializes on the per-order ack, so net stays within ~1 share.
                var buy = i % 2 == 0;
                var qty = buy ? _orderQuantity : -_orderQuantity;
                var place = _clock.Elapsed;
                var ticket = MarketOrder(_equitySymbol, qty, asynchronous: true, tag: $"{trial.Tag}-{i + 1}");
                var leg = new LegState
                {
                    OrderId = ticket.OrderId,
                    Ratio = 1,
                    PlaceElapsed = place,
                    AnchorTouch = buy ? ask : bid,
                    AnchorMid = (bid + ask) / 2m
                };
                trial.Legs.Add(leg);
                _orderIndex[ticket.OrderId] = (trial, leg);
                _dayOrderCount++;
            }

            _active = trial;
        }

        private void MaybeCancelResting()
        {
            if (_active == null || !_active.IsResting || _restingCancelSent)
            {
                return;
            }
            var placed = _active.Legs.Count > 0 ? _active.Legs[0].PlaceElapsed : _clock.Elapsed;
            if (_clock.Elapsed - placed < _restingTtl)
            {
                return;
            }
            foreach (var leg in _active.Legs)
            {
                Transactions.CancelOrder(leg.OrderId, "benchmark ttl");
            }
            _restingCancelSent = true;
        }

        private decimal MarketableLimit(Security security, bool buy, decimal ask, decimal bid)
        {
            var buffer = _marketableBufferBps / 10000m;
            var raw = buy ? ask * (1 + buffer) : bid * (1 - buffer);
            return RoundTick(security, raw);
        }

        private bool PassesNotionalCap(Security security, decimal price)
        {
            var multiplier = security.SymbolProperties.ContractMultiplier;
            if (multiplier <= 0)
            {
                multiplier = 1m;
            }
            var notional = Math.Abs(price) * _orderQuantity * multiplier;
            return notional <= _notionalCapPerOrder;
        }

        private Trial NewTrial(TestCell cell, int side)
        {
            return new Trial
            {
                Cell = cell,
                Session = _currentSession,
                SideSign = side,
                WhenEt = Time,
                Tag = $"bench-{cell.Security}-{cell.Order}-{_cellIndex}-{_trialIndex}"
            };
        }

        private void RegisterLeg(Trial trial, OrderTicket ticket, TimeSpan place, int ratio, decimal anchorTouch, decimal anchorMid)
        {
            var leg = new LegState
            {
                OrderId = ticket.OrderId,
                Ratio = ratio,
                PlaceElapsed = place,
                AnchorTouch = anchorTouch,
                AnchorMid = anchorMid
            };
            trial.Legs.Add(leg);
            _orderIndex[ticket.OrderId] = (trial, leg);
            _dayOrderCount++;
        }

        // ----------------------------------------------------------------------------------------------------
        // Order events
        // ----------------------------------------------------------------------------------------------------

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            // Read the clock before taking the lock so contention never inflates the measurement.
            var now = _clock.Elapsed;

            lock (_sync)
            {
                if (!_orderIndex.TryGetValue(orderEvent.OrderId, out var entry))
                {
                    return; // flatten / liquidation orders are not part of the benchmark
                }

                var trial = entry.trial;
                var leg = entry.leg;

                switch (orderEvent.Status)
                {
                    case OrderStatus.Submitted:
                        leg.SubmittedElapsed ??= now;
                        break;
                    case OrderStatus.Filled:
                    case OrderStatus.PartiallyFilled:
                        if (orderEvent.FillQuantity != 0)
                        {
                            leg.FillPrice = orderEvent.FillPrice;
                            leg.FillQuantity += orderEvent.FillQuantity;
                        }
                        break;
                }

                if (IsTerminal(orderEvent.Status) && !leg.IsTerminal)
                {
                    leg.IsTerminal = true;
                    leg.TerminalElapsed = now;
                    leg.FinalStatus = orderEvent.Status;
                    trial.TerminalCount++;

                    if (trial.TerminalCount >= trial.Legs.Count)
                    {
                        CompleteTrial(trial);
                    }
                }
            }
        }

        private void CompleteTrial(Trial trial)
        {
            foreach (var leg in trial.Legs)
            {
                _orderIndex.Remove(leg.OrderId);
            }
            _active = null;

            if (trial.IsWave)
            {
                CompleteWave(trial);
            }
            else if (!trial.IsWarmup)
            {
                RecordTrialResult(trial);
            }

            // Post-actions: combos hold a spread and must be flattened; fill cells self-flatten via alternation.
            if (!trial.IsWave && ((trial.Cell.Kind == CellKind.Combo) || trial.IsResting) && AnyInvested())
            {
                FlattenLegs();
            }
            else if (!trial.IsWave)
            {
                _nextTrialTimeUtc = UtcTime.Add(_trialCooldown);
            }
        }

        private void RecordTrialResult(Trial trial)
        {
            var place = trial.Legs.Min(l => l.PlaceElapsed);
            var submittedTimes = trial.Legs.Where(l => l.SubmittedElapsed != null).Select(l => l.SubmittedElapsed.Value).ToList();
            var submitted = submittedTimes.Count > 0 ? submittedTimes.Min() : (TimeSpan?)null;
            var terminal = trial.Legs.Max(l => l.TerminalElapsed ?? place);
            var allFilled = trial.Legs.All(l => l.FinalStatus == OrderStatus.Filled);

            double? submitMs = submitted != null ? (submitted.Value - place).TotalMilliseconds : (double?)null;
            double? fillMs = allFilled ? (terminal - place).TotalMilliseconds : (double?)null;
            double? execMs = (allFilled && submitted != null) ? (terminal - submitted.Value).TotalMilliseconds : (double?)null;
            double? resolutionMs = submitted != null ? (terminal - submitted.Value).TotalMilliseconds : (double?)null;

            // Net values: sum leg values by leg ratio (long adds, short subtracts). Single-leg orders have ratio +1
            // and carry direction in SideSign, so the same formula gives the right sign for buys and sells.
            var netFill = trial.Legs.Sum(l => l.Ratio * l.FillPrice);
            var netTouch = trial.Legs.Sum(l => l.Ratio * l.AnchorTouch);
            var netMid = trial.Legs.Sum(l => l.Ratio * l.AnchorMid);

            double? slipTouchBps = null;
            double? slipMidBps = null;
            if (allFilled && netTouch != 0)
            {
                slipTouchBps = (double)(trial.SideSign * (netFill - netTouch) / Math.Abs(netTouch)) * 10000.0;
            }
            if (allFilled && netMid != 0)
            {
                slipMidBps = (double)(trial.SideSign * (netFill - netMid) / Math.Abs(netMid)) * 10000.0;
            }

            var multiplier = Securities[_leg1].SymbolProperties.ContractMultiplier;
            if (multiplier <= 0) multiplier = 1m;
            var contracts = allFilled ? trial.Legs.Sum(l => Math.Abs(l.FillQuantity)) : 0m;
            var notional = allFilled ? Math.Abs(netFill) * multiplier * _orderQuantity : 0m;

            var result = new TrialResult
            {
                Session = trial.Session,
                Security = trial.Cell.Security,
                Order = trial.Cell.Order,
                SideSign = trial.SideSign,
                Resting = trial.IsResting,
                Filled = allFilled,
                FinalStatus = trial.Legs[trial.Legs.Count - 1].FinalStatus,
                SubmitMs = submitMs,
                FillMs = fillMs,
                ExecMs = execMs,
                ResolutionMs = resolutionMs,
                SlipTouchBps = slipTouchBps,
                SlipMidBps = slipMidBps,
                NetFill = netFill,
                NetTouch = netTouch,
                NetMid = netMid,
                Contracts = contracts,
                Notional = notional,
                Legs = trial.Legs.Count,
                WhenEt = trial.WhenEt,
                HadSubmitted = submitted != null
            };
            _results.Add(result);
            AppendCsvRow(result);
        }

        private void CompleteWave(Trial trial)
        {
            var start = trial.WaveStart;
            var submitted = trial.Legs.Where(l => l.SubmittedElapsed != null).Select(l => l.SubmittedElapsed.Value).ToList();
            var filled = trial.Legs.Count(l => l.FinalStatus == OrderStatus.Filled);
            var failed = trial.Legs.Count(l => l.FinalStatus != OrderStatus.Filled);

            var submitWall = (submitted.Count > 0 ? submitted.Max() : start) - start;
            var terminalMax = trial.Legs.Max(l => l.TerminalElapsed ?? start);
            var fillWallMs = (terminalMax - start).TotalMilliseconds;
            var ordersPerSec = fillWallMs > 0 ? trial.Legs.Count / (fillWallMs / 1000.0) : 0;

            var wave = new WaveResult
            {
                Session = trial.Session,
                Orders = trial.Legs.Count,
                Filled = filled,
                Failed = failed,
                SubmitWallMs = submitWall.TotalMilliseconds,
                FillWallMs = fillWallMs,
                OrdersPerSec = ordersPerSec,
                SubmitLatencies = trial.Legs.Where(l => l.SubmittedElapsed != null)
                    .Select(l => (l.SubmittedElapsed.Value - l.PlaceElapsed).TotalMilliseconds).ToList(),
                FillLatencies = trial.Legs.Where(l => l.FinalStatus == OrderStatus.Filled && l.TerminalElapsed != null)
                    .Select(l => (l.TerminalElapsed.Value - l.PlaceElapsed).TotalMilliseconds).ToList()
            };
            _waveResults.Add(wave);

            Log($"[Benchmark] Wave done [{trial.Session}]: {wave.Orders} orders, filled={wave.Filled} failed={wave.Failed}, " +
                $"submit-all {wave.SubmitWallMs:0} ms, fill-all {wave.FillWallMs:0} ms, {wave.OrdersPerSec:0.0} orders/sec " +
                $"(end-to-end integration throughput, not broker parallelism).");
            Log($"[Benchmark]   wave submit latency ms: {FormatStats(wave.SubmitLatencies)}");
            Log($"[Benchmark]   wave fill latency ms:   {FormatStats(wave.FillLatencies)}");

            if (Portfolio[_equitySymbol].Invested)
            {
                Liquidate(_equitySymbol, tag: "wave-flatten");
            }
            _nextTrialTimeUtc = UtcTime.Add(_trialCooldown);
        }

        private void FlattenLegs()
        {
            if (_leg1 != null && Portfolio[_leg1].Invested)
            {
                Liquidate(_leg1, tag: "flatten");
            }
            if (_leg2 != null && Portfolio[_leg2].Invested)
            {
                Liquidate(_leg2, tag: "flatten");
            }
            _awaitingFlatten = true;
        }

        private bool AnyInvested()
        {
            return (_leg1 != null && Portfolio[_leg1].Invested) || (_leg2 != null && Portfolio[_leg2].Invested);
        }

        private bool IsFlat(Symbol symbol)
        {
            return symbol == null || !Portfolio[symbol].Invested;
        }

        // ----------------------------------------------------------------------------------------------------
        // Kill-switch & day roll
        // ----------------------------------------------------------------------------------------------------

        private void RollDay()
        {
            if (Time.Date == _currentDay)
            {
                return;
            }
            _currentDay = Time.Date;
            _dayStartValue = Portfolio.TotalPortfolioValue;
            _dayOrderCount = 0;
        }

        private bool CheckKillSwitch()
        {
            var drawdown = _dayStartValue - Portfolio.TotalPortfolioValue;
            if (drawdown > _dailyLossCap)
            {
                TriggerKill($"daily loss cap breached: drawdown {drawdown:0.00} > {_dailyLossCap}");
                return true;
            }
            if (_dayOrderCount > _dailyOrderCap)
            {
                TriggerKill($"daily order cap breached: {_dayOrderCount} > {_dailyOrderCap}");
                return true;
            }
            return false;
        }

        private void TriggerKill(string reason)
        {
            if (_killed)
            {
                return;
            }
            _killed = true;
            Log($"[Benchmark] KILL-SWITCH: {reason}. Cancelling open orders and liquidating all positions.");
            Transactions.CancelOpenOrders();
            Liquidate(tag: "kill-switch");
            ReportGrandTotal();
            SaveCsv();
            Quit("Benchmark kill-switch triggered");
        }

        public override void OnEndOfAlgorithm()
        {
            ReportGrandTotal();
            SaveCsv();
        }

        // ----------------------------------------------------------------------------------------------------
        // Reporting
        // ----------------------------------------------------------------------------------------------------

        private void ReportSession(string session)
        {
            Log($"[Benchmark] ===== Session '{session}' complete =====");
            ReportBreakdown(r => r.Session == session);
        }

        private void ReportGrandTotal()
        {
            Log("[Benchmark] ========== GRAND SUMMARY (all sessions) ==========");
            ReportBreakdown(_ => true);
            ReportThroughput();
            ReportVolume();
        }

        private void ReportBreakdown(Func<TrialResult, bool> filter)
        {
            var rows = _results.Where(filter).ToList();
            if (rows.Count == 0)
            {
                Log("[Benchmark] no measured orders yet.");
                return;
            }

            Log("[Benchmark] --- per SecurityType x OrderType ---");
            foreach (var cell in rows.GroupBy(r => (r.Security, r.Order))
                         .OrderBy(g => SecurityPriority(g.Key.Security)).ThenBy(g => (int)g.Key.Order))
            {
                LogMetricLine($"{cell.Key.Security}/{cell.Key.Order}", cell.ToList());
            }

            Log("[Benchmark] --- per OrderType (rolled up) ---");
            foreach (var g in rows.GroupBy(r => r.Order).OrderBy(g => (int)g.Key))
            {
                LogMetricLine(g.Key.ToString(), g.ToList());
            }

            Log("[Benchmark] --- per SecurityType (rolled up) ---");
            foreach (var g in rows.GroupBy(r => r.Security).OrderBy(g => SecurityPriority(g.Key)))
            {
                LogMetricLine(g.Key.ToString(), g.ToList());
            }

            Log("[Benchmark] --- per session bucket ---");
            foreach (var g in rows.GroupBy(r => r.Session).OrderBy(g => g.Key))
            {
                LogMetricLine(g.Key, g.ToList());
            }
        }

        private void LogMetricLine(string label, List<TrialResult> rows)
        {
            var n = rows.Count;
            var filled = rows.Count(r => r.Filled);
            var fillRate = n > 0 ? 100.0 * filled / n : 0;
            Log($"[Benchmark] {label}: n={n} filled={filled} ({fillRate:0.0}%)");
            Log($"[Benchmark]   submit ms      {FormatStats(rows.Where(r => r.SubmitMs != null).Select(r => r.SubmitMs.Value).ToList())}");
            Log($"[Benchmark]   fill ms        {FormatStats(rows.Where(r => r.FillMs != null).Select(r => r.FillMs.Value).ToList())}");
            Log($"[Benchmark]   exec ms        {FormatStats(rows.Where(r => r.ExecMs != null).Select(r => r.ExecMs.Value).ToList())}");
            Log($"[Benchmark]   resolution ms  {FormatStats(rows.Where(r => r.ResolutionMs != null).Select(r => r.ResolutionMs.Value).ToList())}");
            Log($"[Benchmark]   slip touch bps {FormatStats(rows.Where(r => r.SlipTouchBps != null).Select(r => r.SlipTouchBps.Value).ToList())}");
            Log($"[Benchmark]   slip mid bps   {FormatStats(rows.Where(r => r.SlipMidBps != null).Select(r => r.SlipMidBps.Value).ToList())}");
        }

        private void ReportThroughput()
        {
            if (_waveResults.Count == 0)
            {
                return;
            }
            Log("[Benchmark] --- throughput waves (end-to-end integration rate, not broker parallelism) ---");
            var i = 1;
            foreach (var w in _waveResults)
            {
                Log($"[Benchmark]   wave {i++} [{w.Session}]: orders={w.Orders} filled={w.Filled} failed={w.Failed} " +
                    $"submit-all={w.SubmitWallMs:0}ms fill-all={w.FillWallMs:0}ms {w.OrdersPerSec:0.0}/sec");
            }
        }

        private void ReportVolume()
        {
            var filled = _results.Where(r => r.Filled).ToList();
            var totalOrders = _results.Count + _waveResults.Sum(w => w.Orders);
            var totalFilled = filled.Count + _waveResults.Sum(w => w.Filled);
            var contracts = filled.Sum(r => r.Contracts);
            var notional = filled.Sum(r => r.Notional);
            var fillRate = totalOrders > 0 ? 100.0 * totalFilled / totalOrders : 0;

            Log("[Benchmark] --- traded volume (renewal framing) ---");
            Log($"[Benchmark]   orders placed={totalOrders} filled={totalFilled} ({fillRate:0.0}%) " +
                $"shares/contracts={contracts:0} notional={notional:0} {Portfolio.CashBook.AccountCurrency}");
        }

        // ----------------------------------------------------------------------------------------------------
        // CSV export
        // ----------------------------------------------------------------------------------------------------

        private void SetCsvHeader()
        {
            _csvRows.Clear();
            _csvRows.Add("timestamp_et,session,security_type,order_type,side,resting,legs,final_status," +
                "submit_ms,fill_ms,exec_ms,resolution_ms,net_fill,anchor_touch,anchor_mid," +
                "slip_touch_bps,slip_mid_bps,contracts,notional,had_submitted");
        }

        private void AppendCsvRow(TrialResult r)
        {
            var side = r.SideSign > 0 ? "BUY" : "SELL";
            _csvRows.Add(string.Join(",", new[]
            {
                r.WhenEt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture),
                r.Session,
                r.Security.ToString(),
                r.Order.ToString(),
                side,
                r.Resting ? "1" : "0",
                r.Legs.ToString(CultureInfo.InvariantCulture),
                r.FinalStatus.ToString(),
                Csv(r.SubmitMs),
                Csv(r.FillMs),
                Csv(r.ExecMs),
                Csv(r.ResolutionMs),
                Csv((double)r.NetFill),
                Csv((double)r.NetTouch),
                Csv((double)r.NetMid),
                Csv(r.SlipTouchBps),
                Csv(r.SlipMidBps),
                Csv((double)r.Contracts),
                Csv((double)r.Notional),
                r.HadSubmitted ? "1" : "0"
            }));
        }

        private void SaveCsv()
        {
            try
            {
                ObjectStore.Save(_csvKey, string.Join("\n", _csvRows));
            }
            catch (Exception e)
            {
                Log($"[Benchmark] WARNING: could not save CSV to ObjectStore: {e.Message}");
            }
        }

        private static string Csv(double? value)
        {
            return value == null ? "" : value.Value.ToString("0.####", CultureInfo.InvariantCulture);
        }

        // ----------------------------------------------------------------------------------------------------
        // Small helpers
        // ----------------------------------------------------------------------------------------------------

        private static (decimal bid, decimal ask) Quote(Security security)
        {
            var bid = security.BidPrice;
            var ask = security.AskPrice;
            // BidPrice/AskPrice fall back to Price when a side is missing; if both collapse to the same value we
            // still return it so the marketable-limit math stays finite.
            if (bid == 0) bid = security.Price;
            if (ask == 0) ask = security.Price;
            return (bid, ask);
        }

        private static decimal RoundTick(Security security, decimal price)
        {
            var mpv = security.SymbolProperties.MinimumPriceVariation;
            if (mpv <= 0)
            {
                mpv = 0.01m;
            }
            return Math.Round(price / mpv, MidpointRounding.AwayFromZero) * mpv;
        }

        private static bool IsTerminal(OrderStatus status)
        {
            return status == OrderStatus.Filled || status == OrderStatus.Canceled || status == OrderStatus.Invalid;
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
            if (sorted.Count == 0) return 0;
            if (sorted.Count == 1) return sorted[0];
            var rank = percentile / 100.0 * (sorted.Count - 1);
            var low = (int)Math.Floor(rank);
            var high = (int)Math.Ceiling(rank);
            var weight = rank - low;
            return sorted[low] * (1 - weight) + sorted[high] * weight;
        }

        // ----------------------------------------------------------------------------------------------------
        // Parameter parsing
        // ----------------------------------------------------------------------------------------------------

        private static List<SecurityType> ParseSecurityTypes(string raw)
        {
            var all = new[]
            {
                SecurityType.Equity, SecurityType.Option, SecurityType.IndexOption,
                SecurityType.Future, SecurityType.FutureOption
            };
            if (string.IsNullOrWhiteSpace(raw) || raw.Trim().Equals("All", StringComparison.OrdinalIgnoreCase))
            {
                return all.ToList();
            }

            var result = new List<SecurityType>();
            foreach (var token in raw.Split(',').Select(t => t.Trim()).Where(t => t.Length > 0))
            {
                switch (token.ToLowerInvariant())
                {
                    case "equity": Add(result, SecurityType.Equity); break;
                    case "option":
                    case "equityoption": Add(result, SecurityType.Option); break;
                    case "indexoption": Add(result, SecurityType.IndexOption); break;
                    case "future": Add(result, SecurityType.Future); break;
                    case "futureoption": Add(result, SecurityType.FutureOption); break;
                }
            }
            return result.Count > 0 ? result : new List<SecurityType> { SecurityType.Equity };
        }

        private static List<OrderType> ParseOrderTypes(string raw)
        {
            var all = new[]
            {
                OrderType.Market, OrderType.Limit, OrderType.StopMarket, OrderType.StopLimit, OrderType.ComboLimit
            };
            if (string.IsNullOrWhiteSpace(raw) || raw.Trim().Equals("All", StringComparison.OrdinalIgnoreCase))
            {
                return all.ToList();
            }

            var result = new List<OrderType>();
            foreach (var token in raw.Split(',').Select(t => t.Trim()).Where(t => t.Length > 0))
            {
                switch (token.ToLowerInvariant())
                {
                    case "market": Add(result, OrderType.Market); break;
                    case "limit": Add(result, OrderType.Limit); break;
                    case "stopmarket":
                    case "stop": Add(result, OrderType.StopMarket); break;
                    case "stoplimit": Add(result, OrderType.StopLimit); break;
                    case "combolimit":
                    case "combo": Add(result, OrderType.ComboLimit); break;
                }
            }
            return result.Count > 0 ? result : new List<OrderType> { OrderType.Market, OrderType.Limit };
        }

        private static HashSet<string> ParseSessions(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw) || raw.Trim().Equals("Any", StringComparison.OrdinalIgnoreCase))
            {
                return new HashSet<string> { "open", "midday", "close", "other" };
            }
            var result = new HashSet<string>();
            foreach (var token in raw.Split(',').Select(t => t.Trim().ToLowerInvariant()).Where(t => t.Length > 0))
            {
                result.Add(token);
            }
            return result.Count > 0 ? result : new HashSet<string> { "open", "midday", "close" };
        }

        private static void Add<T>(List<T> list, T value)
        {
            if (!list.Contains(value))
            {
                list.Add(value);
            }
        }

        // ----------------------------------------------------------------------------------------------------
        // Types
        // ----------------------------------------------------------------------------------------------------

        private enum ResolveState { None, Resolving, Ready, Failed }

        private enum CellKind { Single, Resting, Combo, Wave }

        private class TestCell
        {
            public SecurityType Security { get; }
            public OrderType Order { get; }
            public CellKind Kind { get; }
            public string Label => $"{Security}/{Order}";

            public TestCell(SecurityType security, OrderType order, CellKind? kind = null)
            {
                Security = security;
                Order = order;
                Kind = kind ?? KindOf(order);
            }

            private static CellKind KindOf(OrderType order)
            {
                switch (order)
                {
                    case OrderType.StopMarket:
                    case OrderType.StopLimit: return CellKind.Resting;
                    case OrderType.ComboLimit: return CellKind.Combo;
                    default: return CellKind.Single;
                }
            }
        }

        private class LegState
        {
            public int OrderId;
            public int Ratio;                    // signed leg ratio (+1 long/unit, -1 short)
            public TimeSpan PlaceElapsed;
            public TimeSpan? SubmittedElapsed;
            public TimeSpan? TerminalElapsed;
            public OrderStatus FinalStatus;
            public bool IsTerminal;
            public decimal FillPrice;
            public decimal FillQuantity;
            public decimal AnchorTouch;
            public decimal AnchorMid;
        }

        private class Trial
        {
            public TestCell Cell;
            public string Session;
            public int SideSign;                 // +1 buy/debit, -1 sell
            public bool IsResting;
            public bool IsWarmup;
            public bool IsWave;
            public string Tag;
            public DateTime WhenEt;
            public TimeSpan WaveStart;
            public int TerminalCount;
            public readonly List<LegState> Legs = new();
        }

        private class TrialResult
        {
            public string Session;
            public SecurityType Security;
            public OrderType Order;
            public int SideSign;
            public bool Resting;
            public bool Filled;
            public bool HadSubmitted;
            public OrderStatus FinalStatus;
            public double? SubmitMs;
            public double? FillMs;
            public double? ExecMs;
            public double? ResolutionMs;
            public double? SlipTouchBps;
            public double? SlipMidBps;
            public decimal NetFill;
            public decimal NetTouch;
            public decimal NetMid;
            public decimal Contracts;
            public decimal Notional;
            public int Legs;
            public DateTime WhenEt;
        }

        private class WaveResult
        {
            public string Session;
            public int Orders;
            public int Filled;
            public int Failed;
            public double SubmitWallMs;
            public double FillWallMs;
            public double OrdersPerSec;
            public List<double> SubmitLatencies;
            public List<double> FillLatencies;
        }
    }
}
