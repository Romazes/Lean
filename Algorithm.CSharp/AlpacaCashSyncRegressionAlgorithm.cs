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
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Brokerages;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Live test for <c>PublicBrokerage.GetCashBalance()</c> correctness, driven through the brokerage
    /// cash-sync path.
    ///
    /// At startup Lean seeds the portfolio cash and holdings from the brokerage
    /// (BrokerageSetupHandler -> GetCashBalance / GetAccountHoldings). The base brokerage cash-sync
    /// (Brokerage.PerformCashSync) then re-reads GetCashBalance() and overwrites the Lean cashbook,
    /// logging the delta between the two. By default that runs once a day at 7:45 AM NY; set
    /// "force-cash-sync-seconds" in config.json to make it fire on demand (see Brokerage.ShouldPerformCashSync).
    ///
    /// Scenarios (per TL):
    ///  A) Deploy on an account that already holds positions and place no orders. Every forced sync should
    ///     show no (zero) delta, because nothing changed since startup.
    ///  B) Set "public-cash-test-trade" = true. The algorithm buys a share, then sells it back. After each
    ///     fill the forced sync should show a small or zero delta if Lean and Public model the cash the same way.
    ///  C) Also set "public-cash-test-margin" = true. The buy is sized above settled cash, so the brokerage
    ///     lends and Lean's cash goes negative. The test is whether the brokerage's reported cash goes negative
    ///     to match. A large delta means GetCashBalance() is missing the margin loan.
    ///
    /// The order can also be sent as a resting limit below the ask ("public-cash-test-limit" = true) and/or with
    /// UseMargin off ("public-cash-test-use-margin" = false), to see whether the brokerage rejects a leveraged
    /// order when margin is disabled, without taking a position.
    ///
    /// Set "public-cash-test-option" = true to instead buy one out-of-the-money call on the underlying and close
    /// it ~2 minutes later, checking the premium debit matches Lean (a long option is paid in full). It picks the
    /// most liquid affordable OTM call within "public-cash-test-option-max-cost" and prices a marketable limit so
    /// it fills; the cash dip is recovered when the call is sold back.
    ///
    /// Inspect Launcher/bin/Debug/log.txt for the "CASH-TEST", "Brokerage.PerformCashSync()" and
    /// "ApiClient.GetAccountBalance" lines and line them up by time.
    /// </summary>
    public class PublicCashSyncRegressionAlgorithm : QCAlgorithm
    {
        /// <summary>The equity that scenario B trades.</summary>
        private Symbol _symbol;

        /// <summary>When true the algorithm buys then sells a share (scenario B); when false it only watches (scenario A).</summary>
        private bool _trade;

        /// <summary>Shares to buy and later sell in scenario B (cash mode).</summary>
        private int _tradeQuantity;

        /// <summary>When true the buy is sized above settled cash so the brokerage lends (leveraged-long margin test).</summary>
        private bool _marginMode;

        /// <summary>In margin mode, how much to borrow: the buy notional exceeds settled cash by this amount (account currency).</summary>
        private decimal _borrowTarget;

        /// <summary>The UseMargin order property to send. False makes the order use cash-only buying power.</summary>
        private bool _useMargin;

        /// <summary>When true, place a resting limit buy below the ask instead of a market order, so the accept/reject result is seen without filling.</summary>
        private bool _useLimit;

        /// <summary>How far below the ask the limit price sits, as a fraction (e.g. 0.02 = 2% below).</summary>
        private decimal _limitOffset;

        /// <summary>When true, buy one OTM call on the underlying instead of trading the equity.</summary>
        private bool _optionMode;

        /// <summary>The canonical option symbol subscribed for the chain (option mode).</summary>
        private Symbol _optionSymbol;

        /// <summary>Max cash to spend on the test option (account currency); keeps the premium dip small. Cost = premium x 100.</summary>
        private decimal _optionMaxCost;

        /// <summary>The symbol actually bought (the equity, or the option contract), used to close the position.</summary>
        private Symbol _boughtSymbol;

        /// <summary>Set once the buy has been submitted, so it is submitted only once.</summary>
        private bool _buyPlaced;

        /// <summary>Set once the buy has filled, so the follow-up sell is scheduled only once.</summary>
        private bool _buyFilled;

        /// <summary>Set once the sell has been submitted, so it is submitted only once.</summary>
        private bool _sellPlaced;

        /// <summary>Algorithm time the buy filled, used to wait before selling so a sync runs while long.</summary>
        private DateTime _buyFillTime;

        public PublicCashSyncRegressionAlgorithm()
        {
            Logging.Log.DebuggingEnabled = true;
        }

        public override void Initialize()
        {
            // These are ignored in live mode (real cash and holdings come from the brokerage) but keep the
            // algorithm runnable as a backtest too.
            SetStartDate(2024, 07, 22);
            SetCash(100000);

            SetBenchmark(_ => 1);
            SetBrokerageModel(BrokerageName.Public, AccountType.Margin);

            var ticker = GetParameter("public-cash-test-symbol", "GALT");
            _tradeQuantity = GetParameter("public-cash-test-quantity", 1);
            _trade = GetParameter("public-cash-test-trade", "false").Trim().ToLowerInvariant() == "true";
            _marginMode = GetParameter("public-cash-test-margin", "false").Trim().ToLowerInvariant() == "true";
            _borrowTarget = GetParameter("public-cash-test-borrow", 100m);
            _useMargin = GetParameter("public-cash-test-use-margin", "true").Trim().ToLowerInvariant() == "true";
            _useLimit = GetParameter("public-cash-test-limit", "false").Trim().ToLowerInvariant() == "true";
            _limitOffset = GetParameter("public-cash-test-limit-offset", 0.02m);
            _optionMode = GetParameter("public-cash-test-option", "false").Trim().ToLowerInvariant() == "true";
            _optionMaxCost = GetParameter("public-cash-test-option-max-cost", 200m);

            _symbol = AddEquity(ticker, Resolution.Minute, extendedMarketHours: true).Symbol;

            if (_optionMode)
            {
                // Subscribe to the option chain and keep near-term out-of-the-money calls to pick from. Wide,
                // near-term OTM range so a cheap (low-premium) contract is available.
                var option = AddOption(ticker, Resolution.Minute);
                option.SetFilter(u => u.IncludeWeeklys().CallsOnly().Strikes(+1, +15).Expiration(3, 30));
                _optionSymbol = option.Symbol;
            }

            LogCashState("after startup seed");

            // Re-log the cash state every minute, and in scenario B place the sell once enough time has passed,
            // so the cash sync runs at least once while the position is held.
            Schedule.On(DateRules.EveryDay(), TimeRules.Every(TimeSpan.FromMinutes(1)), OnMinute);
        }

        public override void OnData(Slice slice)
        {
            // Place a single buy once the data is available.
            if (!_trade || _buyPlaced)
            {
                return;
            }

            if (_optionMode)
            {
                TryPlaceOptionBuy(slice);
                return;
            }

            var security = Securities[_symbol];
            //if (security.Price == 0 || !security.Exchange.ExchangeOpen)
            if (security.Price == 0)
            {
                return;
            }

            // Set the flag before submitting so a later slice cannot start a second buy.
            _buyPlaced = true;
            _boughtSymbol = _symbol;
            LogCashState("before buy");

            var price = security.AskPrice > 0 ? security.AskPrice : security.Price;
            PlaceTestBuy(price);
        }

        /// <summary>
        /// Option mode: once the chain is available, buy one near-term out-of-the-money call with a marketable
        /// limit (at the ask). A long call is paid in full, so the premium is a cash debit — the test is whether
        /// the brokerage's reported cash drops by the same premium Lean does.
        /// </summary>
        /// <param name="slice">The current data slice carrying the option chain.</param>
        private void TryPlaceOptionBuy(Slice slice)
        {
            if (!IsMarketOpen(_optionSymbol) || !slice.OptionChains.TryGetValue(_optionSymbol, out var chain))
            {
                return;
            }

            // Most liquid OTM call we can afford: nearest expiry, then closest to the money, capped by max cost
            // (cost = ask x 100). A liquid near-the-money contract fills; the very cheapest deep-OTM ones often do
            // not. Require a bid so the position can be closed.
            var maxAsk = _optionMaxCost / 100m;
            var contract = chain
                .Where(c => c.Right == OptionRight.Call
                            && c.Strike > chain.Underlying.Price
                            && c.AskPrice > 0
                            && c.BidPrice > 0
                            && c.AskPrice <= maxAsk)
                .OrderBy(c => c.Expiry)
                .ThenBy(c => c.Strike)
                .FirstOrDefault();

            if (contract == null)
            {
                return;
            }

            // Set the flag before submitting so a later slice cannot start a second buy.
            _buyPlaced = true;
            _boughtSymbol = contract.Symbol;

            // Price the limit a little above the ask so it crosses and fills (marketable), instead of resting.
            var limitPrice = Math.Round(contract.AskPrice * 1.05m, 2);
            LogCashState("before option buy");
            Log($"CASH-TEST: buying 1 OTM call {contract.Symbol.Value} strike {contract.Strike} exp {contract.Expiry:yyyy-MM-dd} " +
                $"@ {limitPrice:0.00} (ask {contract.AskPrice:0.00}, cost ~{limitPrice * 100m:0.00}), settled cash {Portfolio.CashBook[Currencies.USD].Amount:0.00}");
            LimitOrder(contract.Symbol, 1, limitPrice, tag: "cash-test option buy");
        }

        /// <summary>
        /// Places the test buy: a market order, or — when "public-cash-test-limit" is set — a resting limit buy
        /// below the ask, so the accept/reject result is seen without filling. The UseMargin order property comes
        /// from "public-cash-test-use-margin": false makes the order use cash-only buying power, which on a
        /// leveraged (over-cash) order the brokerage is expected to reject.
        /// </summary>
        /// <param name="referencePrice">The current ask, used to size the order and place the limit below it.</param>
        private void PlaceTestBuy(decimal referencePrice)
        {
            var properties = new PublicOrderProperties { UseMargin = _useMargin, OutsideRegularTradingHours = true };
            var settledCash = Portfolio.CashBook[Currencies.USD].Amount;

            if (_useLimit)
            {
                var limitPrice = Math.Round(referencePrice * (1m - _limitOffset), 2);
                var quantity = _marginMode ? ResolveLeveragedQuantity(limitPrice) : _tradeQuantity;
                Log($"CASH-TEST: placing limit buy {quantity} {_symbol.Value} @ {limitPrice:0.00} (ref ask ~{referencePrice:0.00}), " +
                    $"notional ~{quantity * limitPrice:0.00}, UseMargin={_useMargin}, settled cash {settledCash:0.00}");
                LimitOrder(_symbol, quantity, limitPrice, tag: "cash-test limit buy", orderProperties: properties);
                return;
            }

            var marketQuantity = _marginMode ? ResolveLeveragedQuantity(referencePrice) : _tradeQuantity;
            Log($"CASH-TEST: placing {(_marginMode ? "leveraged " : string.Empty)}market buy {marketQuantity} {_symbol.Value} " +
                $"@ ~{referencePrice:0.00}, notional ~{marketQuantity * referencePrice:0.00}, UseMargin={_useMargin}, settled cash {settledCash:0.00}");
            MarketOrder(_symbol, marketQuantity, tag: _marginMode ? "cash-test margin buy" : "cash-test buy", orderProperties: properties);
        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            Log($"CASH-TEST order event: {orderEvent}");

            if (orderEvent.Status != OrderStatus.Filled)
            {
                return;
            }

            LogCashState($"after {orderEvent.Direction} fill");

            if (orderEvent.Direction == OrderDirection.Buy && !_buyFilled)
            {
                _buyFilled = true;
                _buyFillTime = Time;
            }
        }

        /// <summary>
        /// Runs every minute: logs the current cash state, and in scenario B sells the share back two minutes
        /// after the buy filled so a forced cash sync has run while the position was open.
        /// </summary>
        private void OnMinute()
        {
            LogCashState("scheduled");

            if (_optionMode && _trade && !_buyPlaced)
            {
                Log("CASH-TEST: waiting for the option chain (no contract bought yet).");
            }

            if (_trade && _buyFilled && !_sellPlaced && Time - _buyFillTime >= TimeSpan.FromMinutes(2))
            {
                _sellPlaced = true;
                LogCashState("before sell");

                // Close whatever was bought (equity, option, or a margin loan to repay) back to flat.
                var quantity = Portfolio[_boughtSymbol].Quantity;
                if (_optionMode)
                {
                    // Marketable limit a little below the bid so the close crosses and fills.
                    var bid = Securities[_boughtSymbol].BidPrice;
                    var reference = bid > 0 ? bid : Securities[_boughtSymbol].Price;
                    var limitPrice = Math.Round(Math.Max(0.01m, reference * 0.95m), 2);
                    LimitOrder(_boughtSymbol, -quantity, limitPrice, tag: "cash-test option sell");
                }
                else
                {
                    MarketOrder(_boughtSymbol, -quantity, tag: "cash-test sell");
                }
            }
        }

        /// <summary>
        /// Sizes the leveraged buy so its notional is the settled cash plus the borrow target, which forces the
        /// brokerage to lend the difference. Lean models that loan as negative cash; the test is whether the
        /// brokerage's reported cash goes negative to match.
        /// </summary>
        /// <param name="price">The price used to size the order.</param>
        /// <returns>The share quantity to buy.</returns>
        private int ResolveLeveragedQuantity(decimal price)
        {
            var settledCash = Portfolio.CashBook[Currencies.USD].Amount;
            var targetNotional = settledCash + _borrowTarget;
            return (int)Math.Ceiling(targetNotional / price);
        }

        /// <summary>
        /// Logs the Lean view of cash and holdings. Compare the "Lean USD cash" value against the brokerage
        /// cash in the nearby "ApiClient.GetAccountBalance" line and the "Brokerage.PerformCashSync(): USD Delta"
        /// line — they should agree (zero/near-zero delta) when GetCashBalance() is correct.
        /// </summary>
        /// <param name="context">A short label describing when this snapshot was taken.</param>
        private void LogCashState(string context)
        {
            var usd = Portfolio.CashBook[Currencies.USD].Amount;
            var tracked = _boughtSymbol ?? _symbol;
            var holdingsQuantity = Portfolio[tracked].Quantity;
            Log($"CASH-TEST ({context}): Lean USD cash = {usd:0.00}, TPV = {Portfolio.TotalPortfolioValue:0.00}, " +
                $"{tracked.Value} holdings = {holdingsQuantity}");
        }

        public override void OnEndOfAlgorithm()
        {
            Transactions.CancelOpenOrders();
            LogCashState("end of algorithm");
        }
    }
}
