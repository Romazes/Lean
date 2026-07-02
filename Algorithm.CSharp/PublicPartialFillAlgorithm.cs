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
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Orders;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Grid test for the Public.com brokerage.
    ///
    /// The algorithm keeps ten working orders on GETY at all times. Each buy is a one-share limit kept four
    /// cents below the ask, so it tracks the market as the ask moves. When a buy fills it is replaced by a
    /// sell limit at the fill price plus the regulator fee and a two-cent profit, so every exit stays in
    /// profit after the fee. When a sell fills its slot is refilled with a new buy, so the pool always holds
    /// ten orders. If the algorithm starts with a position, it first places one sell to exit that position,
    /// which takes one slot, and fills the other nine with buys.
    /// </summary>
    public class PublicPartialFillAlgorithm : QCAlgorithm
    {
        public PublicPartialFillAlgorithm()
        {
            Logging.Log.DebuggingEnabled = true;
        }

        /// <summary>
        /// The number of working orders the algorithm keeps at all times.
        /// </summary>
        private const int PoolSize = 10;

        /// <summary>
        /// The number of shares per buy order.
        /// </summary>
        private const int ShareSize = 1;

        /// <summary>
        /// How far below the ask the buy limit is kept.
        /// </summary>
        private const decimal BuyOffset = 0.04m;

        /// <summary>
        /// The regulator fee on GETY. It is charged on sell orders only, so the sell price adds it back to stay in profit.
        /// </summary>
        private const decimal RegulatorFee = 0.02m;

        /// <summary>
        /// The profit added on top of the buy price when the take-profit sell is placed.
        /// </summary>
        private const decimal TargetProfit = 0.02m;

        /// <summary>
        /// The equity the orders are placed on.
        /// </summary>
        private Symbol _symbol;

        /// <summary>
        /// Set once the starting position has been checked, so it is only checked one time.
        /// </summary>
        private bool _existingHoldingHandled;

        public override void Initialize()
        {
            SetStartDate(2024, 07, 22);
            SetEndDate(2024, 07, 26);
            SetCash(100000);

            SetBenchmark(_ => 1);

            SetBrokerageModel(BrokerageName.Public);

            _symbol = AddEquity("GETY", Resolution.Minute).Symbol;
        }

        public override void OnData(Slice slice)
        {
            var ask = Securities[_symbol].AskPrice;

            // Wait for a price, so the limits can be based on the real ask.
            if (ask == 0)
            {
                return;
            }

            // One time: if we started with a position, exit it with a single sell that takes one slot in the pool.
            if (!_existingHoldingHandled)
            {
                _existingHoldingHandled = true;
                SellExistingHolding();
            }

            RepriceOpenBuyOrders(ask);
            MaintainOrderPool(ask);
        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            Log($"{Time}: {orderEvent}");

            if (orderEvent.Status != OrderStatus.Filled)
            {
                return;
            }

            if (orderEvent.Direction == OrderDirection.Buy)
            {
                // A buy filled, so place a take-profit sell for the shares we just bought.
                PlaceSellOrder(orderEvent.FillQuantity, orderEvent.FillPrice);
            }

            // When a sell fills its slot frees up; MaintainOrderPool refills it with a new buy on the next slice.
        }

        /// <summary>
        /// Places one sell to exit the starting position, if there is one.
        /// </summary>
        private void SellExistingHolding()
        {
            var holding = Securities[_symbol].Holdings;
            if (holding.Quantity <= 0)
            {
                return;
            }

            PlaceSellOrder(holding.Quantity, holding.AveragePrice);
        }

        /// <summary>
        /// Adds new buy limits until the pool holds ten orders again.
        /// </summary>
        private void MaintainOrderPool(decimal ask)
        {
            var openOrderCount = Transactions.GetOpenOrderTickets(_symbol).Count();
            var missing = PoolSize - openOrderCount;
            if (missing <= 0)
            {
                return;
            }

            var limitPrice = Math.Round(ask - BuyOffset, 2);
            for (var i = 0; i < missing; i++)
            {
                LimitOrder(_symbol, ShareSize, limitPrice, tag: "Grid buy");
            }

            Log($"{Time}: placed {missing} buy limit(s) at {limitPrice} (ask {ask})");
        }

        /// <summary>
        /// Keeps every open buy four cents below the ask, so the gap to the market stays small.
        /// </summary>
        private void RepriceOpenBuyOrders(decimal ask)
        {
            var limitPrice = Math.Round(ask - BuyOffset, 2);
            foreach (var ticket in Transactions.GetOpenOrderTickets(_symbol))
            {
                // Only the working buys track the ask. The sells stay at their fixed take-profit price.
                if (ticket.Quantity > 0 && ticket.Get(OrderField.LimitPrice) != limitPrice)
                {
                    ticket.UpdateLimitPrice(limitPrice, $"Reprice to {limitPrice}");
                }
            }
        }

        /// <summary>
        /// Places a take-profit sell at the base price plus the regulator fee and the target profit.
        /// </summary>
        private void PlaceSellOrder(decimal quantity, decimal basePrice)
        {
            var limitPrice = Math.Round(basePrice + RegulatorFee + TargetProfit, 2);
            LimitOrder(_symbol, -quantity, limitPrice, tag: $"Take profit at {limitPrice}");
            Log($"{Time}: placed sell limit for {quantity} share(s) at {limitPrice} (base {basePrice})");
        }
    }
}
