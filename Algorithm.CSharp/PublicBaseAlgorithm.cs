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
    /// Keeps nine working orders on a single equity on the Public.com brokerage and ping-pongs between
    /// entering long and taking profit.
    ///
    /// Each bar the order book is rebuilt to nine working orders: every share held gets a sell ("short") limit
    /// a few cents above its cost to take profit, and the remaining slots are filled with long-entry orders -
    /// limit, stop-market and stop-limit in rotation. Because the book is reconciled from the live position and
    /// open orders, a fill, cancellation or rejection just frees a slot that is refilled on the next bar, so
    /// there are always nine open orders no matter which side they are on.
    /// </summary>
    public class PublicBaseAlgorithm : QCAlgorithm
    {
        public PublicBaseAlgorithm()
        {
            Logging.Log.DebuggingEnabled = true;
        }

        /// <summary>
        /// Number of working orders to keep at all times.
        /// </summary>
        private const int OrderCount = 9;

        /// <summary>
        /// Shares to trade with each order.
        /// </summary>
        private const int OrderQuantity = 1;

        /// <summary>
        /// How far below the ask a limit buy is placed.
        /// </summary>
        private const decimal BuyLimitOffset = 0.04m;

        /// <summary>
        /// How far above the ask a stop buy triggers.
        /// </summary>
        private const decimal StopOffset = 0.04m;

        /// <summary>
        /// How far above the stop trigger the limit of a stop-limit buy is placed.
        /// </summary>
        private const decimal StopLimitOffset = 0.02m;

        /// <summary>
        /// How far above the entry cost the profit sell is placed.
        /// </summary>
        private const decimal ProfitOffset = 0.05m;

        /// <summary>
        /// The equity to trade.
        /// </summary>
        private Symbol _symbol;

        /// <summary>
        /// Rotates through the three entry order types when a new long entry is placed.
        /// </summary>
        private int _nextEntryType;

        public override void Initialize()
        {
            SetStartDate(2024, 07, 22);
            SetEndDate(2024, 07, 26);
            SetCash(100000);

            SetBenchmark(_ => 1);

            SetBrokerageModel(BrokerageName.Public, AccountType.Margin);

            _symbol = AddEquity("GALT", Resolution.Minute).Symbol;
        }

        public override void OnData(Slice slice)
        {
            // Wait until the symbol has a price, so the orders are based on the market and pass the order checks.
            if (Securities[_symbol].Price == 0 || Securities[_symbol].AskPrice == 0)
            {
                return;
            }

            var ask = Securities[_symbol].AskPrice;
            var openTickets = Transactions.GetOpenOrderTickets(_symbol).ToList();

            // Keep every open entry a few cents from the market so it has a chance to fill as the price moves.
            // Profit sells are left at their target price.
            foreach (var ticket in openTickets)
            {
                if (ticket.Quantity > 0)
                {
                    RepriceEntry(ticket, ask);
                }
            }

            var openBuys = openTickets.Count(ticket => ticket.Quantity > 0);
            var openSells = openTickets.Count(ticket => ticket.Quantity < 0);

            // Every share we hold needs a profit ("short") sell. Re-place one for any share left uncovered
            // because its sell was filled-and-replaced elsewhere, canceled or rejected.
            var held = (int)Securities[_symbol].Holdings.Quantity;
            for (; openSells < held; openSells++)
            {
                var profitPrice = Math.Round(Securities[_symbol].Holdings.AveragePrice + ProfitOffset, 2);
                PlaceProfitSell(profitPrice);
            }

            // Top the working orders back up to nine with fresh long entries. This also places the first batch
            // and refills any slot freed by a fill, cancellation or rejection.
            for (var working = openBuys + openSells; working < OrderCount; working++)
            {
                PlaceEntry($"Entry {_symbol.Value}", ask);
            }
        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            // Order management is handled by rebuilding the book each bar in OnData, so just log here.
            Log($"{Time}: {orderEvent}");
        }

        public override void OnEndOfAlgorithm()
        {
            Transactions.CancelOpenOrders();
        }

        /// <summary>
        /// Places one long-entry order, cycling limit -> stop-market -> stop-limit so the three types stay balanced.
        /// </summary>
        private void PlaceEntry(string tag, decimal ask)
        {
            var pop = new PublicOrderProperties { TimeInForce = TimeInForce.Day };
            OrderTicket ticket;

            switch (_nextEntryType++ % 3)
            {
                case 0:
                    var limitPrice = Math.Round(ask - BuyLimitOffset, 2);
                    ticket = LimitOrder(_symbol, OrderQuantity, limitPrice, tag: tag, orderProperties: pop);
                    break;

                case 1:
                    var stopPrice = Math.Round(ask + StopOffset, 2);
                    ticket = StopMarketOrder(_symbol, OrderQuantity, stopPrice, tag: tag, orderProperties: pop);
                    break;

                default:
                    var stop = Math.Round(ask + StopOffset, 2);
                    var limit = Math.Round(stop + StopLimitOffset, 2);
                    ticket = StopLimitOrder(_symbol, OrderQuantity, stop, limit, tag: tag, orderProperties: pop);
                    break;
            }

            Log($"{Time}: submitted {ticket.OrderType} entry {ticket.OrderId} for {OrderQuantity} share of {_symbol}");
        }

        /// <summary>
        /// Places the profit ("short") sell that closes one held share above its cost.
        /// </summary>
        private void PlaceProfitSell(decimal profitPrice)
        {
            var pop = new PublicOrderProperties { TimeInForce = TimeInForce.Day };
            LimitOrder(_symbol, -OrderQuantity, profitPrice, tag: $"Take profit at {profitPrice}", orderProperties: pop);
            Log($"{Time}: placed profit sell for {OrderQuantity} share at {profitPrice}");
        }

        /// <summary>
        /// Moves an open entry back to its offset from the current ask, matching how each order type triggers.
        /// </summary>
        private void RepriceEntry(OrderTicket ticket, decimal ask)
        {
            switch (ticket.OrderType)
            {
                case OrderType.Limit:
                    var limitPrice = Math.Round(ask - BuyLimitOffset, 2);
                    if (ticket.Get(OrderField.LimitPrice) != limitPrice)
                    {
                        ticket.UpdateLimitPrice(limitPrice, $"Reprice limit to {limitPrice}");
                    }
                    break;

                case OrderType.StopMarket:
                    var stopPrice = Math.Round(ask + StopOffset, 2);
                    if (ticket.Get(OrderField.StopPrice) != stopPrice)
                    {
                        ticket.UpdateStopPrice(stopPrice, $"Reprice stop to {stopPrice}");
                    }
                    break;

                case OrderType.StopLimit:
                    var stop = Math.Round(ask + StopOffset, 2);
                    var limit = Math.Round(stop + StopLimitOffset, 2);
                    if (ticket.Get(OrderField.StopPrice) != stop || ticket.Get(OrderField.LimitPrice) != limit)
                    {
                        ticket.Update(new UpdateOrderFields { StopPrice = stop, LimitPrice = limit, Tag = $"Reprice stop-limit to {stop}/{limit}" });
                    }
                    break;
            }
        }
    }
}
