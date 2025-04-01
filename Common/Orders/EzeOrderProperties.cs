/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.0. Copyright 2023 QuantConnect Corporation.
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

namespace QuantConnect.Orders
{
    /// <summary>
    /// Contains additional properties and settings for an order submitted to EZE brokerage
    /// </summary>
    public class EzeOrderProperties : OrderProperties
    {
        /// <summary>
        /// User message/notes
        /// </summary>
        public string Notes { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="EzeOrderProperties"/> class
        /// </summary>
        /// <param name="exchange">Exchange name</param>
        /// <param name="notes">Some notes about order</param>
        public EzeOrderProperties(Exchange exchange, string notes = "") : base(exchange)
        {
            Notes = notes;
        }
    }
}
