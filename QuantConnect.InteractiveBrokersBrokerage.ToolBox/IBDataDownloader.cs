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
using QuantConnect.Util;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Brokerages.InteractiveBrokers;
using System.Diagnostics;
using System.Threading;

namespace QuantConnect.ToolBox.IBDownloader
{
    /// <summary>
    /// IB Downloader class
    /// </summary>
    public class IBDataDownloader : IDataDownloader, IDisposable
    {
        private InteractiveBrokersBrokerage _brokerage;

        /// <summary>
        /// Initializes a new instance of the <see cref="IBDataDownloader"/> class
        /// </summary>
        public IBDataDownloader()
        {

            
        }

        private void Init()
        {
            _brokerage = InteractiveBrokersBrokerage.Instance;

            if (_brokerage == null)
            {
                Stopwatch stopwatch = new();
                stopwatch.Start();

                while (_brokerage == null && stopwatch.ElapsedMilliseconds < 1000)
                {
                    _brokerage = InteractiveBrokersBrokerage.Instance;
                    Thread.Sleep(10);
                }

                if (_brokerage == null)
                {
                    new InteractiveBrokersBrokerageFactory().CreateBrokerage(null, null);
                    _brokerage = InteractiveBrokersBrokerage.Instance;
                    _brokerage.Connect();
                }
            }
        }


        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">model class for passing in parameters for historical data</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {

            Init();

            var symbol = dataDownloaderGetParameters.Symbol;
            var resolution = dataDownloaderGetParameters.Resolution;
            var startUtc = dataDownloaderGetParameters.StartUtc;
            var endUtc = dataDownloaderGetParameters.EndUtc;
            var tickType = dataDownloaderGetParameters.TickType;

            if (resolution == Resolution.Tick)
            {
                throw new NotSupportedException("Resolution not available: " + resolution);
            }

            if (endUtc < startUtc)
            {
                throw new ArgumentException("The end date must be greater or equal than the start date.");
            }

            var symbols = new List<Symbol>{ symbol };
            if (symbol.IsCanonical())
            {
                symbols = GetChainSymbols(symbol, true).ToList();
            }

            if (symbol.SecurityType == SecurityType.Future)
            {
                //keep first
                symbols.RemoveRange(1, symbols.Count - 1);
            }

            var exchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = MarketHoursDatabase.FromDataFolder().GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            foreach (var targetSymbol in symbols)
            {
                var historyRequest = new HistoryRequest(startUtc,
                    endUtc,
                    tickType == TickType.Quote ? typeof(QuoteBar) :
                    tickType == TickType.Trade ? typeof(TradeBar) :
                    typeof(OpenInterest),
                    targetSymbol,
                    resolution,
                    exchangeHours: exchangeHours,
                    dataTimeZone: dataTimeZone,
                    resolution,
                    includeExtendedMarketHours: true,
                    false,
                    DataNormalizationMode.Adjusted,
                    tickType);

                foreach (var baseData in _brokerage.GetHistory(historyRequest))
                {
                    yield return baseData;
                }
            }
        }

        /// <summary>
        /// Returns an IEnumerable of Future/Option contract symbols for the given root ticker
        /// </summary>
        /// <param name="symbol">The Symbol to get futures/options chain for</param>
        /// <param name="includeExpired">Include expired contracts</param>
        public IEnumerable<Symbol> GetChainSymbols(Symbol symbol, bool includeExpired)
        {
            Init();

            return _brokerage.LookupSymbols(symbol, includeExpired);
        }

        /// <summary>
        /// Downloads historical data from the brokerage and saves it in LEAN format.
        /// </summary>
        /// <param name="symbols">The list of symbols</param>
        /// <param name="tickType">The tick type</param>
        /// <param name="resolution">The resolution</param>
        /// <param name="securityType">The security type</param>
        /// <param name="startTimeUtc">The starting date/time (UTC)</param>
        /// <param name="endTimeUtc">The ending date/time (UTC)</param>
        public void DownloadAndSave(List<Symbol> symbols, Resolution resolution, SecurityType securityType, TickType tickType, DateTime startTimeUtc, DateTime endTimeUtc)
        {
            Init();

            var writer = new LeanDataWriter(Globals.DataFolder, resolution, securityType, tickType);
            writer.DownloadAndSave(_brokerage, symbols, startTimeUtc, endTimeUtc);
        }

        /// <summary>
        /// Groups a list of bars into a dictionary keyed by date
        /// </summary>
        /// <param name="bars"></param>
        /// <returns></returns>
        private static SortedDictionary<DateTime, List<QuoteBar>> GroupBarsByDate(IList<QuoteBar> bars)
        {
            var groupedBars = new SortedDictionary<DateTime, List<QuoteBar>>();

            foreach (var bar in bars)
            {
                var date = bar.Time.Date;

                if (!groupedBars.ContainsKey(date))
                    groupedBars[date] = new List<QuoteBar>();

                groupedBars[date].Add(bar);
            }

            return groupedBars;
        }

        #region Console Helper

        /// <summary>
        /// Draw a progress bar
        /// </summary>
        /// <param name="complete"></param>
        /// <param name="maxVal"></param>
        /// <param name="barSize"></param>
        /// <param name="progressCharacter"></param>
        private static void ProgressBar(long complete, long maxVal, long barSize, char progressCharacter)
        {

            decimal p = (decimal)complete / (decimal)maxVal;
            int chars = (int)Math.Floor(p / ((decimal)1 / (decimal)barSize));
            string bar = string.Empty;
            bar = bar.PadLeft(chars, progressCharacter);
            bar = bar.PadRight(Convert.ToInt32(barSize) - 1);

            Console.Write($"\r[{bar}] {(p * 100).ToStringInvariant("N2")}%");
        }

        #endregion

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_brokerage != null)
            {
                _brokerage.Disconnect();
                _brokerage.Dispose();
            }
        }
    }
}
