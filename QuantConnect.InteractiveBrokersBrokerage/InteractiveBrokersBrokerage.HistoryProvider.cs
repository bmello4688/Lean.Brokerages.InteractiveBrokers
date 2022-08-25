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

using IBApi;
using NodaTime;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.IBAutomater;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Securities.FutureOption;
using QuantConnect.Securities.Index;
using QuantConnect.Securities.IndexOption;
using QuantConnect.Securities.Option;
using QuantConnect.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bar = QuantConnect.Data.Market.Bar;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;
using Order = QuantConnect.Orders.Order;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// The Interactive Brokers brokerage
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage : IHistoryProvider
    {
        private Dictionary<string, DateTime> contractToEarliestTimeStamp = new();

        public event EventHandler<InvalidConfigurationDetectedEventArgs> InvalidConfigurationDetected;
        public event EventHandler<NumericalPrecisionLimitedEventArgs> NumericalPrecisionLimited;
        public event EventHandler<DownloadFailedEventArgs> DownloadFailed;
        public event EventHandler<ReaderErrorDetectedEventArgs> ReaderErrorDetected;
        public event EventHandler<StartDateLimitedEventArgs> StartDateLimited;

        public void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        public IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            foreach (var request in requests)
            {
                var history = GetHistory(request);

                yield return new Slice(request.StartTimeLocal, history, request.StartTimeUtc);
            }
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of bars covering the span specified in the request</returns>
        /// <remarks>For IB history limitations see https://www.interactivebrokers.com/en/software/api/apiguide/tables/historical_data_limitations.htm </remarks>
        public override IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            if (!IsConnected)
            {
                OnMessage(
                    new BrokerageMessageEvent(
                        BrokerageMessageType.Warning,
                        "GetHistoryWhenDisconnected",
                        "History requests cannot be submitted when disconnected."));
                yield break;
            }

            // skipping universe and canonical symbols
            if (!CanSubscribe(request.Symbol) ||
                (request.Symbol.ID.SecurityType.IsOption() && request.Symbol.IsCanonical()))
            {
                yield break;
            }

            // skip invalid security types
            if (request.Symbol.SecurityType != SecurityType.Equity &&
                request.Symbol.SecurityType != SecurityType.Index &&
                request.Symbol.SecurityType != SecurityType.Forex &&
                request.Symbol.SecurityType != SecurityType.Cfd &&
                request.Symbol.SecurityType != SecurityType.Future &&
                request.Symbol.SecurityType != SecurityType.FutureOption &&
                request.Symbol.SecurityType != SecurityType.Option &&
                request.Symbol.SecurityType != SecurityType.IndexOption)
            {
                yield break;
            }

            // tick resolution not supported for now
            if (request.Resolution == Resolution.Tick)
            {
                // TODO: upgrade IB C# API DLL
                // In IB API version 973.04, the reqHistoricalTicks function has been added,
                // which would now enable us to support history requests at Tick resolution.
                yield break;

            }

            // preparing the data for IB request
            var contract = CreateContract(request.Symbol, true);
            contract.SecType = IB.SecurityType.ContinuousFuture;

            var resolution = ConvertResolution(request.Resolution);
            
            var startTime = request.Resolution == Resolution.Daily ? request.StartTimeUtc.Date : request.StartTimeUtc;
            var endTime = request.Resolution == Resolution.Daily ? request.EndTimeUtc.Date : request.EndTimeUtc;
            var duration = ConvertResolutionToDuration(request.Resolution);

            var earliestHistoryStartTime = GetEarliestHistoryStartTime(request, contract);

            if (startTime < earliestHistoryStartTime)
            {
                startTime = earliestHistoryStartTime;
            }

            //in future
            if(endTime > DateTime.UtcNow)
            {
                yield break;
            }

            Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Submitting request: {request.Symbol.Value} ({GetContractDescription(contract)}): {request.Resolution}/{request.TickType} {startTime} UTC -> {endTime} UTC");

            DateTimeZone exchangeTimeZone;
            if (!_symbolExchangeTimeZones.TryGetValue(request.Symbol, out exchangeTimeZone))
            {
                // read the exchange time zone from market-hours-database
                exchangeTimeZone = MarketHoursDatabase.FromDataFolder().GetExchangeHours(request.Symbol.ID.Market, request.Symbol, request.Symbol.SecurityType).TimeZone;
                _symbolExchangeTimeZones.Add(request.Symbol, exchangeTimeZone);
            }

            IEnumerable<BaseData> history;
            if (request.TickType == TickType.Quote)
            {
                // Quotes need two separate IB requests for Bid and Ask,
                // each pair of TradeBars will be joined into a single QuoteBar
                var historyBid = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, duration, resolution, HistoricalDataType.Bid);
                var historyAsk = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, duration, resolution, HistoricalDataType.Ask);

                history = historyBid.Join(historyAsk,
                    bid => bid.Time,
                    ask => ask.Time,
                    (bid, ask) => new QuoteBar(
                        bid.Time,
                        bid.Symbol,
                        new Bar(bid.Open, bid.High, bid.Low, bid.Close),
                        0,
                        new Bar(ask.Open, ask.High, ask.Low, ask.Close),
                        0,
                        bid.Period));
            }
            else
            {
                // other assets will have TradeBars
                history = GetHistory(request, contract, startTime, endTime, exchangeTimeZone, duration, resolution, HistoricalDataType.Trades);
            }

            // cleaning the data before returning it back to user
            var requestStartTime = request.StartTimeUtc.ConvertFromUtc(exchangeTimeZone);
            var requestEndTime = request.EndTimeUtc.ConvertFromUtc(exchangeTimeZone);

            foreach (var bar in history.Where(bar => bar.Time >= requestStartTime && bar.EndTime <= requestEndTime))
            {
                if (request.Symbol.SecurityType == SecurityType.Equity ||
                    request.ExchangeHours.IsOpen(bar.Time, bar.EndTime, request.IncludeExtendedMarketHours))
                {
                    yield return bar;
                }
            }

            Log.Trace($"InteractiveBrokersBrokerage::GetHistory(): Download completed: {request.Symbol.Value} ({GetContractDescription(contract)})");
        }

        private DateTime GetEarliestHistoryStartTime(HistoryRequest request, Contract contract)
        {
            string contractDescription = GetContractDescription(contract);
            if (contractToEarliestTimeStamp.ContainsKey(contractDescription))
                return contractToEarliestTimeStamp[contractDescription];

            var historicalTicker = GetNextId();

            // This is needed because when useRTH is set to 1, IB will return data only
            // during Equity regular trading hours (for any asset type, not only for equities)
            var useRegularTradingHours = request.Symbol.SecurityType == SecurityType.Equity
                ? Convert.ToInt32(!request.IncludeExtendedMarketHours)
                : 0;

            string dataType = request.TickType == TickType.Quote ? HistoricalDataType.BidAsk : HistoricalDataType.Trades;


            _requestInformation[historicalTicker] = $"[Id={historicalTicker}] GetHeadTimeStamp: {request.Symbol.Value} ({GetContractDescription(contract)})";

            AutoResetEvent getHeadTimestamp = new AutoResetEvent(false);
            DateTime earliestDataPoint = DateTime.UtcNow;
            EventHandler<IB.HeadTimestampEventArgs> clientOnHeadTimestamp = (sender, args) =>
            {
                if (args.RequestId == historicalTicker)
                {
                    earliestDataPoint = args.HeadTimestamp;
                    getHeadTimestamp.Set();
                }
            };

            var pacing = false;
            EventHandler<IB.ErrorEventArgs> clientOnError = (sender, args) =>
            {
                if (args.Id == historicalTicker)
                {
                    if (args.Code == 162 && args.Message.Contains("pacing violation"))
                    {
                        // pacing violation happened
                        pacing = true;
                    }
                    else
                    {
                        getHeadTimestamp.Set();
                    }

                    DownloadFailed?.Invoke(this, new DownloadFailedEventArgs(request.Symbol, args.Message));
                }
            };

            Client.Error += clientOnError;
            Client.HeadTimestamp += clientOnHeadTimestamp;

            CheckRateLimiting();


            Client.ClientSocket.reqHeadTimestamp(historicalTicker, contract, dataType, useRegularTradingHours, 2);

            int timeOut = 60;
            if (!getHeadTimestamp.WaitOne(1000 * timeOut))
            {
                Client.ClientSocket.cancelHeadTimestamp(historicalTicker);
                if (pacing)
                {
                    // we received 'pacing violation' error from IB. So we had to wait
                    Log.Trace("InteractiveBrokersBrokerage::GetEarliestHistoryStartTime() Pacing violation. Paused for {0} secs.", timeOut);
                    earliestDataPoint = GetEarliestHistoryStartTime(request, contract);
                }
                else
                    Log.Trace("InteractiveBrokersBrokerage::GetEarliestHistoryStartTime() HeadTimestamp request timed out ({0} sec)", timeOut);
            }
            else
            {
                if (!contractToEarliestTimeStamp.ContainsKey(contractDescription))
                {
                    contractToEarliestTimeStamp.Add(contractDescription, earliestDataPoint);
                }
                else if(earliestDataPoint > contractToEarliestTimeStamp[contractDescription])
                {
                    contractToEarliestTimeStamp[contractDescription] = earliestDataPoint;
                }
            }

            return earliestDataPoint;
        }

        private IEnumerable<TradeBar> GetHistory(
            HistoryRequest request,
            Contract contract,
            DateTime startTime,
            DateTime endTime,
            DateTimeZone exchangeTimeZone,
            string duration,
            string resolution,
            string dataType)
        {
            const int timeOut = 60; // seconds timeout

            var history = new List<TradeBar>();
            var dataDownloading = new AutoResetEvent(false);
            var dataDownloaded = new AutoResetEvent(false);

            // This is needed because when useRTH is set to 1, IB will return data only
            // during Equity regular trading hours (for any asset type, not only for equities)
            var useRegularTradingHours = request.Symbol.SecurityType == SecurityType.Equity
                ? Convert.ToInt32(!request.IncludeExtendedMarketHours)
                : 0;

            var symbolProperties = _symbolPropertiesDatabase.GetSymbolProperties(request.Symbol.ID.Market, request.Symbol, request.Symbol.SecurityType, Currencies.USD);
            var priceMagnifier = symbolProperties.PriceMagnifier;

            // making multiple requests if needed in order to download the history
            while (endTime >= startTime)
            {
                var pacing = false;
                var historyPiece = new List<TradeBar>();
                var historicalTicker = GetNextId();

                _requestInformation[historicalTicker] = $"[Id={historicalTicker}] GetHistory: {request.Symbol.Value} ({GetContractDescription(contract)})";

                EventHandler<IB.HistoricalDataEventArgs> clientOnHistoricalData = (sender, args) =>
                {
                    if (args.RequestId == historicalTicker)
                    {
                        var bar = ConvertTradeBar(request.Symbol, request.Resolution, args, priceMagnifier);
                        if (request.Resolution != Resolution.Daily)
                        {
                            bar.Time = bar.Time.ConvertFromUtc(exchangeTimeZone);
                        }

                        historyPiece.Add(bar);
                        dataDownloading.Set();
                    }
                };

                EventHandler<IB.HistoricalDataEndEventArgs> clientOnHistoricalDataEnd = (sender, args) =>
                {
                    if (args.RequestId == historicalTicker)
                    {
                        dataDownloaded.Set();
                    }
                };

                EventHandler<IB.ErrorEventArgs> clientOnError = (sender, args) =>
                {
                    if (args.Id == historicalTicker)
                    {
                        if (args.Code == 162 && args.Message.Contains("pacing violation"))
                        {
                            // pacing violation happened
                            pacing = true;
                        }
                        else
                        {
                            dataDownloaded.Set();
                        }

                        DownloadFailed?.Invoke(this, new DownloadFailedEventArgs(request.Symbol, args.Message));
                    }
                };

                Client.Error += clientOnError;
                Client.HistoricalData += clientOnHistoricalData;
                Client.HistoricalDataEnd += clientOnHistoricalDataEnd;

                CheckRateLimiting();

                //Use continous futures for history
                if (contract.SecType == IB.SecurityType.Future)
                {
                    contract.SecType = IB.SecurityType.ContinuousFuture;
                }

                Client.ClientSocket.reqHistoricalData(historicalTicker, contract, endTime.ToStringInvariant("yyyyMMdd HH:mm:ss UTC"),
                   duration, resolution, dataType, useRegularTradingHours, 2, false, new List<TagValue>());

                if (contract.SecType == IB.SecurityType.ContinuousFuture)
                {
                    contract.SecType = IB.SecurityType.Future;
                }

                var waitResult = 0;
                while (waitResult == 0)
                {
                    waitResult = WaitHandle.WaitAny(new WaitHandle[] { dataDownloading, dataDownloaded }, timeOut * 1000);
                }

                Client.Error -= clientOnError;
                Client.HistoricalData -= clientOnHistoricalData;
                Client.HistoricalDataEnd -= clientOnHistoricalDataEnd;

                if (waitResult == WaitHandle.WaitTimeout)
                {
                    if (pacing)
                    {
                        // we received 'pacing violation' error from IB. So we had to wait
                        Log.Trace("InteractiveBrokersBrokerage::GetHistory() Pacing violation. Paused for {0} secs.", timeOut);
                        continue;
                    }

                    Log.Trace("InteractiveBrokersBrokerage::GetHistory() History request timed out ({0} sec)", timeOut);
                    break;
                }

                // if no data has been received this time, we exit
                if (!historyPiece.Any())
                {
                    break;
                }

                var filteredPiece = historyPiece.OrderBy(x => x.Time);

                history.InsertRange(0, filteredPiece);

                var previousEndTime = endTime;

                // moving endTime to the new position to proceed with next request (if needed)
                endTime = filteredPiece.First().Time.ConvertToUtc(exchangeTimeZone);

                if(previousEndTime == endTime)
                {
                    StartDateLimited?.Invoke(this, new StartDateLimitedEventArgs(request.Symbol, $"Data Limited from {history.First().Time} to {history.Last().Time}"));
                    break;
                }

            }

            return history;
        }
    }
}
