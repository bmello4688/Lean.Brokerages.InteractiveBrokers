using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    public sealed class HeadTimestampEventArgs : EventArgs
    {
        /// <summary>
        /// The ID of the data request. Ensures that responses are matched to requests if several requests are in process.
        /// </summary>
        public int RequestId { get; }

        /// <summary>
        /// The Datetime of the earliest data point
        /// </summary>
        public DateTime HeadTimestamp { get; }

        public HeadTimestampEventArgs(int reqId, string headTimestamp)
        {
            RequestId = reqId;
            HeadTimestamp = headTimestamp.Length != 8 ?
                Time.UnixTimeStampToDateTime(Convert.ToDouble(headTimestamp, CultureInfo.InvariantCulture)) :
                DateTime.ParseExact(headTimestamp, "yyyyMMdd", CultureInfo.InvariantCulture);
        }
    }
}
