import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExchangeRateAPISource {

    public static DataStream<Double> getResultStream() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the time characteristic to EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Replace YOUR_RAPIDAPI_KEY and YOUR_RAPIDAPI_ENDPOINT with your actual RapidAPI key and endpoint
        String rapidApiKey = "a87319b68cmsh175e9f86cb40e90p190f6cjsn8572d0df81a4";
        String fromCurrency = "BTC";
        String toCurrency = "TND";
        String function = "CURRENCY_EXCHANGE_RATE";
        String rapidApiEndpoint = "https://alpha-vantage.p.rapidapi.com/query?from_currency=" + fromCurrency + "&function=" + function + "&to_currency=" + toCurrency;
        System.out.println("API Request URL: " + rapidApiEndpoint);

        // Create a data stream by making an HTTP request to the RapidAPI endpoint
        return env.addSource(new DataStreaming.AlphaVantageAPI(rapidApiKey, rapidApiEndpoint))
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
    }
}
