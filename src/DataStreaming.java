import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;

public class DataStreaming {

    public static void main(String[] args) throws Exception {
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
        DataStream<Double> resultStream = env.addSource(new AlphaVantageAPI(rapidApiKey, rapidApiEndpoint))
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        // Print the results to the console
        resultStream.print();

        // Apply sliding window operation (10 seconds window, sliding every 5 seconds)
        DataStream<Double> windowedStream = resultStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new SlidingWindowFunction());

        // Print the results of the sliding window to the console
        windowedStream.print();

        // Execute the job
        env.execute("Bitcoin Exchange Streaming");
    }

    private static class AlphaVantageAPI implements SourceFunction<Double> {
        private String apiKey;
        private String apiUrl;

        public AlphaVantageAPI(String apiKey, String apiUrl) {
            this.apiKey = apiKey;
            this.apiUrl = apiUrl;
        }

        @Override
        public void run(SourceFunction.SourceContext<Double> ctx) throws Exception {
            while (true) {
                try {
                    double exchangeRate = makeHttpRequest(apiKey, apiUrl);
                    ctx.collect(exchangeRate);
                } catch (Exception e) {
                    // Log the exception and continue with the next iteration
                    e.printStackTrace();
                }

                // Sleep for 10 seconds before the next request
                Thread.sleep(10000);
            }
        }

        @Override
        public void cancel() {
            // Implement cancellation logic if needed
        }

        private double makeHttpRequest(String apiKey, String apiUrl) throws Exception {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("X-RapidAPI-Key", apiKey);

            int responseCode = connection.getResponseCode();
            // Print the API Response Code
            // System.out.println("API Response Code: " + responseCode);

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    StringBuilder response = new StringBuilder();
                    String line;

                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }

                    // Parse the exchange rate from the JSON response
                    return parseExchangeRate(response.toString());
                } // Closing brace for the try-with-resources block
            } else if (responseCode == 429) {
                // If you get a 429 response, wait for a minute and try again
                System.out.println("Rate limit exceeded. Waiting for a minute...");
                Thread.sleep(60000);
                return makeHttpRequest(apiKey, apiUrl); // Retry the request
            } else {
                // Handle other response codes as needed
                System.out.println("HTTP request failed with response code: " + responseCode);
                return 0.0; // Provide a default value or adjust the method signature accordingly
            }
        }

        private double parseExchangeRate(String jsonResponse) throws Exception {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(jsonResponse);

                // Navigate through the JSON structure to get the exchange rate
                JsonNode realTimeExchangeRateNode = jsonNode.get("Realtime Currency Exchange Rate");

                if (realTimeExchangeRateNode != null) {
                    JsonNode exchangeRateNode = realTimeExchangeRateNode.get("5. Exchange Rate");

                    if (exchangeRateNode != null) {
                        return exchangeRateNode.asDouble();
                    } else {
                        // Log the actual JSON response for debugging
                        System.out.println("Invalid JSON structure. Missing key '5. Exchange Rate'. Response: " + jsonResponse);
                        throw new Exception("Key '5. Exchange Rate' not found in JSON response.");
                    }
                } else {
                    // Log the actual JSON response for debugging
                    System.out.println("Invalid JSON structure. Missing key 'Realtime Currency Exchange Rate'. Response: " + jsonResponse);
                    throw new Exception("Key 'Realtime Currency Exchange Rate' not found in JSON response.");
                }
            } catch (Exception e) {
                // Log the exception for debugging
                System.out.println("Error parsing JSON response: " + e.getMessage() + ". Response: " + jsonResponse);
                throw new Exception("Error parsing JSON response", e);
            }
        }
    }

    private static class SlidingWindowFunction implements org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<Double, Double, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
        @Override
        public void apply(org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Iterable<Double> values, org.apache.flink.util.Collector<Double> out) throws Exception {
            // Perform computations on the elements in the sliding window
            // For simplicity, just sum the values and emit the result
            double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }
            out.collect(sum);
        }
    }
}
