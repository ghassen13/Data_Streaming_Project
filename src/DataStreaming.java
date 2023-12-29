import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class DataStreaming {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Replace YOUR_RAPIDAPI_KEY and YOUR_RAPIDAPI_ENDPOINT with your actual RapidAPI key and endpoint
        String rapidApiKey = "a87319b68cmsh175e9f86cb40e90p190f6cjsn8572d0df81a4";
        String fromCurrency = "BTC";
        String toCurrency = "TND";
        String function = "CURRENCY_EXCHANGE_RATE";
        String rapidApiEndpoint = "https://alpha-vantage.p.rapidapi.com/query?from_currency=" + fromCurrency + "&function=" + function + "&to_currency=" + toCurrency;
        System.out.println("API Request URL: " + rapidApiEndpoint);

        // Create a data stream by making an HTTP request to the RapidAPI endpoint
        DataStream<String> resultStream = env.fromElements(makeHttpRequest(rapidApiKey, rapidApiEndpoint));

        // Print the results to the console
        resultStream.print();

        // Apply sliding window operation (10 elements window, sliding every 5 elements)
        // Window Size (Length): 10 seconds
        // Sliding Interval: 5 seconds
        DataStream<String> windowedStream = resultStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new SlidingWindowFunction());

        // Print the results of the sliding window to the console
        windowedStream.print();

        // Execute the job
        env.execute("Bitcoin Exchange Streaming");
        while (true) {
            Thread.sleep(10000);
        }
    }

    private static String makeHttpRequest(String rapidApiKey, String rapidApiEndpoint) throws Exception {
        URL url = new URL(rapidApiEndpoint);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("X-RapidAPI-Key", rapidApiKey);

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                // Parsing the JSON response and extracting the exchange rate using Jackson
                String jsonResponse = response.toString();
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(jsonResponse);

                // Navigate through the JSON structure to get the exchange rate
                JsonNode realTimeExchangeRateNode = jsonNode.get("Realtime Currency Exchange Rate");

                // Check if the node is not null before attempting to extract the value
                if (realTimeExchangeRateNode != null) {
                    JsonNode exchangeRateNode = realTimeExchangeRateNode.get("5. Exchange Rate");

                    if (exchangeRateNode != null) {
                        double rate = exchangeRateNode.asDouble();
                        // Printing the exchange rate
                        System.out.println("Exchange rate: " + rate);
                        return jsonResponse; // Return the value if needed
                    } else {
                        System.out.println("Key '5. Exchange Rate' not found in JSON response.");
                    }
                } else {
                    System.out.println("Key 'Realtime Currency Exchange Rate' not found in JSON response.");
                }
            } // Closing brace for the try-with-resources block
        } else if (responseCode == 429) {
            // If we exceed the API limit, wait for a minute and try again
            Thread.sleep(60000);
            return makeHttpRequest(rapidApiKey, rapidApiEndpoint); // Retry the request
        } else {
            throw new IOException("HTTP request failed with response code: " + responseCode);
        }

        // Provide a default return value or adjust the method signature accordingly
        return null;
    }

    private static class SlidingWindowFunction implements org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
        @Override
        public void apply(org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Iterable<String> values, org.apache.flink.util.Collector<String> out) throws Exception {
            // Perform computations on the elements in the sliding window
            // For simplicity, just concatenate the elements and emit the result
            StringBuilder resultBuilder = new StringBuilder();
            for (String value : values) {
                resultBuilder.append(value).append(" ");
            }
            out.collect("Sliding Window Result: " + resultBuilder.toString().trim());

            // Introduce a sleep of 15 seconds between windows
            Thread.sleep(15000);
        }
    }
}
