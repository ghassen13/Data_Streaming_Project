import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.concurrent.CountDownLatch;

public class RealTimeDataViz extends Application {

    private static final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Real-Time Data Visualization");

        // Create Number axes for the chart
        NumberAxis xAxis = new NumberAxis(); // Use NumberAxis for timestamp
        NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("Timestamp");
        yAxis.setLabel("Exchange Rate");

        // Create the chart using the axes
        LineChart<Number, Number> lineChart = new LineChart<>(xAxis, yAxis);
        lineChart.setTitle("Real-Time Data");
        lineChart.setCreateSymbols(false); // Disable default symbols

        // Create a data series for the chart
        XYChart.Series<Number, Number> series = new XYChart.Series<>();
        series.setName("Exchange Rate");

        // Add the series to the chart
        lineChart.getData().add(series);

        // Set up the chart appearance and features
        String seriesStyle = "-fx-stroke: #007ACC;"; // Blue color for the series
        series.getNode().setStyle(seriesStyle);

        // Show the main window
        Scene scene = new Scene(lineChart, 800, 600);
        primaryStage.setScene(scene);
        primaryStage.show();

        // Wait for the latch to be counted down
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Use Exchange Rate data from Flink job
        DataStream<Double> resultStream = DataStreaming.resultStream; // Replace with the actual method to get the DataStream

        if (resultStream == null) {
            System.out.println("resultStream is null. Make sure DataStreaming job is running.");
        } else {
            System.out.println("resultStream is not null. Proceeding with chart updates.");

            // Add a sink to update the JavaFX chart
            resultStream.map(value -> {
                Platform.runLater(() -> {
                    series.getData().add(new XYChart.Data<>(System.currentTimeMillis(), value));
                });
                return value;
            }).print(); // Add a print statement for logging (you can remove it in production)
        }
    }

    public static void main(String[] args) {
        // Launch Flink job first
        Thread flinkJobThread = new Thread(() -> {
            try {
                DataStreaming.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Count down the latch when Flink job is ready
                latch.countDown();
            }
        });

        // Start Flink job thread
        flinkJobThread.start();

        // Launch JavaFX application
        launch(args);
    }
}
