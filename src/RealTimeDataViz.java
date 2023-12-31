import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Tooltip;
import javafx.stage.Stage;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.chart.CategoryAxis; // Import CategoryAxis

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RealTimeDataViz extends Application {

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Visualisation des données en temps réel");

        // Create Category and Number axes for the chart
        CategoryAxis xAxis = new CategoryAxis(); // Change to CategoryAxis
        NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("Date et Temps");
        yAxis.setLabel("Taux de Change");

        // Create the chart using the axes
        LineChart<String, Number> lineChart = new LineChart<>(xAxis, yAxis); // Change to LineChart<String, Number>
        lineChart.setTitle("Données en temps réel");
        lineChart.setCreateSymbols(false); // Disable default symbols

        // Create a data series for the chart
        XYChart.Series<String, Number> series = new XYChart.Series<>(); // Change to XYChart.Series<String, Number>
        series.setName("Taux de Change");

        // Add the series to the chart
        lineChart.getData().add(series);

        // Set up the chart appearance and features
        String seriesStyle = "-fx-stroke: #007ACC;"; // Blue color for the series
        series.getNode().setStyle(seriesStyle);

        // Show the main window
        Scene scene = new Scene(lineChart, 800, 600);
        primaryStage.setScene(scene);
        primaryStage.show();

        // Use Exchange Rate data
        ExchangeRateAPISource APISource = new ExchangeRateAPISource();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            double newValue = APISource.getRealTimeData();
            long timestamp = System.currentTimeMillis();

            // Convert timestamp to a readable date format
            String formattedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));

            // Add data to the chart
            Platform.runLater(() -> {
                series.getData().add(new XYChart.Data<>(formattedDate, newValue));

                if (series.getData().size() > 100) {
                    series.getData().remove(0);
                }
            });
        }, 0, 1, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        launch(args);
    }
}
