import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

public class LinePlotExample extends Application {

    @Override
    public void start(Stage stage) {
        // Create the X and Y axes
        NumberAxis xAxis = new NumberAxis();
        NumberAxis yAxis = new NumberAxis();

        // Create the line chart
        LineChart<Number, Number> lineChart = new LineChart<>(xAxis, yAxis);
        lineChart.setTitle("Line Plot Example");

        // Create a data series
        XYChart.Series<Number, Number> series = new XYChart.Series<>();
        series.setName("Random Data");

        // Add data to the series
        for (int i = 0; i < 10; i++) {
            series.getData().add(new XYChart.Data<>(i, Math.random() * 100));
        }

        // Add the series to the chart
        lineChart.getData().add(series);

        // Create the scene and set it on the stage
        Scene scene = new Scene(lineChart, 600, 400);
        stage.setScene(scene);

        // Set the stage title and show it
        stage.setTitle("JavaFX Line Chart Example");
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
