import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<Double> Temperature = new ArrayList<Double>();
        List<Double> Precipitation = new ArrayList<Double>();
        List<Double> Visibility = new ArrayList<Double>();
        List<Double> WindGustSpeed = new ArrayList<Double>();
        List<Double> WindSpeed = new ArrayList<Double>();

        List<String> WeatherType = new ArrayList<String>();

        // 0 - rain: 0: false; 1: light; 2: normal; 3: heavy
        // 1 - snow: 0: false; 1: light; 2: normal; 3: heavy
        // 2 - mist: 0: false; 1: true
        // 3 - fog: 0: false; 1: true
        // 4 - haze: 0: false; 1: true
        // 5 - freezing: 0: false; 1: true
        int[] weatherCode = new int[] { 0, 0, 0, 0, 0, 0 };

        for (Text text : values) {
            String[] d = text.toString().split(",", -1);
            // * Temperature
            if (!d[0].isEmpty()) {
                if (d[0].contains("*")) {
                    Temperature.add(Double.NaN);
                } else {
                    Temperature.add(Double.valueOf(d[0].replaceAll("[a-zA-z]", "")));
                }
            }
            // * Precipitation
            // ! T = indicates trace amount of precipitation < 0.01 inch, use 0.0 instead
            if (!d[1].isEmpty()) {
                if (d[1].contains("T")) {
                    Precipitation.add(0.00);
                } else {
                    Precipitation.add(Double.valueOf(d[1].replaceAll("[a-zA-z]", "")));
                }
            }

            // * Weather type
            // 0 - rain: 0: false; 1: light; 2: normal; 3: heavy
            // 1 - snow: 0: false; 1: light; 2: normal; 3: heavy
            // 2 - mist: 0: false; 1: true
            // 3 - fog: 0: false; 1: true
            // 4 - haze: 0: false; 1: true
            // 5 - freezing: 0: false; 1: true
            String code = d[2];
            if (!code.isEmpty()) {

                WeatherType.add(code);

                // Rain: RA:02
                if (code.contains("RA")) {
                    if (code.contains("-") && weatherCode[0] <= 1) {
                        weatherCode[0] = 1;
                    } else if (code.contains("+")) {
                        weatherCode[0] = 3;
                    } else if (weatherCode[0] <= 2) {
                        weatherCode[0] = 2;
                    }
                }

                // Snow: SN:03
                if (code.contains("SN")) {

                    if (code.contains("-") && weatherCode[1] <= 1) {
                        weatherCode[1] = 1;
                    } else if (code.contains("+")) {
                        weatherCode[1] = 3;
                    } else if (weatherCode[1] <= 2) {
                        weatherCode[1] = 2;
                    }
                }

                // Mist: BR:1
                if (code.contains("BR")) {
                    weatherCode[2] = 1;
                }

                // Fog
                if (code.contains("FG")) {
                    weatherCode[3] = 1;
                }

                // Haze
                if (code.contains("HZ") || code.contains("FU")) {
                    weatherCode[4] = 1;
                }

                // Freezing
                if (code.contains("FZ")) {
                    weatherCode[5] = 1;
                }

            }

            // * Visibility
            if (!d[4].isEmpty()) {
                {
                    Visibility.add(Double.valueOf(d[4].replaceAll("[a-zA-z]", "")));
                }
            }
            // * WindGustSpeed
            if (!d[5].isEmpty()) {
                {
                    WindGustSpeed.add(Double.valueOf(d[5].replaceAll("s", "")));
                }
            }
            // * WindSpeed
            if (!d[6].isEmpty()) {
                {
                    WindSpeed.add(Double.valueOf(d[6].replaceAll("s", "")));
                }
            }
        }

        double TemperatureAvg = Arrays.stream(Temperature.stream().mapToDouble(Double::doubleValue).toArray())
                .filter(e -> !Double.isNaN(e)).average().orElse(Double.NaN);
        double PrecipitationAvg = Arrays.stream(Precipitation.stream().mapToDouble(Double::doubleValue).toArray())
                .filter(e -> !Double.isNaN(e)).average().orElse(Double.NaN);
        double VisibilityAvg = Arrays.stream(Visibility.stream().mapToDouble(Double::doubleValue).toArray())
                .filter(e -> !Double.isNaN(e)).average().orElse(Double.NaN);
        double WindGustSpeedAvg = Arrays.stream(WindGustSpeed.stream().mapToDouble(Double::doubleValue).toArray())
                .filter(e -> !Double.isNaN(e)).average().orElse(Double.NaN);
        double WindSpeedAvg = Arrays.stream(WindSpeed.stream().mapToDouble(Double::doubleValue).toArray())
                .filter(e -> !Double.isNaN(e)).average().orElse(Double.NaN);

        String out = String.format("%.2f,%.2f,%.2f,%.2f,%.2f,%s", TemperatureAvg, PrecipitationAvg, VisibilityAvg,
                WindGustSpeedAvg, WindSpeedAvg, Arrays.toString(weatherCode).replaceAll("\\[|\\]|\\s", ""));

        // String out = String.format("%s, %s", Arrays.toString(weatherCode),
        // WeatherType);

        context.write(key, new Text(out));
    }
}