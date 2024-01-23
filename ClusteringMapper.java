import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClusteringMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(";");

        if (values.length >= 6) {
            // Extraer las columnas relevantes y realizar el escalamiento
            double o3 = Double.parseDouble(values[0].replace(',', '.'));
            double co = Double.parseDouble(values[1].replace(',', '.'));
            double no2 = Double.parseDouble(values[2].replace(',', '.'));
            double so2 = Double.parseDouble(values[3].replace(',', '.'));
            double pm25 = Double.parseDouble(values[4].replace(',', '.'));

            // Escalar los datos
            double scaledO3 = (o3 - meanO3) / stdDevO3;
            double scaledCO = (co - meanCO) / stdDevCO;
            double scaledNO2 = (no2 - meanNO2) / stdDevNO2;
            double scaledSO2 = (so2 - meanSO2) / stdDevSO2;
            double scaledPM25 = (pm25 - meanPM25) / stdDevPM25;

            // Construir una cadena con los datos escalados
            String scaledData = scaledO3 + ";" + scaledCO + ";" + scaledNO2 + ";" + scaledSO2 + ";" + scaledPM25;

            // Emitir la clave y el valor
            context.write(new Text("1"), new Text(scaledData));
        }
    }
}
