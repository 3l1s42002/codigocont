import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusteringReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Lista para almacenar los datos escalados
        List<double[]> scaledDataList = new ArrayList<>();

        for (Text value : values) {
            String[] scaledDataArray = value.toString().split(";");
            double[] scaledData = new double[scaledDataArray.length];

            for (int i = 0; i < scaledDataArray.length; i++) {
                scaledData[i] = Double.parseDouble(scaledDataArray[i]);
            }

            scaledDataList.add(scaledData);
        }

        // Realizar KMeans en los datos escalados
        List<Integer> clusters = kMeans(scaledDataList);

        // Emitir resultados
        int index = 0;
        for (double[] scaledData : scaledDataList) {
            String result = scaledData[0] + ";" + scaledData[1] + ";" + scaledData[2] + ";" + scaledData[3] + ";" + scaledData[4]
                    + ";" + clusters.get(index);
            context.write(new Text(result), new Text(""));
            index++;
        }
    }

    private List<Integer> kMeans(List<double[]> data) {
        int numClusters = 3;  // Número de clusters
        KMeansPlusPlusClusterer<double[]> kMeansClusterer = new KMeansPlusPlusClusterer<>(numClusters, -1, new EuclideanDistance());

        // Convertir la lista de datos a un array
        double[][] dataArray = data.toArray(new double[0][0]);

        // Realizar el clustering
        List<Cluster<double[]>> clusters = kMeansClusterer.cluster(dataArray);

        // Obtener la asignación de clusters para cada punto
        List<Integer> clusterAssignments = new ArrayList<>();
        for (double[] point : dataArray) {
            for (int i = 0; i < clusters.size(); i++) {
                if (clusters.get(i).getPoints().contains(point)) {
                    clusterAssignments.add(i);
                    break;
                }
            }
        }

        return clusterAssignments;
    }
}
