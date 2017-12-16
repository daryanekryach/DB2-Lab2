package mongodb;

import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) {
        MongoDB mongo = new MongoDB();
        Metrics.start();
        workWithMongo(mongo);
        Metrics.stop();
        Metrics.getAllMetrics();
    }

    public static boolean workWithMongo(MongoDB mongo){
        mongo.insert(parseCSV());
        printQuery(mongo.getLogsBySortedByIP());
        printQuery(mongo.getLogsBySortedByURL("2017-11-05T10:00:00Z","2017-11-06T20:00:00Z"));
        printQuery(mongo.getLogsBySortedByURL("1.1.1.1"));

        printQuery(mongo.getURLVisitedDuration());
        printQuery(mongo.getURLSumOfVisits());
        printQuery(mongo.getURLVisitsPerPeriod("2017-11-05T10:00:00Z","2017-11-08T20:00:00Z"));
        printQuery(mongo.getIPSumAndDuration());
        return true;
    }

    private static ArrayList<WebLog> parseCSV() {
        ArrayList<WebLog> logs = new ArrayList<>();
        Converter converter = new Converter();
        String csvName = "logs.csv";
        String line = "";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(csvName));
            while ((line = br.readLine()) != null) {
                logs.add(converter.convertCSVToObject(line));
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return logs;
    }

    private static void printQuery(Iterator<Document> query){
        while (query.hasNext()) {
            System.out.println(query.next());
        }
    }
}
