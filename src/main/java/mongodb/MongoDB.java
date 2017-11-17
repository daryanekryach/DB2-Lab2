package mongodb;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;

import static com.mongodb.client.model.Sorts.*;

public class MongoDB {
    private MongoClient mongo;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    private void connectToMongo() {
        mongo = new MongoClient("localhost", 27017);
        database = mongo.getDatabase("weblogs");
        collection = database.getCollection("logs");
    }

    public void insert(ArrayList<WebLog> weblogs) {
        connectToMongo();
        ArrayList<Document> documents = new ArrayList<>();
        for (int i = 0; i < weblogs.size(); i++) {
            Document log = new Document();
            log.append("URL", weblogs.get(i).getURL());
            log.append("IP", weblogs.get(i).getIP());
            log.append("timeStamp", weblogs.get(i).getTimeStamp());
            log.append("timeSpent", weblogs.get(i).getTimeSpent());
            documents.add(log);
        }
        collection.insertMany(documents);
        mongo.close();
    }

    public Iterator<Document> getLogsBySortedByIP() {
        connectToMongo();
        Iterator<Document> webLogs = collection.find().sort(descending("IP")).iterator();
        //mongo.close();
        return webLogs;
    }

    public Iterator<Document> getLogsBySortedByURL() {
        connectToMongo();
        Iterator<Document> webLogs = collection.find().sort(descending("URL")).iterator();
        mongo.close();
        return webLogs;
    }

    public Iterator<Document> getLogsBySortedByURL(String ip) {
        connectToMongo();
        Iterator<Document> webLogs = collection.find(new BasicDBObject("IP", ip)).sort(descending("URL"))
                .iterator();
        mongo.close();
        return webLogs;
    }

    public Iterator<Document> getURLVisitedDuration() {
        connectToMongo();
        String map = "function()" +
                "{ emit(this.URL,this.timeSpent); }";
        String reduce = "function(key, values)" +
                "{return Array.sum(values)}";
        collection.mapReduce(map, reduce).collectionName("logs").toCollection();
        Iterator<Document> webLogs = collection.find().sort(descending("values")).iterator();
        mongo.close();
        return webLogs;
    }

    public Iterator<Document> getURLSumOfVisits() {
        connectToMongo();
        String map = "function()" +
                "{ emit(this.URL,1); }";
        String reduce = "function(key, values)" +
                "{var count = 0; for(var i in values)" +
                "{count += values[i];} " +
                "return count;}";
        collection.mapReduce(map, reduce).collectionName("logs").toCollection();
        Iterator<Document> webLogs = collection.find().sort(descending("values")).iterator();
        mongo.close();
        return webLogs;
    }

    public Iterator<Document> getURLVisitsPerPeriod(String dateFrom, String dateTwo) {
        connectToMongo();
        String map = "function () {"
                + "if (this.datetime > ISODate"
                + "(\"" + dateFrom + "\")"
                + " && this.datetime < ISODate"
                + "(\"" + dateTwo + "\"))"
                + " emit(this.url, 1); }";
        String reduce = "function(key, values) { return Array.sum(values); }";
        collection.mapReduce(map, reduce).collectionName("logs").toCollection();
        Iterator<Document> webLogs = collection.find().sort(descending("values")).iterator();
        mongo.close();
        return webLogs;
    }

    public Iterator<Document> getIPSumAndDuration() {
        connectToMongo();
        String map = "function(){" +
                "values ={count:1,duration:this.timeSpent};" +
                "emit(this.IP,values);}";
        String reduce = "function(key, values) {" +
                "var count=0; var duration=0;" +
                "for(var i in values)" +
                "{count += values[i].count;" +
                "duration += values[i].duration;}" +
                "return {count:count, duration:duration}}";
        collection.mapReduce(map, reduce).collectionName("logs").toCollection();
        Iterator<Document> webLogs = collection.find().sort(descending("count", "duration")).iterator();
        mongo.close();
        return webLogs;
    }
}
