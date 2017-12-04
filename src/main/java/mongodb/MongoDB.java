package mongodb;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoDB {
    private MongoClient mongo;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    /**
     * Connects to MongoDB, selects 'weblogs' database and collection 'logs'.
     */
    private void connectToMongo() {
        mongo = new MongoClient("localhost", 27017);
        database = mongo.getDatabase("weblogs");
        collection = database.getCollection("logs");
    }

    /**
     * Inserts list of weblogs into collection.
     *
     * @param weblogs - list of weblogs to insert
     */
    public void insert(ArrayList<WebLog> weblogs) {
        connectToMongo();
        ArrayList<Document> documents = new ArrayList<>();
        for (WebLog weblog : weblogs) {
            Document log = new Document();
            log.append("URL", weblog.getURL());
            log.append("IP", weblog.getIP());
            log.append("timeStamp", weblog.getTimeStamp());
            log.append("timeSpent", weblog.getTimeSpent());
            documents.add(log);
        }
        collection.insertMany(documents);
        mongo.close();
    }

    /**
     * Selects logs sorted by IP.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getLogsBySortedByIP() {
        connectToMongo();
        return collection.find().sort(descending("IP")).iterator();
    }

    /**
     * Selects logs sorted by URL.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getLogsBySortedByURL(String dateFrom, String dateTo) {
        try {
            Date dateFromFormated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom);
            Date dateToFormated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo);
            Timestamp timeStampFrom = new Timestamp((dateFromFormated.getTime()));
            Timestamp timestampTo = new Timestamp((dateToFormated.getTime()));
            connectToMongo();
            return collection.find(and(gt("timeStamp", timeStampFrom),
                    lt("timeStamp", timestampTo))).sort(descending("URL")).iterator();
        } catch (ParseException e) {
            System.out.println("Problem occurred while parsing date");
            return null;
        }
    }

    /**
     * Selects logs sorted by URL with specific IP.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getLogsBySortedByURL(String ip) {
        connectToMongo();
        return collection.find(new BasicDBObject("IP", ip)).sort(descending("URL")).iterator();
    }

    //region Methods for MapReduce

    /**
     * Selects logs for each URL and the sum of visit duration per each.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getURLVisitedDuration() {
        connectToMongo();
        MongoCollection<Document> urlVisitDurationCollection =
                database.getCollection("url_visit_duration");
        String map = "function()" +
                "{ emit(this.URL,this.timeSpent); }";
        String reduce = "function(key, values)" +
                "{return Array.sum(values)}";
        collection.mapReduce(map, reduce).collectionName("url_visit_duration").toCollection();
        Iterator<Document> webLogs = urlVisitDurationCollection.find()
                .sort(descending("values")).iterator();
        mongo.close();
        return webLogs;
    }

    /**
     * Selects logs for each URL and the sum of its visit.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getURLSumOfVisits() {
        connectToMongo();
        MongoCollection<Document> urlVisitSumCollection =
                database.getCollection("url_visit_sum");
        String map = "function()" +
                "{ emit(this.URL,1); }";
        String reduce = "function(key, values)" +
                "{var count = 0; for(var i in values)" +
                "{count += values[i];} " +
                "return count;}";
        collection.mapReduce(map, reduce).collectionName("url_visit_sum").toCollection();
        Iterator<Document> webLogs = urlVisitSumCollection.find()
                .sort(descending("value")).iterator();
        mongo.close();
        return webLogs;
    }

    /**
     * Selects logs for each URL and the sum of its visits during specific time period.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getURLVisitsPerPeriod(String dateFrom, String dateTwo) {
        connectToMongo();
        MongoCollection<Document> urlVisitPerPeriodCollection =
                database.getCollection("url_visit_per_period");
        String map = "function () {"
                + "if (this.datetime > ISODate"
                + "(\"" + dateFrom + "\")"
                + " && this.datetime < ISODate"
                + "(\"" + dateTwo + "\"))"
                + " emit(this.url, 1); }";
        String reduce = "function(key, values) { return Array.sum(values); }";
        collection.mapReduce(map, reduce).collectionName("url_visit_per_period").toCollection();
        Iterator<Document> webLogs = urlVisitPerPeriodCollection.find()
                .sort(descending("value")).iterator();
        mongo.close();
        return webLogs;
    }

    /**
     * Selects logs for each IP, the sum of its visit and visit duration.
     *
     * @return Iterator<Document> of selected logs.
     */
    public Iterator<Document> getIPSumAndDuration() {
        connectToMongo();
        MongoCollection<Document> ipVisitSumDurationCollection =
                database.getCollection("ip_visit_sum_duration");
        String map = "function(){" +
                "values ={count:1,duration:this.timeSpent};" +
                "emit(this.IP,values);}";
        String reduce = "function(key, values) {" +
                "var count=0; var duration=0;" +
                "for(var i in values)" +
                "{count += values[i].count;" +
                "duration += values[i].duration;}" +
                "return {count:count, duration:duration}}";
        collection.mapReduce(map, reduce).collectionName("ip_visit_sum_duration").toCollection();
        Iterator<Document> webLogs = ipVisitSumDurationCollection.find()
                .sort(descending("count", "duration")).iterator();
        mongo.close();
        return webLogs;
    }
    //endregion
}
