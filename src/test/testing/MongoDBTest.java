package testing;

import mongodb.MongoDB;
import mongodb.WebLog;
import org.bson.Document;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.Before;
import org.dbunit.dataset.xml.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

public class MongoDBTest extends MongoDBConfig {
    MongoDB mongo = new MongoDB();


    public MongoDBTest(String name) {
        super(name);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        beforeData = new FlatXmlDataSetBuilder().build(
                Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("logsDataSet.xml"));

        tester.setDataSet(beforeData);
        tester.onSetup();
    }
    @Test
    public void testInsert() throws DataSetException, Exception {
        ArrayList<WebLog> logs = new ArrayList<>();
        WebLog log1 = new WebLog();
        log1.setIP("1.1.1.1");
        log1.setURL("https://tutorialspoint.com");
        log1.setTimeSpent(7000);
        log1.formateStringToDate("2017-11-05T14:00:00Z");
        WebLog log2 = new WebLog();
        log2.setIP("2.2.2.2");
        log2.setURL("https://github.com");
        log2.setTimeSpent(67000);
        log2.formateStringToDate("2017-11-06T16:10:00Z");
        logs.add(log1);
        logs.add(log2);

        mongo.insert(logs);
        IDataSet expectedData = new FlatXmlDataSetBuilder().build(
                Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("logsDataSetInserted.xml"));

        IDataSet actualData = tester.getConnection().createDataSet();
        assertEquals(expectedData, actualData);
        assertEquals(expectedData.getTable("log").getRowCount(), logs.size());
    }
}
