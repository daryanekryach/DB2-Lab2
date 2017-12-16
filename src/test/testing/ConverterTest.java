package testing;


import org.junit.*;
import mongodb.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class ConverterTest {
    private String csvData = "";
    private WebLog log;

    @Before
    public void init() throws ParseException{
        String csvName = "logsTest.csv";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(csvName));
            csvData = br.readLine();
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
        log = new WebLog();
        log.setURL("https://tutorialspoint.com");
        log.setIP("1.1.1.1");
        log.setTimeSpent(7000);
        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .parse("2017-11-05T16:00:00Z");
        Timestamp stamp = new Timestamp(date.getTime());
        log.setTimeStamp(stamp);
    }

    @Test
    public void convertCSVToObject(){
        Converter converter = new Converter();
        WebLog actualLog = converter.convertCSVToObject(csvData);
        assertEquals(log.getTimeStamp(),actualLog.getTimeStamp());
    }

    @Test
    public void convertToJSON(){
        Converter converter = new Converter();
        assertEquals(converter.convertToJSON(log), converter.convertToJSON(log));
    }
}
