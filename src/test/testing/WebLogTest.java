package testing;

import org.junit.*;
import mongodb.*;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class WebLogTest {

    @Test
    public void setIP() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        String ip = "1.1.1.1";
        log.setIP(ip);
        final Field field = log.getClass().getDeclaredField("IP");
        field.setAccessible(true);
        assertEquals(field.get(log), ip);
    }

    @Test
    public void getIP() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        final Field field = log.getClass().getDeclaredField("IP");
        field.setAccessible(true);
        String ip = "1.1.1.1";
        field.set(log, ip);
        assertEquals(log.getIP(), ip);
    }

    @Test
    public void setURL() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        String url = "google.com";
        log.setURL(url);
        final Field field = log.getClass().getDeclaredField("URL");
        field.setAccessible(true);
        assertEquals(field.get(log), url);
    }

    @Test
    public void getURL() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        final Field field = log.getClass().getDeclaredField("URL");
        field.setAccessible(true);
        String url = "google.com";
        field.set(log, url);
        assertEquals(log.getURL(), url);
    }

    @Test
    public void setTimeStamp() throws NoSuchFieldException, IllegalAccessException, ParseException {
        final WebLog log = new WebLog();
        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .parse("2017-11-06T08:02:00Z");
        Timestamp stamp = new Timestamp(date.getTime());
        log.setTimeStamp(stamp);
        final Field field = log.getClass().getDeclaredField("timeStamp");
        field.setAccessible(true);
        assertEquals(field.get(log), stamp);
    }

    @Test
    public void getTimeStamp() throws NoSuchFieldException, IllegalAccessException, ParseException {
        final WebLog log = new WebLog();
        final Field field = log.getClass().getDeclaredField("timeStamp");
        field.setAccessible(true);
        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .parse("2017-11-06T08:02:00Z");
        Timestamp stamp = new Timestamp(date.getTime());
        field.set(log, stamp);
        assertEquals(log.getTimeStamp(), stamp);
    }

    @Test
    public void setTimeSpent() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        long time = 2;
        log.setTimeSpent(time);
        final Field field = log.getClass().getDeclaredField("timeSpent");
        field.setAccessible(true);
        assertEquals(field.get(log), time);
    }

    @Test
    public void getTimeSpent() throws NoSuchFieldException, IllegalAccessException {
        final WebLog log = new WebLog();
        final Field field = log.getClass().getDeclaredField("timeSpent");
        field.setAccessible(true);
        long time = 2;
        field.set(log, time);
        assertEquals(log.getTimeSpent(), time);
    }
}
