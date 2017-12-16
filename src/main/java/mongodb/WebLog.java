package mongodb;

import com.google.gson.*;
import lombok.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


@Getter
@Setter
public class WebLog {
    private String URL;
    private String IP;
    private Timestamp timeStamp;
    private long timeSpent;

    public void formateStringToDate(String date){
        try {
            Date dateToFormate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(date);
            timeStamp = new Timestamp((dateToFormate.getTime()));
        }
        catch (ParseException e){
            System.out.println("Error occurred while parsing date");
        }
    }

    public String toJSON(){
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.toJson(this);
    }
}
