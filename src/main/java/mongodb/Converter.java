package mongodb;

public class Converter {

    public WebLog convertCSVToObject(String csvLine){
        WebLog webLog = new WebLog();
        String[] log = csvLine.split(",");
        webLog.setURL(log[0]);
        webLog.setIP(log[1]);
        webLog.formateStringToDate(log[2]);
        webLog.setTimeSpent(Long.parseLong(log[3]));
        return webLog;
    }

    public String convertToJSON(WebLog webLogs){
        StringBuilder json = new StringBuilder();
        json.append("{").append("\"URL\":\"").append(webLogs.getURL()).append("\",\"IP\":\"")
                .append(webLogs.getIP()).append("\",\"timeStamp\":\"").append(webLogs.getTimeStamp())
                .append("\",\"timeSpent\":").append(webLogs.getTimeSpent()).append("}");
        return json.toString();
    }
}
