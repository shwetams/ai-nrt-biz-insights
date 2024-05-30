package contoso.example;
import java.sql.Timestamp;

public class Events {

    public String user;
    public String url;
    public String ts;

    public Events() {
    }

    public Events(String user, String url, String ts) {
        this.user = user;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString(){
        return "{" +
                "user: \"" + user + "\""  +
                ",url: \"" + url + "\""  +
                ",ts: " + ts +
                "}";
    }
}
