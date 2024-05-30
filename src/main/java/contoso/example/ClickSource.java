package contoso.example;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Events> {
    // declare a flag
    private Boolean running = true;

    // declare a flag
    public void run(SourceContext<Events> ctx) throws Exception{
        // generate random record
        Random random = new Random();
        String[] users = {"Mary","Alice","Bob","Cary"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100","./prod?id=10"};

        // loop generate
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            String ts = timestamp.toString();
            ctx.collect(new Events(user,url,ts));
            Thread.sleep(2000);
        }
    }
    @Override
    public void cancel()
    {
        running = false;
    }
}