import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Slf4jLog;

public class Starter {
    public static void main(String[] args) throws Exception {
        System.getProperties().list(System.out);
        Log.setLog(new Slf4jLog());
        ServerRunner.main(args);
    }
}
