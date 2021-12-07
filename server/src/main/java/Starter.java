import com.amazonaws.services.dynamodbv2.local.main.CommandLineInput;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.local.server.LocalDynamoDBServerHandler;
import com.amazonaws.services.dynamodbv2.local.shared.access.LocalDBAccess;
import com.amazonaws.services.dynamodbv2.local.shared.access.awssdkv1.client.LocalAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.shared.access.awssdkv1.client.LocalAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.local.shared.jobs.JobsRegister;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Slf4jLog;

import java.util.concurrent.Executors;

public class Starter {
    public static void main(String[] args) throws Exception {
        System.getProperties().list(System.out);
        Log.setLog(new Slf4jLog());

        CommandLineInput cli = new CommandLineInput(args);
        cli.init();

        LocalDBAccess localDBAccess = new InMemoryStore();
        final JobsRegister jobsRegister = new JobsRegister(Executors.newSingleThreadExecutor(), false);
        LocalAmazonDynamoDB localAmazonDynamoDB = new LocalAmazonDynamoDB(localDBAccess, jobsRegister);

        LocalAmazonDynamoDBStreams localAmazonDynamoDBStreams = new LocalAmazonDynamoDBStreams(localDBAccess, jobsRegister);

        final DynamoDBProxyServer server = new DynamoDBProxyServer(
            cli.getPort(),
            new LocalDynamoDBServerHandler(
                new InMemoryRequestHandler(0, localAmazonDynamoDB, localAmazonDynamoDBStreams),
                cli.getCorsParams()));

        server.start();
    }
}
