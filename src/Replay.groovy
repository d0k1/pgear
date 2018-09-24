import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import groovy.io.FileType

import javax.sql.DataSource
import java.sql.Connection
import java.sql.Statement
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

@GrabConfig(systemClassLoader=true, initContextClassLoader=true)
@Grapes(value = [
        @Grab(group = 'org.postgresql', module = 'postgresql', version = '42.2.2'),
        @Grab(group = 'com.zaxxer', module = 'HikariCP', version = '3.2.0')])

class UnitOfLoad implements Callable {
    DataSource ds;
    String query;
    int id;
    String file;
    String originalId;
    String originalTime;

    @Override
    String call() {
        Connection connection = ds.getConnection();
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        Exception exception = null;
        def result = null;
        try {
            result = stmt.execute(query);
        } catch(Exception e){
            exception = e;
        }
        String error = exception==null?"false":exception.toString();

        stmt.close();
        connection.close();
        return "${id}; ${file}; ${originalId}; ${originalTime}; ${System.currentTimeMillis() - start}; ${result}; ${error} ";
    }
}

String url = args[0];
String login = args[1];
String password = args[2];
String queriesFilename = args[3];
int threads = 20;

ExecutorService pool = Executors.newFixedThreadPool(threads);

println("${new Date()} loading ${url}");

HikariConfig config = new HikariConfig();
config.setJdbcUrl( url );
config.setUsername( login );
config.setPassword( password );
config.setMinimumIdle(threads);
config.setMaximumPoolSize(threads);
HikariDataSource ds = new HikariDataSource( config );

def queries = []

def futures = []

FileReader freader = new FileReader(queriesFilename);
BufferedReader reader = new BufferedReader(freader, 256*1024*1024)

StringBuffer queryBuffer = new StringBuffer();
String line = null;

while ( (line = reader.readLine()) != null) {
    if(line.equals("(\$\$)")){
        String query = queryBuffer.toString()
        queryBuffer = new StringBuffer();
        if(query.trim().length()>0){
            queries << query;
        }
    } else {
        queryBuffer.append("\n"+line);
    }
}

String query = queryBuffer.toString()
if(query.trim().length()>0){
    queries << query;
}

println "Found ${queries.size()} queries"

int position = 0;

long time = System.currentTimeMillis();

queries.each { item ->
    UnitOfLoad unit = new UnitOfLoad();
    unit.ds = ds;
    unit.file = queriesFilename;
    String timeFromFile="0.0";
    String idFromFile = "-1";

    unit.originalTime = timeFromFile;
    unit.originalId = idFromFile;

    unit.query = item;
    unit.id = position;

    futures << pool.submit(unit);
    position++;
}

File output = new File("result.csv");
output.delete();
output = new File("result.csv");

position = 0;
while(futures.size()>0){
    def newFutures = [];
    futures.each { Future it->
        if(it.isDone()){
            try {
                def result = it.get(1, TimeUnit.NANOSECONDS);
                position++;
                String outline = "${result}; ${position}; ${queries.size()}";

                output << outline+"\n";
                println(outline);
            } catch(TimeoutException e){
                newFutures << it;
            }
        } else {
            newFutures << it;
        }
    }
    futures = newFutures;
}

println("${new Date()} Done Time: ${System.currentTimeMillis() - time}")

