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
    def exceptions;

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
            exceptions<<e;
            exception = e;
        }
        String error = exception==null?"false":exception.toString();

        stmt.close();
        connection.close();
        return "${id}; ${file}; ${System.currentTimeMillis() - start}; ${originalId}; ${originalTime}; ${result}; ${error} ";
    }
}

String url = args[0];
String login = args[1];
String password = args[2];
String queriesDir = args[3];
int threads = 16;

ExecutorService pool = Executors.newFixedThreadPool(threads);

println('loading '+url);

HikariConfig config = new HikariConfig();
config.setJdbcUrl( url );
config.setUsername( login );
config.setPassword( password );
config.setMinimumIdle(threads);
config.setMaximumPoolSize(threads);
HikariDataSource ds = new HikariDataSource( config );

def queries = [:]

def exceptions = []

def futures = []

def dir = new File(queriesDir)

def files = [];

def results = [];

dir.eachFileRecurse (FileType.FILES) { file ->
    files << file.getAbsolutePath();
}

files.sort();

files.each {file->
    File f = new File(file);
    queries.put(f.getName(),  f.text);
}

int position = 0;

long time = System.currentTimeMillis();

queries.each { entry->
    UnitOfLoad unit = new UnitOfLoad();
    unit.ds = ds;
    unit.file = entry.getKey();
    String timeFromFile="0.0";
    String idFromFile = "-1";

    def group = unit.file =~ /(\d+)_(.+)?.sql/
    if(group.hasGroup()&&group.size()>0){
        timeFromFile = group[0][2];
        idFromFile = group[0][1];
    }
    unit.originalTime = timeFromFile;
    unit.originalId = idFromFile;
    unit.query = entry.getValue();
    unit.exceptions = exceptions;
    unit.id = position;

    futures << pool.submit(unit);
    position++;
}

File output = new File("result.csv");
output.delete();
output = new File("result.csv");

while(futures.size()>0){
    def newFutures = [];
    futures.each { Future it->
        if(it.isDone()){
            try {
                def result = it.get(1, TimeUnit.NANOSECONDS);

                results << results;
                String outline = "${result}; ${results.size()}; ${files.size()}";

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

println("Done. Time: ${System.currentTimeMillis() - time}")

