@GrabConfig(systemClassLoader=true, initContextClassLoader=true)
// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
/**
 * grape -V resolve group module version
 * ~/.groovy/grape
 */
@Grapes(
@Grab(group='org.apache.commons', module='commons-math3', version='3.6.1')
)

import org.apache.commons.math3.random.EmpiricalDistribution

import java.util.regex.Matcher
import java.util.regex.Pattern
import groovy.transform.Field

@Field Map ARGS_PATTERNS = [:];


/*
In case of OpenJ9 it is better to use options like that
-server -Xmx10512m -Xms250m -Djava.net.preferIPv4Stack=true -Duser.language=ru -Duser.region=RU -Xverify:none -Xshareclasses:name=myCache,cacheDir="./cds",verbose -Xscmx500M -Xaggressive -Xcodecachetotal128m -Xscmaxaot128M -Xcodecache8m -Dcom.ibm.enableClassCaching=true -Xloggc:logs/gc.log -XX:+PrintGCDetails
 */
class Query {
    int id;
    double time;
    String text = "";
    String args = "";
    String full = "";
    String type = "";
}

Double getTime(String line){
    def group = line =~ /продолжительность\:.(\d+).(\d+)/
    if(group.hasGroup() && group.size()>0){
        String time = group[0][1]+"."+group[0][2];
        return Double.valueOf(time);
    }

    return null;
}

boolean ignoreLine(String line){
    return line.contains('] ОШИБКА:  ');
}
boolean isItEventStart(String line) {
    return line.contains('СООБЩЕНИЕ:  ');
}

boolean isItAQueryStart(String line){
    return line.contains('СООБЩЕНИЕ:  продолжительность:');// && line.contains('выполнение');
}

Query createAQuery(def lines){
    Query query = new Query();
    boolean body = false;

    lines.each{String line->
        Double time = getTime(line);
        if(time!=null){
            query.time = time;
        }

        if(line.contains("СООБЩЕНИЕ:  продолжительность:")){
            body = true;
            String tempLine = line.toUpperCase();
            int index = tempLine.indexOf("SELECT");

            if(index>0) {
                if (tempLine.indexOf("COUNT") > 0) {
                    query.type = "count"
                } else {
                    query.type = "select"
                }
            }

            if(index<0) {
                index = tempLine.indexOf("WITH");
                if(index>0){
                    query.type = "with"
                }
            }

            if(index>0){
                line = line.substring(index);
            } else {
                line="";
            }
        }
        if(line.contains("ПОДРОБНОСТИ:  параметры:")){
            body = false;
            int index = line.indexOf('$1');
            if(index>0) {
                line = line.substring(index);
            }
        }

        if(line.length()>0) {
            if (body) {
                query.text += (line + "\n");
            } else {
                query.args += (line + "\n");
            }
        }
    }

    if(query.text.length()==0){
        println("Unknown query '${lines}'")
    }

    return query;
}

void prepareFullQueary(Query query){
    query.full = query.text;
    def group = query.args =~ /(\$\d+) = (\'.+?\')/

    def args = [:]
    def keys = [];
    if(group.hasGroup() && group.size()>0){

        group.each{ it->
            args[it[1]] = it[2];
            keys<<it[1];
        }
    }

        keys.each { arg ->
            try {
                query.full = queryArgReplace(query.full+" ", arg, args[arg].replace('$', '\\$'))
            }catch(Exception e){
                println(e.toString())
                throw e;
            }
        }
}

void saveAQuery(Query query){
    if(query.text.length()==0){
        return;
    }

    prepareFullQueary(query);
//
//    File file = new File("result"+File.separatorChar+query.id+"_"+query.time+".src")
//    file<<query.text;
//    file<<"\n";
//    file<<query.args;
    FileWriter writer = new FileWriter("result"+File.separatorChar+query.id+"_"+query.time+".sql");
    writer.write(query.full+"\n")
    writer.close()
}

int parseLogGetQueries(String filename, String type){
    String tempLine = "";

    int queries = 0;

    def lines = [];

    def times = [];

    long readLines = 0;
    long readBytes = 0;

    FileReader freader = new FileReader(filename);
    BufferedReader reader = new BufferedReader(freader, 256*1024*1024)
    while ( (line = reader.readLine()) != null) {
        readLines++;
        readBytes+=line.length();

        if(isItEventStart(line)) {
            if(!isItAQueryStart(line) && isItAQueryStart(tempLine)){
                // окончание запроса
                tempLine = "";

                Query query = createAQuery(lines);
                query.id=queries;

                saveAQuery(query);
                queries++;

                if(type!=null){
                    if(type.equalsIgnoreCase(query.type)){
                        times << query.time;
                    }
                } else {
                    times << query.time;
                }
            }
            else if (isItAQueryStart(line) && isItAQueryStart(tempLine)) {
                // окончание запроса
                tempLine = "";

                Query query = createAQuery(lines);
                query.id=queries;

                saveAQuery(query);
                queries++;

                if(type!=null){
                    if(type.equalsIgnoreCase(query.type)){
                        times << query.time;
                    }
                } else {
                    times << query.time;
                }
            }
            if(isItAQueryStart(line) && !isItAQueryStart(tempLine)){
                lines = [];
                // начало запроса
                tempLine = line;
            }
        }
        if(!ignoreLine(line)) {
            lines += line;
        }
        if(readLines%1000==0){
            println "Read "+readLines+" lines "+readBytes+" bytes. Queries: ${queries}";
        }
    }

    EmpiricalDistribution distribution = new EmpiricalDistribution();
    distribution.load(times as double[])
    def stat = distribution.getBinStats();
    stat.each{ it->
        if(!it.min.isNaN() && !it.max.isNaN()) {
            println "[$it.min..$it.max]: $it.n"
        }
    }

    println "Size: "+times.size();
    println "Sum: "+distribution.sampleStats.sum
    return times.size();
}

println "Analyzing ${args[0]}..."

for(int i=1;i<10001;i++) {
    String pattern = '(?:\\s|=|\\()(\\$'+i+')(?:\\s|=|\\)|\\,)';
    Pattern r = Pattern.compile(pattern);
    ARGS_PATTERNS.put('$'+i, r);
}

String queryArgReplace(String text, String key, String value) {
    Pattern r = ARGS_PATTERNS[key];
    if(r==null){
        System.err.println("ERROR: no pattern found for ${key}. compiling");
        r = Pattern.compile(/(?:\s|=|\()(\${key})(?:\s|=|\)|\,)/);
    }
    Matcher m = r.matcher(text);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
        String arg0 = m.group(0);
        String arg1 = arg0.replaceFirst(Pattern.quote(m.group(1)), (value));
        arg1 = arg1.replace('$', '\\$')
        m.appendReplacement(sb, arg1);
    }
    m.appendTail(sb); // append the rest of the contents
    return sb.toString();
}

int queries = parseLogGetQueries(args[0], "count");
println "Found $queries queries";

//
//String arg='$1'
//String a = /(?:\s|=|\()(\${arg})(?:\s|=|\)|\,)/
//String query = 'where a = $1 ';
//query = query.replaceAll(a, 'serviceCall$1$1');
//
//println query;
//def v = 'employee$473784836'
//System.out.println(queryArgReplace('where a = ($1) ', '$1', v.replace('$', '\\$')));

