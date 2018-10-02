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
@Field Map ARGS_QUOTED_PATTERNS = [:];
@Field BufferedOutputStream outputStream;

/*
In case of OpenJ9 it is better to use options like that
-server -Xmx10512m -Xms250m -Djava.net.preferIPv4Stack=true -Duser.language=ru -Duser.region=RU -Xverify:none -Xshareclasses:name=myCache,cacheDir="./cds",verbose -Xscmx500M -Xaggressive -Xcodecachetotal128m -Xscmaxaot128M -Xcodecache8m -Dcom.ibm.enableClassCaching=true -Xloggc:logs/gc.log -XX:+PrintGCDetails
 */
class Query {
    int id;
    double time= Double.NaN;
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

    return Double.NaN;
}

boolean ignoreLine(String line){
    return line.contains('] ОШИБКА:  ')||
           line.contains('] ОШИБКА:  повторяющееся')||
           line.contains('] ОПЕРАТОР:') ||
           line.contains('] ПОДРОБНОСТИ:  Процесс ') ||
           line.contains('] КОНТЕКСТ:  ') ||
           line.contains('] ПОДСКАЗКА:  ') ||
           (line.contains('] ПОДРОБНОСТИ:  Ключ ') && line.contains(' уже существует.')) ||

           (line.contains('\tПроцесс ') && line.contains(' ожидает в режиме ShareLock блокировку ')) ||
           (line.contains('\tПроцесс ') && line.contains(': '));

}
boolean isItEventStart(String line) {
    return line.contains('СООБЩЕНИЕ:  ');
}

boolean isItAQueryStart(String line){
    return line.contains('СООБЩЕНИЕ:  продолжительность:') || line.contains('СООБЩЕНИЕ:  выполнение');
}

Query createAQuery(def lines){
    Query query = new Query();
    boolean body = false;

    lines.each{String line->

        if(query.time==Double.NaN){
            Double time = getTime(line);
            if(time!=Double.NaN) {
                query.time = time;
            }
        }

        if(isItAQueryStart(line)){
            body = true;
            String tempLine = line.toUpperCase();

            int index = -1;

            if(index<0) {
                index = tempLine.indexOf("WITH");
                if(index>0){
                    query.type = "with"
                }
            }

            if(index<0) {
                index = tempLine.indexOf("SELECT");

                if (index > 0) {
                    if (tempLine.indexOf("COUNT") > 0) {
                        query.type = "count"
                    } else {
                        query.type = "select"
                    }
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
        //println("Unknown query '${lines}'")
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

int saveAQuery(Query query){

    if(outputStream==null){
        return 0;
    }

    if(query.text.length()==0){

        // avoid calculation with unsupported queries like create table / drop table / vacuum
        if(query.time!=Double.NaN){
            query.time = Double.NaN;
        }

        return 0;
    }

    prepareFullQueary(query);

    outputStream.write(query.full.getBytes());
    outputStream.write("\n".getBytes());
    outputStream.write("(\$\$)".getBytes());
    outputStream.write("\n".getBytes());
    outputStream.flush();

    return 1;
}

int parseLogGetQueries(String filename, String type){

    long savedQueries = 0;

    int foundProbableQueries = 0;

    def lines = [];

    def queriesOfTypeDurations = [];

    long readLines = 0;
    long readBytes = 0;
    String prevStateLine = "";

    FileReader freader = new FileReader(filename);
    BufferedReader reader = new BufferedReader(freader, 256*1024*1024)
    while ( (line = reader.readLine()) != null) {
        readLines++;
        readBytes+=line.length();

        if(isItEventStart(line) /*СОБЫТИЕ*/ ) {
            // текущая строка не выполнение, а предыдущая выполнение - окончание запроса
            if(!isItAQueryStart(line) && isItAQueryStart(prevStateLine)){
                // окончание запроса
                prevStateLine = "";

                Query query = createAQuery(lines);
                query.id=foundProbableQueries;

                savedQueries+=saveAQuery(query);
                foundProbableQueries++;

                if(type!=null){
                    if(type.equalsIgnoreCase(query.type)){
                        queriesOfTypeDurations << query.time;
                    }
                } else {
                    queriesOfTypeDurations << query.time;
                }
            }
            // текущая строка выполнение и предыдущая выполнение - окнчание запроса
            else if (isItAQueryStart(line) && isItAQueryStart(prevStateLine)) {
                // окончание запроса
                prevStateLine = "";

                Query query = createAQuery(lines);
                query.id=foundProbableQueries;

                savedQueries+=saveAQuery(query);
                foundProbableQueries++;

                if(type!=null){
                    if(type.equalsIgnoreCase(query.type)){
                        queriesOfTypeDurations << query.time;
                    }
                } else {
                    queriesOfTypeDurations << query.time;
                }
            }
            //если текущая строка выполнение а предыдущая нет - начало запроса
            if(isItAQueryStart(line) && !isItAQueryStart(prevStateLine)){
                lines = [];
                // начало запроса
                prevStateLine = line;
            }
        }
        if(!ignoreLine(line)) {
            lines += line;
        }
        if(readLines%1000==0){
            println "Read "+readLines+" lines "+readBytes+" bytes. Queries: ${foundProbableQueries}";
        }
    }
    if(lines.size()>0){
        // окончание запроса
        Query query = createAQuery(lines);
        query.id=foundProbableQueries;

        savedQueries+=saveAQuery(query);
        if(type!=null){
            if(type.equalsIgnoreCase(query.type)){
                queriesOfTypeDurations << query.time;
            }
        } else {
            queriesOfTypeDurations << query.time;
        }
    }

    long totalQueries = queriesOfTypeDurations.size();
    def withTimes = [];
    queriesOfTypeDurations.each {it->
        if(it!=Double.NaN){
            withTimes << it;
        }
    }

    EmpiricalDistribution distribution = new EmpiricalDistribution();
    distribution.load(withTimes as double[])
    def stat = distribution.getBinStats();
    stat.each{ it->
        if(!it.min.isNaN() && !it.max.isNaN()) {
            println "[$it.min..$it.max]: $it.n"
        }
    }

    println "Queries total: "+totalQueries;
    println "Queries with time: "+withTimes.size();
    println "Sum: "+distribution.sampleStats.sum.toLong();

    println "Queries saved: "+savedQueries;
    return queriesOfTypeDurations.size();
}

println "Analyzing ${args[0]}..."

String getArgPatternForArg(String i){
    if(i.indexOf('$')>=0) {
        i = i.substring(i.indexOf('$')+1)
    }
    return '(?:\\s|=|\\(|>|<|\\,)(\\$'+i+')(?:\\s|=|\\)|\\,)';
}

for(int i=1;i<10001;i++) {
    String pattern = getArgPatternForArg(''+i);
    Pattern r = Pattern.compile(pattern);
    String text = '$'+i;
    ARGS_PATTERNS.put(text, r);
    ARGS_QUOTED_PATTERNS.put(text, Pattern.compile(Pattern.quote(text)));
}

String queryArgReplace(String text, String key, String value) {
    Pattern r = ARGS_PATTERNS[key];
    if(r==null){
        System.err.println("ERROR: no pattern found for ${key}. compiling");
        String pattern = getArgPatternForArg(key);
        r = Pattern.compile(pattern);
    }
    Pattern quotedR = ARGS_QUOTED_PATTERNS[key];
    if(quotedR==null){
        System.err.println("ERROR: no quoted pattern found for ${key}. compiling");
        quotedR = Pattern.compile(Pattern.quote(key));
    }

    Matcher m = r.matcher(text);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
        String arg0 = m.group(0);

        //String arg1 = arg0.replaceFirst(Pattern.quote(m.group(1)), (value));
        String arg1 = quotedR.matcher(arg0).replaceFirst(value);


        arg1 = arg1.replace('$', '\\$')
        m.appendReplacement(sb, arg1);
    }
    m.appendTail(sb); // append the rest of the contents
    return sb.toString();
}

if(args.size()>=2) {
    new File(args[1]).delete();
    outputStream = new BufferedOutputStream(new FileOutputStream(new File(args[1])))
}
else {
    outputStream = null;
}

long start = System.currentTimeMillis()
println "${new Date()} Extracting queries from ${args[0]}"

int queries = parseLogGetQueries(args[0], null);
println "Found $queries queries";

println "${new Date()} Done extracting for ${System.currentTimeMillis() - start} ms"

//
////String arg='$1'
//String a = /(?:\s|=|\()(\${arg})(?:\s|=|\)|\,)/
//String query = 'where a = $1 ';
//query = query.replaceAll(a, 'serviceCall$1$1');
//
//println query;
//def v = '%%'
//System.out.println(queryArgReplace(' lower($1,$2,$3) ', '$2', v.replace('$', '\\$')));

//def group = "2018-09-17 15:09:01.802 MSK [12471] ПОДРОБНОСТИ:  параметры: \$1 = '%%'" =~ /(\$\d+) = (\'.+?\')/
//
//def args = [:]
//def keys = [];
//if(group.hasGroup() && group.size()>0){
//
//    group.each{ it->
//        args[it[1]] = it[2];
//        keys<<it[1];
//    }
//}
//
//println args