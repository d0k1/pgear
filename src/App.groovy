import org.apache.commons.math3.random.EmpiricalDistribution

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
@Grapes(
@Grab(group='org.apache.commons', module='commons-math3', version='3.6.1')
)

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
}

Double getTime(String line){
    def group = line =~ /продолжительность\:.(\d+).(\d+)/
    if(group.hasGroup() && group.size()>0){
        String time = group[0][1]+"."+group[0][2];
        return Double.valueOf(time);
    }

    return null;
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

            if(index<0) {
                index = tempLine.indexOf("WITH");
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
//            query.full = query.full.replaceAll("\\${it[1]}", it[2]);
        }
    }

    keys.sort {-it.size()};
    keys.each { arg->
        query.full = query.full.replace(arg, args[arg]);
    }
}

void saveAQuery(Query query){

    prepareFullQueary(query);

    File file = new File("result"+File.separatorChar+query.id+"_"+query.time+".src")
    file<<query.text;
    file<<"\n";
    file<<query.args;

    File file2 = new File("result"+File.separatorChar+query.id+"_"+query.time+".sql")
    file2<<query.full;
}

int parseLogGetTimeStat(String filename){
    def times = [];

    new File(filename).eachLine { line ->
        Double time = getTime(line);
        if(time!=null){
            times+=time;
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
    return times.size();
}

int parseLogGetQueries(String filename){
    String tempLine = "";

    int queries = 0;

    def lines = [];
    new File(filename).eachLine {line->

        if(isItEventStart(line)) {
            if(!isItAQueryStart(line) && isItAQueryStart(tempLine)){
                // окончание запроса
                tempLine = "";

                Query query = createAQuery(lines);
                query.id=queries;
                saveAQuery(query);

                queries++;
            }
            else if (isItAQueryStart(line) && isItAQueryStart(tempLine)) {
                // окончание запроса
                tempLine = "";

                Query query = createAQuery(lines);
                query.id=queries;
                saveAQuery(query);

                queries++;
            }
            if(isItAQueryStart(line) && !isItAQueryStart(tempLine)){
                lines = [];
                // начало запроса
                tempLine = line;
            }
        }
        lines+=line;
    }

    return queries;
}

println 'Starting...'

int queries = parseLogGetQueries(args[0]);
//int queries = parseLogGetTimeStat(args[0]);
//int queries = 0;
//
//String test = '''$1 = 'secondLine', $2 = 'ThirdLine' ''';
//
//def group = test =~ /(\$\d+) = (\'.+?\')/
//if(group.hasGroup() && group.size()>0){
//
//    group.each{ it->
//        println "${it[1]} = ${it[2]}"
//    }
//
//}

println "Found $queries queries";
