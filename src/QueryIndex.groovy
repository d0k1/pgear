
def queries = []
String queriesFilename = args[0];


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

String num = null;
reader = System.in.newReader();
while((num = reader.readLine()) != null){
    if(num.length()==0){
        break
    }
    println queries[num.toInteger()]
}

println("bye!")