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

String csvFilename = args[0];

FileReader freader = new FileReader(csvFilename);
BufferedReader reader = new BufferedReader(freader, 256*1024*1024)

String line = null;

def times = [];

while ( (line = reader.readLine()) != null) {
    def items = line.split("\\;");
    times << items[4];
}

File output = new File("histo.csv");
output.delete();
output = new File("histo.csv");

EmpiricalDistribution distribution = new EmpiricalDistribution();
distribution.load(times as double[])
def stat = distribution.getBinStats();
output << "Bin;Freq"+"\n";
stat.each{ it->
    if(!it.min.isNaN() && !it.max.isNaN()) {
        println "[$it.min..$it.max]: $it.n"
        output << "$it.min..$it.max;$it.n"+"\n";
    }
}

println "Queries total: "+times.size();
println "Sum: "+distribution.sampleStats.sum.toLong();


