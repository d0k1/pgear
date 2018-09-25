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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
EmpiricalDistribution distribution = new EmpiricalDistribution();

String csvFilename = args[0];

FileReader freader = new FileReader(csvFilename);
BufferedReader reader = new BufferedReader(freader, 256*1024*1024)

String line = null;

def times = [];

while ( (line = reader.readLine()) != null) {
    def items = line.split("\\;");
    double time = Double.parseDouble(items[5])

    descriptiveStatistics.addValue(time);
    times << time;
}

File output = new File("histo.csv");
output.delete();
output = new File("histo.csv");


distribution.load(times as double[])
def stat = distribution.getBinStats();
output << "Bin;Freq"+"\n";
stat.each{ it->
    if(!it.min.isNaN() && !it.max.isNaN()) {
        println "[$it.min..$it.max]: $it.n"
        output << "$it.min..$it.max;$it.n"+"\n";
    }
}
output<<"\nmin;50%;95%;99%;99.9%;max;stddev;n\n"

output << descriptiveStatistics.min <<";"
output << descriptiveStatistics.getPercentile(50.0) <<";"
output << descriptiveStatistics.getPercentile(95.0) <<";"
output << descriptiveStatistics.getPercentile(99.0) <<";"
output << descriptiveStatistics.getPercentile(99.9) <<";"
output << descriptiveStatistics.max <<";"
output << descriptiveStatistics.standardDeviation <<";"
output << descriptiveStatistics.n <<";"

println "Queries total: "+times.size();
println "Sum: "+distribution.sampleStats.sum.toLong();
print "\nmin;50%;95%;99%;99.9%;max;stddev;n\n";
print  descriptiveStatistics.min+";"
print  descriptiveStatistics.getPercentile(50.0) +";"
print  descriptiveStatistics.getPercentile(95.0) +";"
print  descriptiveStatistics.getPercentile(99.0) +";"
print  descriptiveStatistics.getPercentile(99.9) +";"
print  descriptiveStatistics.max+";"
print  descriptiveStatistics.standardDeviation +";"
print  descriptiveStatistics.n+";"
println ""




