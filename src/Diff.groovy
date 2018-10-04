String csvFilename0 = args[0];
String csvFilename1 = args[1];

class Measurement{
    long index;
    String date;
    long time;
}

def test0 = [];

def test1 = [];

def readMeasurements(String csvFilename){
    def data = [];
    FileReader freader = new FileReader(csvFilename);
    BufferedReader reader = new BufferedReader(freader, 256*1024*1024)

    String line = null;
    int errors = 0;

    while ( (line = reader.readLine()) != null) {
        def items = line.split("\\;");
        try {
            long time = Long.parseLong(items[5].trim());
            long index = Long.parseLong(items[1].trim());
            String date = items[0];

            data<< new Measurement(index:index, time:time, date:date);

        } catch (Exception e){
            errors++;
        }
    }
    return data;
}

def compare(List measurements0, List measurements1){
    def result = [];

    measurements0.each{Measurement m0->
        if(measurements1.size()>m0.index) {
            Measurement m1 = measurements1.get(m0.index as Integer)
            result<<new Measurement(index: m0.index, time:m1.time-m0.time);
        } else {
            result << new Measurement(index:m0.index, time: Long.MAX_VALUE);
        }
    }

    return result;
}

println "Comparing queries between ${csvFilename0} and ${csvFilename1}"

test0 = readMeasurements(csvFilename0);
test1 = readMeasurements(csvFilename1);

test0.sort { it.index }

test1.sort { it.index }

println("Got ${test0.size()} items in the first file and ${test1.size()} in the second one");

def diff = compare(test0, test1);

File output = new File("diff.csv");
output.delete();
output = new File("diff.csv");

output << "index;time;\n";

diff.each {Measurement item->
    output << "${item.index};${item.time}\n";
}
