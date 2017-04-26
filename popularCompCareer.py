from pyspark import SparkConf, SparkContext

def loadCareerNames():
    careerNames = {}
    with open("codeBook.items") as f:
        for line in f:
            fields = line.split('|')
       #print(fields)
            careerNames[int(fields[0])] = fields[1]
    return careerNames

conf = SparkConf().setMaster("local").setAppName("PopularComputerCareers")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadCareerNames())

lines = sc.textFile("file:///SparkCourse/employeed.dat")
careers = lines.map(lambda x: (int(x.split()[1]), 1))
careerCounts = careers.reduceByKey(lambda x, y: x + y)

flipped = careerCounts.map( lambda x : (x[1], x[0]))
sortedCareers = flipped.sortByKey()

sortedCareerWithNames = sortedCareers.map(lambda countCareer : (nameDict.value[countCareer[1]], countCareer[0]))

results = sortedCareerWithNames.collect()

for result in results:
    print result