from pyspark import SparkContext


def obter_taxa(x: str):
    taxa = x.split(";")[1]
    return taxa, 1


# Inicializando o SparkContext
sc = SparkContext("local", "MapReduceExample")

# remove header
input_rdd = sc.textFile("taxa-selic.csv")
header = input_rdd.first()
values = input_rdd.filter(lambda x: (x != header))

rdd_mapped = values.map(obter_taxa)

rdd_reduced = rdd_mapped.reduceByKey(lambda x, y:  x + y)

# Exibindo os resultados
results = rdd_reduced.collect()

sorted_list = sorted(
    results,
    key=lambda t: t[1],
    reverse=True
)

for word, count in sorted_list:
    print(f"{word}: {count}")

# Encerrando o SparkContext
sc.stop()
