data = [
    ("John", "Apple", 10),
    ("Alice", "Apple", 20),
    ("John", "Banana", 12),
    ("Alice", "Banana", 14),
    ("Mike", "Apple", 15),
    ("Mike", "Banana", 17)
]

schema = ["salesperson", "product", "quantity"]

input_df = spark.createDataFrame(data=data, schema=schema)
input_df.display()

result_df = input_df.groupBy("product").pivot("salesperson").sum("quantity").orderBy("product")

result_df.display(