from pyspark.sql.functions import when
from pyspark.sql.functions import concat_ws

data = [
    (1, 'John', None, 'Doe'),
    (2, 'Alice', 'Ann', 'Smith'),
    (3, 'Mike', None, 'Johnson')
]

schema = ["id", "first_name", "middle_name", "last_name"]

input_df = spark.createDataFrame(data=data, schema=schema)
input_df.display()
df2 = input_df.withColumn('full_name', concat_ws(" ", "first_name", "middle_name", "last_name"))
df2.display()