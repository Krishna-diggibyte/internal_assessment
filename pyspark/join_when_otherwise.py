from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import avg, when

data3 = [(101, "Aravind"),
         (102, "John"),
         (103, "Sridevi")]

data4 = [("pyspark", 90, 101),
         ("sql", 70, 101),
         ("pyspark", 70, 102),
         ("sql", 60, 102),
         ("sql", 30, 103),
         ("pyspark", 20, 103)]

schema3 = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])

schema4 = StructType([
    StructField("course_name", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("student_id", IntegerType(), True)
])

input1 = spark.createDataFrame(data=data3, schema=schema3)

input2 = spark.createDataFrame(data=data4, schema=schema4)

new_input2 = input2.groupBy("student_id").agg(avg("marks").alias("percentage"))

# new_input2.display()

joined_table = input1.join(new_input2, input1.student_id == new_input2.student_id, "inner").drop(new_input2.student_id)
# joined_table.display()

result = joined_table.withColumn("Result", when(joined_table.percentage >= 70, 'Distinction')
                                 .when(joined_table.percentage >= 60, 'First Class')
                                 .when(joined_table.percentage >= 50, 'Second Class')
                                 .when(joined_table.percentage >= 40, 'Third Class')
                                 .when(joined_table.percentage <= 39, 'Fail')
                                 )
display(result)