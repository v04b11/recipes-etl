# %%
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, split, expr, regexp_replace, regexp_extract, coalesce, lit, when

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Recipe Analysis") \
    .getOrCreate()

# %%
# Load the recipes.json file into a PySpark DataFrame
df_raw_recipes = spark.read.json("recipes.json")

# Display the DataFrame to ensure it's loaded correctly
df_raw_recipes.show()

# %%
# Convert the ingredients string to an array based on newline characters
df_ingredients_split_recipes = (
    df_raw_recipes.withColumn("ingredients_split", split(lower(trim(col("ingredients"))), "\n")
))

# Show the updated DataFrame to verify the change
df_ingredients_split_recipes.show()

# %%
#  Filter recipes that have any variations of "Chilies" in the ingredients. The regular expression that matches different variations of the word "Chilies"
# # c(h|h): Matches 'ch' or 'ch' (just an extra check for both).
# # i(l|l): Matches 'i' followed by 'l' (again, an extra check).
# # (i|i): Matches 'i', allowing for possible repetitions or misspellings.
# # (e|e)?: Matches an optional 'e'.
# # (s|s)?: Matches an optional 's'.
# # The entire expression can match "Chilies", "Chili", "Chilis", "Chiles", and various misspellings or alterations of these words.
# # .*: This before and after the regular expression allows for any characters (including none) before and after the word variations, making the match flexible in the context of longer ingredient names.

df_recipes_only_chilies = (df_ingredients_split_recipes
    .filter(expr("exists(ingredients_split, x -> lower(x) rlike '.*(c(h|h)i(l|l)(i|i)(e|e)?(s|s)?|chilis?|chiles?).*')"))
)

# Regular expressions to extract hours and minutes
hours_regex = r'(\d+)H'
minutes_regex = r'(\d+)M'

# Creating the totalTime column
df_recipes_only_chilies_time_min = (df_recipes_only_chilies
    .withColumn("cookTime", regexp_replace("cookTime", "PT", ""))
    .withColumn('cookTimeMinutes', coalesce(regexp_extract(col('cookTime'), hours_regex, 1).cast('int') * 60, lit(0)) +
                                      coalesce(regexp_extract(col('cookTime'), minutes_regex, 1).cast('int'), lit(0)))
    .withColumn("prepTime", regexp_replace("prepTime", "PT", ""))
    .withColumn('prepTimeMinutes', coalesce(regexp_extract(col('prepTime'), hours_regex, 1).cast('int') * 60, lit(0)) +
                                      coalesce(regexp_extract(col('prepTime'), minutes_regex, 1).cast('int'), lit(0)))
    .withColumn('totalTime', col('cookTimeMinutes') + col('prepTimeMinutes'))
)

# Drop the temporary columns if you don't need them
df_recipes_only_chilies_time_min = df_recipes_only_chilies_time_min.drop('cookTimeMinutes', 'prepTimeMinutes','ingredients_split')

# Add the difficulty column based on total time
df_recipes_only_chilies_with_dif = df_recipes_only_chilies_time_min.withColumn(
    "difficulty",
    when(col("totalTime") > 60, "Hard")
    .when((col("totalTime") >= 30) & (col("totalTime") <= 60), "Medium")
    .when((col("totalTime") > 0) & (col("totalTime") < 30), "Easy")
    .otherwise("Unknown")
).drop('totalTime')

df_recipes_only_chilies_with_dif.show()

# %%
# Specify the output directory for the CSV file
output_dir = "recipes_with_chilies_output"

# Coalesce the DataFrame to a single partition and save it as a CSV file
df_recipes_only_chilies_with_dif.coalesce(1).write.csv(output_dir, header=True, mode='overwrite')

# Find the part file and move it to the desired location
for file in os.listdir(output_dir):
    if file.startswith("part-"):
        # Move the file to a new name
        shutil.move(os.path.join(output_dir, file), "recipes_with_chilies.csv")
        break

# Remove the output directory because it's no longer needed
shutil.rmtree(output_dir)


