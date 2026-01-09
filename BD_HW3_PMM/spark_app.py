import os
import shutil
import sys
from datetime import datetime
import time


os.environ['HADOOP_HOME'] = "C:\\hadoop"

sys_path = os.environ.get('PATH', '')
if "C:\\hadoop\\bin" not in sys_path:
    os.environ['PATH'] = sys_path + ";C:\\hadoop\\bin"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf, lower
from pyspark.sql.types import StringType, StructType, StructField

kafka_topic = "telegram_data"
kafka_server = "localhost:9092"
output_dir = "spark_results"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def clean_suffix_logic(word):
    if not word:
        return ""
    vowels = "аеёиоуыэюяйАЕЁИОУЫЭЮЯЙ"
    while len(word) > 0 and word[-1] in vowels:
        word = word[:-1]
    return word

clean_suffix_udf = udf(clean_suffix_logic, StringType())

def process_batch(df, epoch_id):
    try:
        if df.rdd.isEmpty():
            return
    except:
        return

    sorted_df = df.orderBy(col("count").desc())

    pdf = sorted_df.toPandas()
    
    if pdf.empty:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(output_dir, f"result_{timestamp}.txt")
    
    with open(filename, "w", encoding="utf-8") as f:
        for index, row in pdf.iterrows():
            f.write(f"{row['word']}, {row['count']}\n")
            
    print(f"[{datetime.now().time()}] Сохранено в {filename}")
    print(f"ТОП-10: {pdf.head(10)[['word', 'count']].values.tolist()}")

def main():
    print(f"Python path: {sys.executable}")
    print("Запуск Spark Session...")
    
    spark = SparkSession.builder \
        .appName("TelegramWordCount") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()
    except Exception as e:
        print(f"ОШИБКА ПОДКЛЮЧЕНИЯ К KAFKA: {e}")
        return

    df_text = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
                    .select(from_json(col("json_str"), StructType([StructField("text", StringType())])).alias("data")) \
                    .select("data.text")

    words = df_text.select(explode(split(col("text"), "[^а-яА-ЯёЁ]+")).alias("raw_word"))

    stop_words_list = [
        "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она", "так", "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "только", "ее", "мне", "было", "вот", "от", "меня", "еще", "нет", "о", "из", "ему", "теперь", "когда", "даже", "ну", "вдруг", "ли", "если", "уже", "или", "ни", "быть", "был", "него", "до", "вас", "нибудь", "опять", "уж", "вам", "ведь", "там", "потом", "себя", "ничего", "ей", "может", "они", "тут", "где", "есть", "надо", "ней", "для", "мы", "тебя", "их", "чем", "была", "сам", "чтоб", "без", "будто", "чего", "раз", "тоже", "себе", "под", "будет", "ж", "тогда", "кто", "этот", "того", "потому", "этого", "какой", "совсем", "ним", "здесь", "этом", "один", "почти", "мой", "тем", "чтобы", "нее", "сейчас", "были", "куда", "зачем", "всех", "никогда", "можно", "при", "наконец", "два", "об", "другой", "хоть", "после", "над", "больше", "тот", "через", "эти", "нас", "про", "всего", "них", "какая", "много", "разве", "три", "эту", "моя", "впрочем", "хорошо", "свою", "этой", "перед", "иногда", "лучше", "чуть", "том", "нельзя", "такой", "им", "более", "всегда", "конечно", "всю", "между"
    ]

    proper_nouns = words.filter(col("raw_word").rlike("^[А-ЯЁ][а-яё]+$"))

    filtered_nouns = proper_nouns.filter(~lower(col("raw_word")).isin(stop_words_list))

    cleaned_words = filtered_nouns.withColumn("word", clean_suffix_udf(col("raw_word"))) \
                                  .filter(col("word") != "")

    word_counts = cleaned_words.groupBy("word").count()

    print("Слушаю Kafka и сохраняю файлы...")
    query = word_counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(process_batch) \
        .trigger(processingTime='1 minute') \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Остановка...")
        query.stop()

if __name__ == "__main__":
    main()