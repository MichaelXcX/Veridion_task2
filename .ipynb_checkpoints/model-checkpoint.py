import pandas as pd
import sparknlp
from sparknlp.training import CoNLL
from sparknlp.annotator import *
from sparknlp.base import *
from pyspark.ml import Pipeline
from sparknlp_display import NerVisualizer

products = pd.read_csv('./products.csv').to_numpy()
products = products[1:]
    
spark = sparknlp.start()

print('Spark NLP version', sparknlp.version())
print('Apache Spark version', spark.version)

train_data = CoNLL().readDataset(spark, './eng.train')
train_data.show()

bert = BertEmbeddings.pretrained('bert_base_cased', 'en') \
    .setInputCols(["sentence",'token'])\
    .setOutputCol("bert")\
    .setCaseSensitive(False)

nerTagger = NerDLApproach()\
    .setInputCols(["sentence", "token", "bert"])\
    .setLabelColumn("label")\
    .setOutputCol("ner")\
    .setMaxEpochs(10)\
    .setRandomSeed(0)\
    .setVerbose(1)\
    .setValidationSplit(0.2)\
    .setEvaluationLogExtended(True)\
    .setEnableOutputLogs(True)\
    .setIncludeConfidence(True)\

ner_pipeline = Pipeline(stages=[bert, nerTagger])
ner_model = ner_pipeline.fit(train_data)

products = pd.read_csv('./products.csv').to_numpy()

test_products = []
for product in products:
    test_products.append(product[0])

test_data = spark.createDataFrame(pd.DataFrame({'text': test_products}))

predicts = ner_model.transform(test_data)

ner_vis = NerVisualizer()

ner_vis.display(predicts, label_col='entities', document_col='text', labels=['PROD', 'MISC'], output_file='ner_vis.html')

ner_vis.set_label_colors({'PROD':'#008080', 'MISC':'#AFEEEE'})

# parquetFile = spark.read.parquet("list of company websites.snappy.parquet")