from pyspark.ml import Pipeline
from sparknlp import DocumentAssembler
from sparknlp.base import LightPipeline
from sparknlp.pretrained import UniversalSentenceEncoder, ClassifierDLModel


class SpamDetector:
    def __init__(self, spark):
        self.document_assembler = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")

        self.use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
            .setInputCols(["document"]) \
            .setOutputCol("sentence_embeddings")

        self.document_classifier = ClassifierDLModel.pretrained('classifierdl_use_spam', 'en') \
            .setInputCols(["document", "sentence_embeddings"]) \
            .setOutputCol("class")

        self.nlp_pipeline = Pipeline(stages=[self.document_assembler, self.use, self.document_classifier])
        self.light_pipeline = LightPipeline(self.nlp_pipeline.fit(spark.createDataFrame([['']]).toDF("text")))

    def predict(self, text):
        self.light_pipeline.fullAnnotate(text)['class'].values()[0]
