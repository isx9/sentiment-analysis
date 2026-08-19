# Sentiment Analysis of Tweets with Spark + Kafka

Multiclass sentiment classification (positive / negative / uncertainty /
litigious) on tweets, built for the Hardware and Software for Big Data
course. Combines an offline Spark ML training pipeline with a real-time
Kafka + Spark Structured Streaming inference pipeline.

## Project structure

.
├── sentiment_analysis_testing.ipynb # main notebook: training + streaming inference
├── docker-compose.yml # local single-node Kafka broker
├── producer.py # publishes tweets into Kafka for the streaming demo
├── dataset.csv # tweet dataset (see Data source below)
└── README.md


## Requirements

- Python 3.10+
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- PySpark (`pip install pyspark`)
- `kafka-python` (`pip install kafka-python`)
- Jupyter (`pip install jupyterlab`)

## Data source

Tweets dataset: [Sentiment dataset with 1 million tweets (Kaggle)](https://www.kaggle.com/datasets/tariqsays/sentiment-dataset-with-1-million-tweets)

## How to run

### 1. Start Kafka
```bash
docker compose up -d
docker ps   # confirm the "kafka" container is Up
```

### 2. Stream tweets into Kafka
In a terminal, from the project folder:
```bash
python producer.py --file dataset.csv --delay 2
```
Leave this running — it continuously publishes tweets to the `tweets-input`
topic that the notebook consumes from.

### 3. Run the notebook
In a separate terminal:
```bash
jupyter lab
```
Open `sentiment_analysis_testing_FIXED.ipynb` and **Run → Run All Cells**.

The notebook will:
1. Load and clean the dataset.
2. Train a Logistic Regression classifier on TF-IDF features (Tokenizer →
   StopWordsRemover → CountVectorizer → IDF → LogisticRegression).
3. Evaluate the model (accuracy, precision, recall).
4. Connect to Kafka and classify tweets in real time via Spark Structured
   Streaming, printing predictions per micro-batch for ~2 minutes.

### 4. Shut down
```bash
# Ctrl+C in the producer terminal
docker compose down
```

## Approach

- **Batch training**: Spark MLlib pipeline (Tokenizer, StopWordsRemover,
  CountVectorizer, IDF, LogisticRegression) trained on an 80/20 split of
  the English-language subset of the dataset.
- **Streaming inference**: Spark Structured Streaming reads tweets from a
  Kafka topic (`tweets-input`) and applies the same fitted pipeline to each
  micro-batch via `foreachBatch`, printing predicted sentiment and class
  probabilities.

## Evaluation

Reported via `MulticlassClassificationEvaluator` in the notebook: accuracy,
weighted precision, and weighted recall on the held-out test set.
