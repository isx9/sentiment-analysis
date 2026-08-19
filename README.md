# Sentiment Analysis of Tweets with Spark + Kafka

![Python](https://img.shields.io/badge/python-3.10+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-academic%20project-orange)

Multiclass sentiment classification (positive / negative / uncertainty / litigious) on tweets, combining an offline Spark ML training pipeline with a real-time Kafka + Spark Structured Streaming inference pipeline.

Built for the Hardware and Software for Big Data course at the University of Naples Federico II (2026).

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Dataset](#dataset)
- [How to Run](#how-to-run)
- [Results](#results)
- [Documentation](#documentation)
- [License](#license)

## Architecture

```
Kafka Producer (producer.py)
|
v
Kafka Topic: tweets-input
|
v
Spark Structured Streaming (micro-batches)
|
v
Fitted ML Pipeline (Tokenizer → StopWordsRemover → CountVectorizer → IDF → LogisticRegression)
|
v
Predicted Sentiment + Class Probabilities
```


The same pipeline is trained and evaluated offline on a static 80/20 train/test split, then reused unmodified at inference time — so streaming predictions use an identical feature representation and decision boundary
to the one validated during batch evaluation.

## Project Structure

```
├── sentiment_analysis.ipynb # main notebook: training + streaming inference
├── docker-compose.yml # local single-node Kafka broker
├── producer.py # publishes tweets into Kafka for the streaming demo
├── requirements.txt
├── LICENSE
├── .gitignore
├── doc/
│ └── project_report.pdf # full write-up: challenge, methodology, results
├── data/
│ └── README.md # where to download the dataset
└── README.md
```


## Technology Stack

- **Apache Spark (PySpark)** — batch training + Structured Streaming inference
- **Apache Kafka** — real-time message ingestion
- **Spark MLlib** — Tokenizer, StopWordsRemover, CountVectorizer, IDF, Logistic Regression
- **Docker** — local Kafka broker

## Dataset

[Sentiment dataset with 1 million tweets (Kaggle)](https://www.kaggle.com/datasets/tariqsays/sentiment-dataset-with-1-million-tweets) —
937,854 tweets labeled across four sentiment classes: positive, negative,
uncertainty, litigious.

Download `dataset.csv` and place it in `data/` — not included in this repo due to its size (around 167MB). See [`data/README.md`](data/README.md).

## How to Run

**1. Start Kafka**
```bash
docker compose up -d
docker ps   # confirm the "kafka" container is Up
```

**2. Stream tweets into Kafka**
```bash
pip install -r requirements.txt
python producer.py --file data/dataset.csv --delay 2
```
Leave this running — it continuously publishes tweets to the `tweets-input` topic that the notebook consumes from.

**3. Run the notebook**
```bash
jupyter lab
```
Open `sentiment_analysis.ipynb` and **Run → Run All Cells**. It will train the model, evaluate it, then connect to Kafka and classify tweets in real time for around 2 minutes.

**4. Shut down**
```bash
# Ctrl+C in the producer terminal
docker compose down
```

## Results

| Metric              | Score  |
|----------------------|--------|
| Accuracy             | 95.6%  |
| Weighted Precision    | 95.6%  |
| Weighted Recall       | 95.6%  |

Streaming predictions on live, unseen tweets show varying class probabilities across sentiment categories, confirming the model performs genuine per-message inference rather than returning a static output — see
[`doc/project_report.pdf`](doc/project_report.pdf) for the full evaluation, confusion matrix, and streaming output samples.

## Documentation

Full report — challenge description, methodology, architecture, and experimental results — in [`doc/project_report.pdf`](doc/project_report.pdf).

## License

MIT — see [LICENSE](LICENSE).
