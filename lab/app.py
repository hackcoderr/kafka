import os
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from pydantic import BaseModel, Field

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

class TopicRequest(BaseModel):
    name: str = Field(min_length=1, max_length=249)
    partitions: int = Field(default=3, ge=1, le=12)
    replication_factor: int = Field(default=1, ge=1, le=2)

class MessageRequest(BaseModel):
    topic: str
    value: str = Field(min_length=1)
    key: str | None = None
    acks: str = Field(default="all", pattern="^(0|1|all)$")

def admin():
    return KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="kafka-lab")

app = FastAPI(title="Kafka Lab")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def home(): return FileResponse("static/index.html")

@app.get("/notes")
def notes():
    return FileResponse("static/notes.html")

@app.get("/notes.md")
def notes_markdown():
    """The detailed, versioned reference behind the interactive course."""
    return FileResponse("README.md", media_type="text/plain; charset=utf-8")

@app.get("/api/health")
def health():
    try:
        client = admin(); topics = client.list_topics(); client.close()
        return {"connected": True, "brokers": BOOTSTRAP, "topic_count": len(topics)}
    except Exception as exc:
        return {"connected": False, "brokers": BOOTSTRAP, "detail": str(exc)}

@app.get("/api/topics")
def list_topics():
    try:
        client = admin(); metadata = client.describe_topics(client.list_topics()); client.close()
        return [{"name": x["topic"], "partitions": len(x["partitions"]), "replication": len(x["partitions"][0]["replicas"]) if x["partitions"] else 0} for x in metadata if not x["topic"].startswith("__")]
    except KafkaError as exc: raise HTTPException(503, f"Kafka is not ready: {exc}")

@app.get("/api/topics/{topic}/insights")
def topic_insights(topic: str):
    """Small, consumer-group-free view of the log boundaries for a topic."""
    try:
        client = admin()
        description = client.describe_topics([topic])[0]
        client.close()
        partitions = [TopicPartition(topic, item["partition"]) for item in description["partitions"]]
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
        )
        starts = consumer.beginning_offsets(partitions)
        ends = consumer.end_offsets(partitions)
        consumer.close()
        return {
            "name": topic,
            "partitions": [
                {"id": part.partition, "start": starts[part], "end": ends[part], "events": ends[part] - starts[part]}
                for part in partitions
            ],
            "replication": len(description["partitions"][0]["replicas"]) if description["partitions"] else 0,
        }
    except KafkaError as exc: raise HTTPException(400, str(exc))

@app.post("/api/topics", status_code=201)
def create_topic(request: TopicRequest):
    if not all(ch.isalnum() or ch in ".-_" for ch in request.name): raise HTTPException(400, "Use only letters, numbers, dots, hyphens, and underscores.")
    try:
        client = admin(); client.create_topics([NewTopic(request.name, request.partitions, request.replication_factor)]); client.close()
        return {"name": request.name, "message": "Topic created."}
    except TopicAlreadyExistsError: raise HTTPException(409, "That topic already exists.")
    except KafkaError as exc: raise HTTPException(503, str(exc))

@app.post("/api/messages", status_code=201)
def produce(request: MessageRequest):
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks=request.acks, key_serializer=lambda x: x.encode() if x else None, value_serializer=lambda x: x.encode())
        record = producer.send(request.topic, key=request.key, value=request.value).get(timeout=10)
        producer.flush(); producer.close()
        return {"topic": record.topic, "partition": record.partition, "offset": record.offset, "acks": request.acks}
    except KafkaError as exc: raise HTTPException(400, str(exc))

@app.get("/api/messages/{topic}")
def read_messages(topic: str, limit: int = 20, group_id: str | None = None, commit: bool = False):
    limit = max(1, min(limit, 100)); group_id = group_id or f"kafka-lab-preview-{uuid.uuid4()}"
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=group_id, auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=1200, key_deserializer=lambda x: x.decode(errors="replace") if x else None, value_deserializer=lambda x: x.decode(errors="replace") if x else None)
        messages = []; deadline = time.monotonic() + 3
        while len(messages) < limit and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=500, max_records=limit-len(messages)).values():
                for record in batch: messages.append({"partition": record.partition, "offset": record.offset, "key": record.key, "value": record.value, "timestamp": record.timestamp})
        if commit: consumer.commit()
        consumer.close(); return {"messages": messages, "group_id": group_id, "committed": commit}
    except KafkaError as exc: raise HTTPException(400, str(exc))
