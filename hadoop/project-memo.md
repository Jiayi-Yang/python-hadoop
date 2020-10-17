## 1. Create a Kafka Topic
```bash
kafka-topics --bootstrap-server ip-172-31-91-232.ec2.internal:9092 --create --replication-factor 2 --partitions 2 --topic rsvp_jaye
```
## 2. Create Kafka Data Source
### Method 1: Console producer
```bash
curl https://stream.meetup.com/2/rsvps | kafka-console-producer -broker-list ip-172-31-91-232.ec2.internal:9092 --topic rsvp_jaye
```