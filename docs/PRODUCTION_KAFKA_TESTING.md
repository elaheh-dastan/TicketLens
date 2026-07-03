# Production Kafka Testing Guide

This guide explains how to test the QC Agent deployment with production Kafka using Avro serialization.

## Prerequisites

1. **Kafka Cluster Access**: Ensure you can reach the Kafka cluster at `kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`
2. **OpenRouter API Key**: Required for LLM calls
3. **Python Environment**: Python 3.13+ with all dependencies installed

## Configuration

### 1. Set Up Environment Variables

Copy the production environment template:

```bash
cp .env.production .env
```

Edit `.env` and update the following:

```bash
# Required: Your OpenRouter API key
LLMS__OPENROUTER_API_KEY=sk-or-v1-your-actual-key-here

# Kafka settings (already configured for production)
KAFKA__ENABLED=true
KAFKA__BOOTSTRAP_SERVERS=kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
KAFKA__INPUT_TOPIC=ai.ticket-qc-input
KAFKA__RESULTS_TOPIC=ai.ticket-qc-results
KAFKA__SERIALIZATION_FORMAT=avro
```

### 2. Verify Configuration

Check that settings are loaded correctly:

```bash
python scripts/check_settings.py
```

Expected output should show:
- Kafka enabled: `true`
- Bootstrap servers: `kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`
- Serialization format: `avro`

## Testing Workflows

### Workflow 1: Send Test Message

Send a test message to the input topic:

```bash
# Send a good support conversation
python scripts/test_production_kafka.py send --chat-type good_support

# Send a poor support conversation
python scripts/test_production_kafka.py send --chat-type poor_support

# Send a technical issue conversation
python scripts/test_production_kafka.py send --chat-type technical_issue
```

**Expected Output:**
```
📊 Production Kafka Settings:
   Bootstrap Servers: kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
   Input Topic: ai.ticket-qc-input
   Results Topic: ai.ticket-qc-results
   Serialization: avro

📝 Sending test message:
   Chat Type: good_support
   Chat ID: prod-test-good_support-20260215134500
   Correlation ID: corr-prod-test-good_support-20260215134500
   Messages: 6

✅ Test message sent successfully!
   Topic: ai.ticket-qc-input
   Correlation ID: corr-prod-test-good_support-20260215134500
```

### Workflow 2: Monitor Results Topic

In a separate terminal, monitor the results topic:

```bash
# Monitor all messages
python scripts/test_production_kafka.py monitor

# Monitor specific correlation ID
python scripts/test_production_kafka.py monitor --correlation-id corr-prod-test-good_support-20260215134500

# Monitor with custom timeout (default: 300s)
python scripts/test_production_kafka.py monitor --timeout 600
```

**Expected Output:**
```
👀 Monitoring results topic: ai.ticket-qc-results
   Timeout: 300s
   Press Ctrl+C to stop

================================================================================
📨 RESULT MESSAGE #1
================================================================================
Correlation ID: corr-prod-test-good_support-20260215134500
Source Agent: QC_Agent
Status: SUCCESS
Timestamp: 2026-02-15T13:45:30.123Z

----------------------------------------
Payload:
{
  "chat_id": "prod-test-good_support-20260215134500",
  "qc_result": {
    "overall_score": 8.5,
    "categories": {
      "professionalism": 9,
      "responsiveness": 8,
      "problem_solving": 9,
      "communication": 8
    },
    "feedback": "پشتیبانی خوبی ارائه شده...",
    "suggestions": [...]
  }
}
================================================================================
```

### Workflow 3: Send and Monitor (Combined)

Send a message and automatically monitor for the result:

```bash
# Send and wait for result (default timeout: 300s)
python scripts/test_production_kafka.py send-and-monitor --chat-type good_support

# With custom timeout
python scripts/test_production_kafka.py send-and-monitor --chat-type poor_support --timeout 600
```

This workflow:
1. Sends the test message
2. Waits 5 seconds
3. Starts monitoring for the specific correlation ID
4. Stops automatically when the result is received

### Workflow 4: Using Original Scripts

You can also use the original scripts:

```bash
# Send message (uses JSON serialization by default)
python scripts/send_test_kafka_message.py

# Read input topic
python scripts/read_kafka_qc_input.py
```

**Note:** These scripts use JSON serialization. For Avro testing, use [`test_production_kafka.py`](scripts/test_production_kafka.py).

## Available Chat Types

The test script includes three pre-configured chat conversations:

### 1. `good_support` (Default)
- Professional and helpful agent
- Clear problem resolution
- Compensation offered
- Expected QC score: 8-9/10

### 2. `poor_support`
- Unhelpful responses
- No problem resolution
- Poor communication
- Expected QC score: 2-4/10

### 3. `technical_issue`
- Technical problem handling
- Appropriate escalation
- Clear instructions
- Expected QC score: 7-8/10

## Troubleshooting

### Connection Issues

If you can't connect to Kafka:

```bash
# Test network connectivity
ping kafka-kafka-bootstrap.kafka.svc.cluster.local

# Check if Kafka port is accessible
nc -zv kafka-kafka-bootstrap.kafka.svc.cluster.local 9092

# Verify DNS resolution
nslookup kafka-kafka-bootstrap.kafka.svc.cluster.local
```

### Serialization Errors

If you see Avro serialization errors:

1. Check that the schema file exists:
   ```bash
   ls -la src/kafka/schemas/agent_envelope.avsc
   ```

2. Verify the schema is valid:
   ```bash
   python -c "from src.kafka.avro_serializer import load_avro_schema; print(load_avro_schema())"
   ```

3. Check serialization format in settings:
   ```bash
   python -c "from src.config.settings import get_settings; print(get_settings().kafka.serialization_format)"
   ```

### No Messages Received

If monitoring doesn't show any messages:

1. **Check if agent is running:**
   ```bash
   # In production, check pod status
   kubectl get pods -n your-namespace | grep qc-agent
   
   # Check logs
   kubectl logs -f deployment/qc-agent -n your-namespace
   ```

2. **Verify message was sent:**
   - Check the correlation ID from send output
   - Verify topic name matches configuration

3. **Check consumer group:**
   ```bash
   # List consumer groups
   kafka-consumer-groups.sh --bootstrap-server kafka-kafka-bootstrap.kafka.svc.cluster.local:9092 --list
   
   # Check group status
   kafka-consumer-groups.sh --bootstrap-server kafka-kafka-bootstrap.kafka.svc.cluster.local:9092 --group agent-qc --describe
   ```

### Timeout Issues

If messages timeout:

1. **Increase timeout:**
   ```bash
   python scripts/test_production_kafka.py monitor --timeout 600
   ```

2. **Check agent processing time:**
   - LLM calls can take 10-30 seconds
   - Complex conversations may take longer
   - Default timeout is 300s (5 minutes)

3. **Check agent logs for errors:**
   ```bash
   kubectl logs -f deployment/qc-agent -n your-namespace --tail=100
   ```

## Advanced Testing

### Custom Messages

Send custom chat conversations:

```python
import asyncio
from scripts.test_production_kafka import send_test_message

# Define custom chat
custom_chat = [
    {
        "role": "customer",
        "message": "Your custom message here",
        "timestamp": "2026-02-15T10:00:00Z",
    },
    {
        "role": "agent",
        "message": "Agent response",
        "timestamp": "2026-02-15T10:01:00Z",
    },
]

# Send it
asyncio.run(send_test_message("custom"))
```

### Load Testing

For load testing, use a loop:

```bash
# Send 10 messages
for i in {1..10}; do
    python scripts/test_production_kafka.py send --chat-type good_support
    sleep 2
done
```

### Monitoring Multiple Messages

Monitor all messages for a period:

```bash
# Monitor for 10 minutes
python scripts/test_production_kafka.py monitor --timeout 600
```

## Integration with CI/CD

Example GitLab CI job:

```yaml
test-kafka-production:
  stage: test
  script:
    - cp .env.production .env
    - echo "LLMS__OPENROUTER_API_KEY=$OPENROUTER_API_KEY" >> .env
    - python scripts/test_production_kafka.py send-and-monitor --timeout 300
  only:
    - main
  when: manual
```

## Monitoring Best Practices

1. **Use correlation IDs**: Always track specific messages with correlation IDs
2. **Set appropriate timeouts**: 300s for normal testing, 600s for load testing
3. **Monitor logs**: Keep agent logs visible during testing
4. **Check metrics**: Monitor Kafka lag, processing time, error rates
5. **Test all scenarios**: Good support, poor support, technical issues

## Next Steps

After successful testing:

1. **Production Deployment**: Deploy the QC agent to production
2. **Monitoring Setup**: Configure Langfuse, Prometheus, Grafana
3. **Alerting**: Set up alerts for errors, timeouts, high latency
4. **Documentation**: Document production procedures
5. **Training**: Train support team on QC results interpretation

## Support

For issues or questions:
- Check logs: `kubectl logs -f deployment/qc-agent`
- Review Kafka topics: Use Kafka UI or CLI tools
- Check this documentation: [`docs/PRODUCTION_KAFKA_TESTING.md`](docs/PRODUCTION_KAFKA_TESTING.md)
- Review agent configuration: [`agent_config/qc_agent.yml`](agent_config/qc_agent.yml)
