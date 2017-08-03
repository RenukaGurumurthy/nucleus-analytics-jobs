package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.gooru.nucleus.consumer.sync.jobs.commands.CommandProcessorBuilder;
import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.ConfigConstants;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer implements Runnable {
  private KafkaConsumer<String, String> consumer = null;
  private JSONObject config = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

  public MessageConsumer(KafkaConsumer<String, String> consumer, JSONObject config, ExecutorService service) {
    this.config = config;
    this.consumer = consumer;
  }

  @Override
  public void run() {
    LOGGER.debug("config : " + config);
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(200);
      for (ConsumerRecord<String, String> record : records) {
        switch (record.topic()) {
        case ConfigConstants.CONFIG_TEST_TOPIC:
          LOGGER.warn("Don't process test topic message : " + record.value());
          break;
        default:
          // FIXME: Revisit this logic.
          LOGGER.warn("Assume that message is coming from unknown topic. Send to handlers anyway");
          sendMessage(record.value());
        }
      }
    }

  }

  private void sendMessage(String record) {
    LOGGER.debug("Message : " + record);
    if (record != null && !record.isEmpty()) {
      JSONObject eventObject = null;
      try {
        eventObject = new JSONObject(record);
        final String eventName = eventObject.getString(AttributeConstants.ATTR_EVENT_NAME);
        LOGGER.debug("eventName {} ", eventName);
        try {
          CommandProcessorBuilder.lookupBuilder(eventName).build(eventObject);
        } catch (Exception e) {
          LOGGER.error("Error while processing event", e);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to parse kafka message. It should be JSONObject", e);
      }
    }
  }
}
