package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.HashSet;
import java.util.Set;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private Set<String> processedMessages = new HashSet<>();
  private final int NUM_OF_PROCESSED_ATTEMPS = 10;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord) {
    // что бы исключить повторную отправку сообщений не придумал ничего лучше чем стороннее хранилище
    // выполняется обычная проверка, была ли выполнена обработка ранее.
    // метод  ack.acknowledge() получается больше не нужен

    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    if (!processedMessages.contains(consumerRecord.value())){
      processedMessages.add(consumerRecord.value());
      service.handle("topic1", consumerRecord.value());
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    // поскольку мы обеспечили точную отправку сообщений минумум 1 раз, здесь мы не завершаем обработку сообщения
    // без явного уведомления об окончании обработки ack.acknowledge(). Некоторые сообщения будут обработаны
    // олее 1 раза

    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    // поскольку в internal лезть нежелательно, я попытался приблизиться к 100% exactlyOnce обработки
    // по факту получилось 98% exactlyOnce. Не изменяя логику service.handle() невозможно отловить 2% вероятность
    // выброса второго эксепшена и сообщение обрабатывается повторноэ
    // логика как в atMostOnce - в стороннем хранилище следим, было ли обработано сообщение. Если нет - пытаемся
    // его обработать до какого-то условного предельного значения попыток, что бы сообщение не подвисло

    if (!processedMessages.contains(consumerRecord.value())){
      boolean messageProcessed = false;
      int attemptCounter = 0;

      while (!messageProcessed && attemptCounter <= NUM_OF_PROCESSED_ATTEMPS) {
        attemptCounter++;
        try {
          service.handle("topic3", consumerRecord.value());
          messageProcessed = true;
        } catch (Exception ignore) {}
      }

      processedMessages.add(consumerRecord.value());
    }
  }
}