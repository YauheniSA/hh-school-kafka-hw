package ru.hh.kafkahw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;
  private final int NUM_OF_SENT_ATTEMPS = 10;
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  public void doSomething(String topic, String message) {
    // для семантик atLeastOnce и exactlyOnce нужно убедиться, что сообщение отправлено хотя бы один раз
    // введена константа в 10 попыток отправки, что бы в приближенной к реальности ситуации приложение не
    // подвисло в этом месте. При таком подходе сообщение будет отправлено минимум 1 раз.
    boolean messageSent = false;
    int attemptCounter = 0;
    // пока сообщение не отправлено и не достигнуто предельное количество попыток - пытаемся отправлять
    while (!messageSent && attemptCounter <= NUM_OF_SENT_ATTEMPS) {
      try {
        attemptCounter ++;
        producer.send(topic, message);
        messageSent = true;
      } catch (Exception e) {
        LOGGER.info("Продюссер не смог отправить сообщение: {}", message);
      }
    }
  }
}
