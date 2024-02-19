package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.TopicListener;

@Component
public class Service {

  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);

  public void handle(String topic, String message) {
    if (random.nextInt(100) < 10) {
      LOGGER.info("10% ошибка при обработке!");
      throw new RuntimeException();
    }
    counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
        .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
    if (random.nextInt(100) < 2) {
      LOGGER.info("2% ошибка при обработке!");
      throw new RuntimeException();
    }
  }

  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>()).getOrDefault(message, new AtomicInteger(0)).get();
  }
}
