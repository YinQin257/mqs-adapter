# mqs-adapter

> 消息中间件适配器

![消息适配器](消息适配器.svg)

## 功能清单

- 目前支持rocketmq和kafka两种消息中间件。
- 支持多数据源
- 支持多种消费模式，批量消费，单条消费，广播消费

## 使用说明

### 配置定义

```yaml
mqs:
  #组件类型
  rocketmq:
    #自定义名称（生产者 or 消费者名称）
    rocketmq32:
      # 消费组名称
      group-name: MQS_TEST
      # 批量消费数量
      consume-message-batch-max-size: 20
      # 消费最小线程
      consume-thread-min: 10
      # 消费最大线程
      consume-thread-max: 15
      # rocketmq原生配置
      client-config:
        namesrv-addr: 10.100.15.32:9876
    #生产者名称
    rocketmq128:
      # 消费组名称
      group-name: MQS_TEST
      client-config:
        namesrv-addr: 192.168.175.128:9876
  #组件类型
  kafka:
    #生产者名称
    kafka33:
      group-name: MQS_TEST
      client-config:
        bootstrap.servers: 192.168.175.128:9092,192.168.175.128:19092,192.168.175.128:29092
```

### 生产者使用

```java
@Autowired
ProducerManager producerManager;

// 从生产者管理器中获取指定生产者
MessageProducer producer = producerManager.get('rocketmq32');
// 使用指定生产者发送消息
producer.sendMessage(message);
```

### 消费者使用

使用@MessageAdapter注解，声明使用的组件自定义名称、topic。

实现MessageHandler接口即可。

```java
@Component
@MessageAdapter(vendorName = "kafka33",topicName = "MQS_TEST_TOPIC")
public class ConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);


    @Override
    public void process(AdapterMessage message) throws Exception {

    }

    @Override
    public void process(List<AdapterMessage> messages) throws Exception {
        logger.info("监听到批量消息，消息总数为：{}", messages.size());
        messages.forEach(message -> {
            logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
        });
    }
}
```

