package ru.zhenik.akka.example.persistence;

import static java.lang.System.out;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Pair;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.alpakka.file.javadsl.Directory;
import akka.stream.alpakka.file.javadsl.DirectoryChangesSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.FiniteDuration;

public class App {
  // kafka topic
  public static final String TOPIC = "new-topic";

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    // config
    final Config conf = ConfigFactory.load();
    final String kafkaBootstrapServers = conf.getString("component-file.kafka.bootstrap-servers");
    final String imgDir = conf.getString("component-file.file-root-path");
    final String appName = conf.getString("component-file.name");
    System.out.println("config:kafka-bootstrap-servers = " + kafkaBootstrapServers);
    System.out.println("config:img-dir = " + imgDir);
    System.out.println("config:app-name = " + appName);


    // Akka
    final ActorSystem system = ActorSystem.create(appName);
    //TODO
    final ActorRef persistentActor = null;

    final Materializer materializer = ActorMaterializer.create(system);

    // File system access
    final FileSystem fs = FileSystems.getDefault();
    final FiniteDuration pollingInterval = FiniteDuration.create(1, TimeUnit.SECONDS);
    final int maxBufferSize = 1000000;
    // PreStart: create topic if not exist
    preStart(kafkaBootstrapServers);

    // Two sources will be concatenated later
    // Source LIST all existing files in directory
    final Source<Path, NotUsed> listDirSource =
        Directory.ls(fs.getPath(imgDir))
            .filter(e -> {
              System.out.println("FILENAME: "+e.toString());
              return e.toString().endsWith(".jpg");
            })
            .map(e -> {
              out.println("Existing file: " + e.toString());
              return e;
            });
    // Source CHANGE LISTENER
    final Source<Path, NotUsed> changesSource =
        DirectoryChangesSource
            .create(fs.getPath(imgDir), pollingInterval, maxBufferSize)
            .filter(pair -> pair.second().equals(DirectoryChange.Creation))
            .map(Pair::first)
            .map(e -> {
              out.println("New file: " + e.toString());
              return e;
            });

    // Flow
    // TODO: how2 affect FileNotFound
    final Flow<Path, ProducerRecord<String, String>, NotUsed> flow = Flow
        .of(Path.class)
        .map(Path::toString)
        .map(File::new)
        .map(file -> {
          try {
            final String componentId = file.getCanonicalPath();
            return new ProducerRecord<String, String>(TOPIC, componentId, componentId);
          } catch (Throwable throwable) {
            throwable.printStackTrace();
            return null;
          }
        })
        .filterNot(Objects::isNull);


    // Sink
    final ProducerSettings<String, String> kafkaProducerSettings =
        ProducerSettings
            .create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(kafkaBootstrapServers);
    final Sink<ProducerRecord<String, String>, CompletionStage<Done>> kafkaSink =
        Producer.<String, String>plainSink(kafkaProducerSettings);


    Sink<Path, NotUsed> sink = flow
        .map(e -> {
          out.println("Element to send: " + e.key());
          return e;
        })
        .to(kafkaSink);

    final RunnableGraph<NotUsed> runnableGraph = listDirSource.to(sink);
    final RunnableGraph<NotUsed> runnableGraph1 = changesSource.to(sink);

    runnableGraph.run(materializer);
    runnableGraph1.run(materializer);
  }




  private static void preStart(String kafkaBootstrapServers)
      throws ExecutionException, InterruptedException {

    final Properties adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

    final AdminClient adminClient = KafkaAdminClient.create(adminProperties);
    final Set<String> existingTopic = adminClient.listTopics().names().get();

    final Map<NewTopic, List<ConfigEntry>> requiredTopicsAndConfig = new HashMap<>();
    requiredTopicsAndConfig.put(
        new NewTopic(TOPIC, 1, (short) 1),
        Collections.emptyList());

    for (Map.Entry<NewTopic, List<ConfigEntry>> newTopicAndConfig : requiredTopicsAndConfig
        .entrySet()) {
      final NewTopic topic = newTopicAndConfig.getKey();
      if (!existingTopic.contains(topic.name())) {
        out.println("Topic " + topic.name() + " is been created.");
        topic.configs(newTopicAndConfig.getValue().stream().collect(
            Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
        final CreateTopicsResult result = adminClient
            .createTopics(Collections.singletonList(topic));
        result.all().get();
        out.println("Topic " + topic.name() + " created.");
      } else {
        Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs = new HashMap<>();
        final org.apache.kafka.clients.admin.Config config = new org.apache.kafka.clients.admin.Config(
            newTopicAndConfig.getValue());
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()), config);
        adminClient.alterConfigs(configs).all().get();
        out.println("Topic " + topic.name() + " is already created.");
      }
    }
  }

}