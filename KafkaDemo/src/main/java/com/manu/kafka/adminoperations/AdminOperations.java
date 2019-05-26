//package com.manu.kafka.adminoperations;
//
//import org.apache.kafka.clients.admin.*;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.errors.TopicExistsException;
//
//import java.util.*;
//import java.util.concurrent.ExecutionException;
//
//
//public class AdminOperations {
//
//    public static void main(String[] args) {
//        AdminOperations adminOperations = new AdminOperations();
//        Set<String> topics = new HashSet<>();
//        topics.add("topic1");
//        topics.add("topic2");
//        adminOperations.createTopic(topics, 3);
//        adminOperations.listTopics();
//        adminOperations.describeTopic(topics);
//        adminOperations.deleteTopic(topics);
//        // after delete list topics
//        adminOperations.listTopics();
//
//
//    }
//
//    // create topic
//    public void createTopic(Set<String> topics, final int partitions) {
//        final short replicationFactor = 1;
//
//        // Create admin client instance
//        try (final AdminClient adminClient = KafkaAdminClient.create(loadDefaultClientConfigs())) {
//            for (String topicName : topics) {
//                try {
//
//                    // create new topic
//                    final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
//
//                    // Create topic (async call)
//                    final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
//
//                    // Since the call is Async, Lets wait for it to complete.
//                    createTopicsResult.values().get(topicName).get();
//                    System.out.println(createTopicsResult.values());
//                } catch (InterruptedException | ExecutionException | TopicExistsException e) {
//                    // TopicExistsException - Swallow this exception, just means the topic already exists.
//                    if (!(e.getCause() instanceof TopicExistsException)) {
//                        throw new RuntimeException(e.getMessage(), e);
//                    }
//
//                }
//
//            }
//        }
//    }
//
//    // list topics
//    public void listTopics() {
//        ListTopicsOptions options = new ListTopicsOptions();
//        options.listInternal(true);
//        // Create admin client
//        try (final AdminClient adminClient = KafkaAdminClient.create(loadDefaultClientConfigs())) {
//            ListTopicsResult result = adminClient.listTopics(options);
//            System.out.println("TOPICS: " + result.names().get());
//
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//    }
//
//    // describe topic
//    public void describeTopic(Set<String> topics) {
//        try (final AdminClient adminClient = KafkaAdminClient.create(loadDefaultClientConfigs())) {
//            Map<String, KafkaFuture<TopicDescription>> result = adminClient.describeTopics(topics).values();
//            System.out.println(adminClient.describeTopics(topics).all().get());
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//
//    }
//
//    // delete topic
//    public void deleteTopic(Set<String> topics) {
//        try (final AdminClient adminClient = KafkaAdminClient.create(loadDefaultClientConfigs())) {
//            System.out.println(adminClient.deleteTopics(topics).all().get());
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//
//    }
//
//    /**
//     * loads configs
//     * @return
//     */
//    private Map<String, Object> loadDefaultClientConfigs() {
//        Map<String, Object> defaultClientConfig = new HashMap<String, Object>();
//        defaultClientConfig.put("bootstrap.servers", "localhost:9092");
//        defaultClientConfig.put("client.id", "test-consumer-id");
//        return defaultClientConfig;
//    }
//
//
//
//}
