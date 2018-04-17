package ru.zhenik.akka.example.persistence;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;


public class StreamsPlayApp {

  public static void main(String[] args) {
    // Akka
    final ActorSystem system = ActorSystem.create("play-for-fun-with-akka");
    final Materializer materializer = ActorMaterializer.create(system);


    // Produce some state
    final ActorRef persistentActor = system.actorOf(Props.create(DirListingStatePersistentActor.class), "persistentActor-2-java8");
//    persistentActor.tell(new Cmd("filename/dasd"), null);
//        persistentActor.tell(new Cmd("baz"), null);
//        persistentActor.tell(new Cmd("bar"), null);
//        persistentActor.tell("snap", null);
//        persistentActor.tell(new Cmd("buzz"), null);


    // Query all persisted events
    LeveldbReadJournal queries =
        PersistenceQuery
            .get(system).getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());

    Source<String, NotUsed> processedFiles = queries
        .currentPersistenceIds()
        .flatMapConcat(eachPersistentId -> queries.currentEventsByPersistenceId(eachPersistentId, 0, Long.MAX_VALUE))
        .map(eventEnvelope -> ((Evt)eventEnvelope.event()).getData());

    processedFiles.runForeach(System.out::println, materializer);

  }
}
