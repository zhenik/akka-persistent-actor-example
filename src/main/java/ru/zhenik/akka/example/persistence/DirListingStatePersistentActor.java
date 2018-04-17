package ru.zhenik.akka.example.persistence;

import akka.persistence.AbstractPersistentActor;
import akka.persistence.SnapshotOffer;
import java.io.Serializable;
import java.util.ArrayList;


class Cmd implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String data;

  public Cmd(String data) { this.data = data; }
  public String getData() { return data; }
}

class Evt implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String data;

  public Evt(String data) { this.data = data; }
  public String getData() { return data; }
}

class StateExample implements Serializable {
  private static final long serialVersionUID = 1L;
  private final ArrayList<String> events;

  public StateExample() { this(new ArrayList<>()); }
  public StateExample(ArrayList<String> events) { this.events = events; }

  public StateExample copy() { return new StateExample(new ArrayList<>(events)); }
  public void update(Evt evt) { events.add(evt.getData()); }
  public int size() { return events.size(); }

  @Override
  public String toString() { return events.toString(); }
}

/**
 * State-full Actor,
 * state is stored on locally on disk
 * */
public class DirListingStatePersistentActor extends AbstractPersistentActor {

  private StateExample stateExample = new StateExample();
  private int snapShotInterval = 1000;

  public int getNumEvents() { return stateExample.size(); }
  @Override
  public String persistenceId() { return "sample-id-1"; }


  @Override
  public Receive createReceiveRecover() {
    return receiveBuilder()
        .match(Evt.class, stateExample::update)
        .match(SnapshotOffer.class, ss -> stateExample = (StateExample) ss.snapshot())
        .build();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Cmd.class, c -> {
          final String data = c.getData();
//          final Evt evt = new Evt(data + "-" + getNumEvents());
          final Evt evt = new Evt(data);
          persist(evt, (Evt e) -> {
            stateExample.update(e);
            getContext().getSystem().eventStream().publish(e);
            if (lastSequenceNr() % snapShotInterval == 0 && lastSequenceNr() != 0)
              // IMPORTANT: create a copy of snapshot because ExampleState is mutable
              saveSnapshot(stateExample.copy());
          });
        })
        .matchEquals("print", s -> System.out.println(stateExample))
        .build();
  }


}