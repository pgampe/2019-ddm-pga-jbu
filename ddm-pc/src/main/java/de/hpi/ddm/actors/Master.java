package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    public static class RequestLineMessage implements Serializable {
        private static final long serialVersionUID = 4791804711649009868L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FoundPassword implements Serializable {
        private static final long serialVersionUID = 4791804711649009868L;
        private String entry_number;
        private String password;
    }


    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

    private Stack<String[]> lines = new Stack<>();
    private boolean ready_for_termination = false;
    private List<String> lines_in_flight = new LinkedList<>();

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(RequestLineMessage.class, this::handle)
                .match(FoundPassword.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
    }

    protected void handle(BatchMessage message) {

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        // The input file is read in batches for two reasons: /////////////////////////////////////////////////
        // 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
        // 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
        // TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        if (message.getLines().isEmpty()) {
            this.ready_for_termination = true;
            return;
        }

        this.lines.addAll(message.getLines());
        for (String[] line : message.getLines()) {
            this.lines_in_flight.add(line[0]);
        }
        // Give workers a hint, this new items are ready
        for (ActorRef worker : this.workers) {
            worker.tell(new Worker.StartMessage(), this.self());
        }
        //System.out.println("Processed batch of size " + message.getLines().size());
    }

    protected void handle(RequestLineMessage requestLineMessage) {
        //System.out.println("RequestLine message");
        if (this.ready_for_termination) {
            //System.out.println("Ready for termination with lines open " + this.lines_in_flight.size());
            if (lines_in_flight.size() == 0) {
                this.collector.tell(new Collector.PrintMessage(), this.self());
                this.terminate();
            }
        }
        if (this.lines.size() > 0) {
            //System.out.println("new line request, sending now with lines in buffer " + this.lines.size());
            this.sender().tell(new Worker.ProcessLineMessage(this.lines.pop()), this.self());
        } else {
            if (!this.ready_for_termination) {
                //System.out.println("Requesting new data");
                this.reader.tell(new Reader.ReadMessage(), this.self());
            }
        }
    }

    public void handle(FoundPassword foundPassword) {
        this.lines_in_flight.remove(foundPassword.entry_number);
        this.collector.tell(new Collector.CollectMessage(
                "Found password: " + foundPassword.password + " for entry " + foundPassword.entry_number
        ), this.self());
    }


    protected void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.log().info("Registered {}", this.sender());
        // Make sure the workers actually do something
        this.sender().tell(new Worker.StartMessage(), this.self());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }
}
