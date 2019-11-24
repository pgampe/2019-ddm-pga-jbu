package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static final int CHUNK_SIZE = 512;

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> extends Output implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    public interface JsonSerializable {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesStreamMessage<T> implements JsonSerializable {
        private static final long serialVersionUID = 4057807743872319843L;
        private SourceRef<ByteString> byteStream;
        private ActorRef sender;
        private ActorRef receiver;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final Materializer materializer = Materializer.createMaterializer(this.getContext());

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(BytesStreamMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        // @see https://doc.akka.io/docs/akka/current/stream/stream-refs.html
        try {
            // Setup kryo, setup byte stream of message and attach it to the output
            Kryo kryo = new Kryo();
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            Output output = new Output(byteStream);
            kryo.writeClassAndObject(output, message.getMessage());
            // Close streams
            output.close();
            byteStream.close();

            // Setup the input stream
            ByteArrayInputStream inputStream = new ByteArrayInputStream(byteStream.toByteArray());
            Input input = new Input(inputStream);
            // Close streams
            input.close();
            inputStream.close();

            // Setup source
            Source<ByteString, CompletionStage<IOResult>> source = StreamConverters.fromInputStream(() -> input, CHUNK_SIZE);
            SourceRef<ByteString> sourceRef = source.runWith(StreamRefs.sourceRef(), materializer);

            // Finally send message
            System.out.println(sourceRef);
            BytesStreamMessage byteStreamMessage = new BytesStreamMessage(sourceRef, this.sender(), message.getReceiver());
            System.out.println(byteStreamMessage);
            receiverProxy.tell(byteStreamMessage, this.self());
        } catch (Exception e) {
            this.log().error(e.getMessage());
        }
    }

    private void handle(BytesStreamMessage message) {
        // Connect sink
        final CompletionStage<ByteString> byteStringCompletionStage;
        byteStringCompletionStage = (CompletionStage<ByteString>) message.getByteStream().getSource().runWith(
                Sink.fold(ByteString.empty(), ByteString::concat),
                materializer
        );
        // Once complete forward message for reconstruction
        byteStringCompletionStage.whenCompleteAsync((byteString, notUsed) -> reconstructObjectAndForward(byteString, message.getReceiver(), message.getSender()));
        this.log().info("LargeMessage streaming started");
    }

    private void reconstructObjectAndForward(ByteString byteString, ActorRef receiver, ActorRef sender) {
        this.log().info("LargeMessage streaming complete");
        try {
            // Setup kryo and connect streams
            Kryo kryo = new Kryo();
            ByteArrayInputStream bytesStream = new ByteArrayInputStream(byteString.toArray());
            Input input = new Input(bytesStream);
            // Close streams
            input.close();
            bytesStream.close();

            // Now forward the message
            receiver.tell(kryo.readClassAndObject(input), sender);
            this.log().info("LargeMessage sent successfully");
        } catch (IOException e) {
            this.log().error(e.getMessage());
        }
    }
}
