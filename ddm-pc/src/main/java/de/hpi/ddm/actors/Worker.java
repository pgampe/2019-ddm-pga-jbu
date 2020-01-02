package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = 3658961703483581871L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessLineMessage implements Serializable {
        private static final long serialVersionUID = -5552548416077950569L;
        private String[] line;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(StartMessage.class, this::handle)
                .match(ProcessLineMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(StartMessage startMessage) {
        System.out.println("worker startmessage");
        this.sender().tell(new Master.RequestLineMessage(), this.self());
    }

    private void handle(ProcessLineMessage processLineMessage) {
        String[] line = processLineMessage.line;
        String chars = line[2];
        char[] chars_array = chars.toCharArray();
        List<Character> chars_in_password = new ArrayList<>();
        for (char c : chars_array) {
            chars_in_password.add(c);
        }
        int password_length = Integer.parseInt(line[3]);
        String password_hash = line[4];

        System.out.println("Processing line " + line[0]);
        System.out.println("Password: " + password_hash);

        Set<String> hints = new HashSet<>(Arrays.asList(line).subList(5, line.length));

        // Lets crack the hints
        for (int i = 0; i < chars_array.length; i++) {
            System.out.println("Trying permutations without char " + chars_array[i]);
            char[] current_chars = ArrayUtils.remove(chars_array, i);
            if (this.heapPermutation(current_chars, current_chars.length, hints)) {
                System.out.println("Char " + chars_array[i] + " is not in password");
                chars_in_password.remove((Character) chars_array[i]);
            }
        }
        System.out.println("Chars in password: " + chars_in_password);

        String password = crackPassword(
                chars_in_password,
                "",
                chars_in_password.size(),
                password_length,
                password_hash
        );
        System.out.println("The password is " + password);

        // Send out the found password
        this.sender().tell(new Master.FoundPassword(line[0], password), this.self());
        // Since we are done, request more work
        this.sender().tell(new Master.RequestLineMessage(), this.self());
    }


    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    // Slightly optimized by IDEA hints
    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuffer = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    // Adopted for current use case
    private boolean heapPermutation(char[] a, int size, Set<String> hints) {
        // If size is 1, store the obtained permutation
        if (size == 1) {
            //l.add(new String(a));
            String hint = new String(a);
            String hash = hash(hint);
            if (hints.contains(hash)) {
                // No need to check the same has again
                hints.remove(hash);
                System.out.println("Found hint " + hint);
                return true;
            }
            return false;
        }

        for (int i = 0; i < size; i++) {
            if (heapPermutation(a, size - 1, hints)) {
                // Found a hint already, abort early to save CPU time
                return true;
            }

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
        return false;
    }

    // The main recursive method
    // to print all possible
    // strings of length k
    // https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
    // Adopted for current use case
    private String crackPassword(List<Character> set, String prefix, int n, int k, String hash) {

        // Base case: k is 0,
        // print prefix
        if (k == 0) {
            if (hash(prefix).equals(hash)) {
                return prefix;
            }
            return "";
        }

        // One by one add all characters
        // from set and recursively
        // call for k equals to k-1
        for (int i = 0; i < n; ++i) {

            // Next character of input added
            String newPrefix = prefix + set.get(i);

            // k is decreased, because
            // we have added a new character
            String result = crackPassword(set, newPrefix, n, k - 1, hash);
            if (!result.equals("")) {
                // abort because we already found the password
                return result;
            }
        }
        return "";
    }
}