package org.felp;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.reactive.messaging.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

@ApplicationScoped
public class MyReactiveMessagingApplication {

    @Inject
    @Channel("words-out")
    @OnOverflow(value = OnOverflow.Strategy.UNBOUNDED_BUFFER)
    Emitter<String> emitterWords;
    
    @Inject
    @Channel("uppercase")
    Emitter<String> emitterUperCase;

    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application.
     * Messages are sent to the broker.
     **/
    void onStart(@Observes StartupEvent ev){
        Lorem lorem = LoremIpsum.getInstance();

        for (int i = 0; i < 10000; i++)
        {
            emitterWords.send(lorem.getWords(20,50));
            
            try {
                Thread.sleep(4);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     **/
    @Incoming("words-in")
    public CompletionStage<Void>  toUpperCase(Message<List<String>> message) {

        System.out.println(LocalDateTime.now().toString() + " " +  message.getPayload().size());
        for(var item :message.getPayload()) 
        {
            emitterUperCase.send(item.toUpperCase());
        }
        return message.ack();
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        // System.out.println(">> " + word);
    }
}
