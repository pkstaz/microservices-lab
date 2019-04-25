package io.debezium.examples.ticketmsa.order;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.debezium.examples.ticketmsa.order.model.Order;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

@Path("/orders")
@ApplicationScoped
public class OrderService {

    @PersistenceContext
    private EntityManager entityManager;
    private final BlockingQueue<Order> queue = new ArrayBlockingQueue<>(1);

    @Outgoing("orders") 
    public KafkaMessage<Integer, String> deliver() {
        try {
            final Order order = queue.take();
            return KafkaMessage.of(order.getId(), order.toJson().toString());
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @POST
    @Transactional
    @Produces("application/json")
    @Consumes("application/json")
    public Order addOrder(Order order) {
        order = entityManager.merge(order);
        try {
            queue.put(order);
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
       return order;
    }
}