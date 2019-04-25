package io.debezium.examples.ticketmsa.order;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import io.debezium.examples.ticketmsa.order.model.Order;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Stream;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

@Path("/orders")
@ApplicationScoped
public class OrderService {

    @Inject
    @Stream("orders")
    private Emitter<KafkaMessage<Integer, String>> orderStream;

    @PersistenceContext
    private EntityManager entityManager;

    @POST
    @Transactional
    @Produces("application/json")
    @Consumes("application/json")
    public Order addOrder(Order order) {
        order = entityManager.merge(order);
        orderStream.send(KafkaMessage.of(order.getId(), order.toJson().toString()));
        return order;
    }
}
