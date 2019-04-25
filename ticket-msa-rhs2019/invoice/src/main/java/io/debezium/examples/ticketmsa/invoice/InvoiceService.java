package io.debezium.examples.ticketmsa.invoice;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class InvoiceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvoiceService.class);

    @Incoming("orders")
    public void orderArrived(final String order) {
        LOGGER.info("Order event '{}' arrived", order);
    }
}
