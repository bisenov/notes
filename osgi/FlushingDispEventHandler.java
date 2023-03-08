package com.mysite.core.dispatcher;

import com.day.cq.replication.ReplicationAction;
import com.day.cq.replication.ReplicationActionType;
import com.day.cq.replication.ReplicationException;
import org.apache.sling.api.resource.ResourceResolver;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FlushingDispEventHandler implements EventHandler {

    private ResourceResolver resourceResolver;
    private DispatcherFlusher dispatcherFlusher;

    private final Logger log = LoggerFactory.getLogger(FlushingDispEventHandler.class);

    private final String[] flushedPaths;

    public FlushingDispEventHandler(String[] flushedPath,
                                    ResourceResolver resourceResolver,
                                    DispatcherFlusher dispatcherFlusher) {
        this.flushedPaths = flushedPath;
        this.resourceResolver = resourceResolver;
        this.dispatcherFlusher = dispatcherFlusher;
    }

    @Override
    public void handleEvent(Event event) {
        ReplicationAction action = ReplicationAction.fromEvent(event);

        log.debug("Requesting dispatcher flush of associated path: {}", action.getPath());
        try {
            dispatcherFlusher.flush(resourceResolver, ReplicationActionType.ACTIVATE, false, action.getPath());
        } catch (ReplicationException e) {
            // ReplicationException must be caught here, as otherwise this will prevent the replication at all
            log.error("Error issuing dispatcher flush rules, some downstream replication exception occurred: {}",
                      e.getMessage(), e);
        }
    }
}
