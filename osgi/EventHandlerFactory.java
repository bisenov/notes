package com.mysite.core.dispatcher;

import com.day.cq.replication.ReplicationAction;
import com.day.cq.replication.ReplicationException;
import com.day.cq.replication.Replicator;
import com.mysite.core.dispatcher.EventHandlerFactory.Configuration;
import com.mysite.core.util.ParameterUtil;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.component.propertytypes.ServiceDescription;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component(
    immediate = true,
//    service = SomeInterface.class,
    configurationPolicy = ConfigurationPolicy.OPTIONAL
    )
@ServiceDescription("Dispatcher flush rules")
@Designate(ocd = Configuration.class, factory = true)
public class EventHandlerFactory {

    @ObjectClassDefinition()
    public @interface Configuration {

        @AttributeDefinition(
            name = "Flush Rules (ResourceOnly)",
            description = "LDAP-style filter syntax pattern to Path associations for flush rules. "
                + "Format: <pattern-of-trigger-content>=<path-to-flush>"
                + "Example: /content/mysite/triggeredcontent/*=/content/mysite/flushedcontent/somepage"
                + "& as a separator for multiple flushed paths.",
            cardinality = Integer.MAX_VALUE)
        String[] prop_rules_resourceOnly();
    }

    @Reference
    Replicator replicator;

//    @Reference(
//        target = "(&(event.topics=com/day/cq/replication)(specProp=fac))",
//        cardinality = ReferenceCardinality.MULTIPLE,
//        policy = ReferencePolicy.DYNAMIC)
//    volatile List<EventHandler> handlerList;

    private final UUID uuid = UUID.randomUUID();

    private final Logger LOGGER = LoggerFactory.getLogger(EventHandlerFactory.class);

    Map<String, String[]> flushRules = new LinkedHashMap<>();
    Configuration eventHandlerFactoryConfig;

    final private String UUID_FIELD = "UUID";

    final private String SERVICE_FILTER_FORMAT = "(&(event.topics=%1$s)(&(%2$s=%3$s)))";

    final private String SERVICE_FILTER = String.format(SERVICE_FILTER_FORMAT, ReplicationAction.EVENT_TOPIC, UUID_FIELD, uuid);

    List<ServiceRegistration<?>> serviceRegistrations = new ArrayList<>();

    @Activate
    protected final void activate(final Configuration eventHandlerFactoryConfig)
        throws InvalidSyntaxException {
        this.eventHandlerFactoryConfig = eventHandlerFactoryConfig;
        this.flushRules = this.configureFlushRules(
            ParameterUtil.toMap(eventHandlerFactoryConfig.prop_rules_resourceOnly(), "="));

//        EventHandler eventHandler = new MyEventHandler();

        flushRules.forEach((k, v) -> serviceRegistrations.add(registerHandler(k, v)));
//
//        Hashtable<String, String> ht = new Hashtable<>();
//        ht.put(EventConstants.EVENT_TOPIC, ReplicationAction.EVENT_TOPIC);
//        ht.put(EventConstants.EVENT_FILTER, "(paths=/content/*)");
//        ht.put(UUID_FIELD, uuid.toString());

//        serviceRegistrations.add(registerHandler());
        LOGGER.debug("All registered services: {}",
                     (Object[]) getBundleContext().getAllServiceReferences(EventHandler.class.getName(), SERVICE_FILTER));

    }

    private ServiceRegistration<?> registerHandler(String triggeredPaths, String[] flushedPaths) {
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put(EventConstants.EVENT_TOPIC, ReplicationAction.EVENT_TOPIC);
        properties.put(EventConstants.EVENT_FILTER, "(paths=" + triggeredPaths +")");
        properties.put(UUID_FIELD, uuid.toString());

        return getBundleContext().registerService(
            EventHandler.class.getName(),
            new MyEventHandler(flushedPaths),
            properties);

    }

    @Deactivate
    protected final void deactivate() {
        serviceRegistrations.forEach(ServiceRegistration::unregister);
        LOGGER.info("smth happened here");
    }

    /**
     * Create Pattern-based flush rules map.
     *
     * @param configuredRules String based flush rules from OSGi configuration
     * @return returns the configures flush rules
     */
    protected final Map<String, String[]> configureFlushRules(final Map<String, String> configuredRules) {
        final Map<String, String[]> rules = new LinkedHashMap<>();

        for (final Map.Entry<String, String> entry : configuredRules.entrySet()) {
//            final Pattern pattern = Pattern.compile();
            rules.put(entry.getKey().trim(), entry.getValue().trim().split("&"));
        }

        return rules;
    }

    BundleContext getBundleContext() {
        return FrameworkUtil.getBundle(this.getClass()).getBundleContext();
    }

    private class MyEventHandler implements EventHandler {
        private final Logger LOGGER = LoggerFactory.getLogger(MyEventHandler.class);

        private final String[] flushedPaths;

        @Reference
        private DispatcherFlusher dispatcherFlusher;
        @Reference
        private ResourceResolverFactory resourceResolverFactory;

        public MyEventHandler(String[] flushedPath) {
            this.flushedPaths = flushedPath;
        }

        @Override
        public void handleEvent(Event event) {
            ReplicationAction action = ReplicationAction.fromEvent(event);
            LOGGER.info("There is event {}", event);
            protected static final Map<String, Object> AUTH_INFO;
            private static final String SERVICE_NAME = "dispatcher-flush";

            static {
                AUTH_INFO = Collections.singletonMap(ResourceResolverFactory.SUBSERVICE, (Object) SERVICE_NAME);
            }

            try (ResourceResolver resourceResolver = resourceResolverFactory.getServiceResourceResolver(AUTH_INFO)){


                // Flush explicit resources using the CQ-Action-Scope ResourceOnly header
                for (final Map.Entry<Pattern, String[]> entry : this.resourceOnlyFlushRules.entrySet()) {
                    final Pattern pattern = entry.getKey();
                    final Matcher m = pattern.matcher(path);

                    if (m.matches()) {
                        for (final String value : entry.getValue()) {
                            final String flushPath = m.replaceAll(value);

                            log.debug("Requesting ResourceOnly flush of associated path: {} ~> {}", path, entry.getValue());
                            dispatcherFlusher.flush(resourceResolver, flushActionType, false,
                                                    RESOURCE_ONLY_FILTER,
                                                    flushPath);
                        }
                    }
                }

            } catch (ReplicationException e) {
                // ReplicationException must be caught here, as otherwise this will prevent the replication at all
                log.error("Error issuing dispatcher flush rules, some downstream replication exception occurred: {}", e.getMessage(), e);
            } catch (LoginException e) {
                log.error("Error issuing dispatcher flush rules due to a repository login exception: {}", e.getMessage(), e);
            }
        }
    }
}

