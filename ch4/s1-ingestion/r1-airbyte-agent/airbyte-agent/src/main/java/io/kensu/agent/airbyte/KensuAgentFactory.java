package io.kensu.agent.airbyte;

import java.util.concurrent.ConcurrentHashMap;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.MessageTracker;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;

import io.kensu.dam.*;

public class KensuAgentFactory {

    private static KensuAgentFactory INSTANCE = new KensuAgentFactory();
    private ConcurrentHashMap<AgentKey, KensuAgent> agents = new ConcurrentHashMap<>();

    public KensuAgentFactory() {
    }

    // Static methods
    public static KensuAgentFactory getInstance() {
        return INSTANCE;
    }
    
    public static void initAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
                                        MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
                                        WorkerMetricReporter metricReporter) {
        getAgent(source, destination, mapper, messageTracker, recordSchemaValidator, metricReporter);
    }
  
    public static KensuAgent getAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
                                        MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
                                        WorkerMetricReporter metricReporter) {
        return getInstance().getOrCreateAgent(source, destination, mapper, messageTracker, recordSchemaValidator, metricReporter);
    }
  
    public static void terminate(KensuAgent kensuAgent) {
        AgentKey key = new AgentKey(kensuAgent.source, kensuAgent.destination, kensuAgent.mapper, kensuAgent.messageTracker, 
                                    kensuAgent.recordSchemaValidator, kensuAgent.metricReporter);
        getInstance().terminateAgent(key);
    }

    // Object methods
    public KensuAgent createAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
                                    MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
                                    WorkerMetricReporter metricReporter) {
        return new KensuAgent(source, destination, mapper, messageTracker, recordSchemaValidator, metricReporter);
    }

    public KensuAgent getOrCreateAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
                                        MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
                                        WorkerMetricReporter metricReporter) {
        AgentKey key = new AgentKey(source, destination, mapper, messageTracker, recordSchemaValidator, metricReporter);
        return agents.computeIfAbsent(key, k -> createAgent(k.source, k.destination, k.mapper, k.messageTracker, k.recordSchemaValidator, k.metricReporter));
    }

    private void terminateAgent(AgentKey key) {
        this.agents.remove(key);
    }

    // Internal Key to uniquely identify an agent in the cache
    public static class AgentKey {
        public AirbyteSource source;
        public AirbyteDestination destination;
        public AirbyteMapper mapper;
        public MessageTracker messageTracker;
        public RecordSchemaValidator recordSchemaValidator;
        public WorkerMetricReporter metricReporter;

        public AgentKey(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
                MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
                WorkerMetricReporter metricReporter) {
            this.source = source;
            this.destination = destination;
            this.mapper = mapper;
            this.messageTracker = messageTracker;
            this.recordSchemaValidator = recordSchemaValidator;
            this.metricReporter = metricReporter;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((destination == null) ? 0 : destination.hashCode());
            result = prime * result + ((mapper == null) ? 0 : mapper.hashCode());
            result = prime * result + ((messageTracker == null) ? 0 : messageTracker.hashCode());
            result = prime * result + ((metricReporter == null) ? 0 : metricReporter.hashCode());
            result = prime * result + ((recordSchemaValidator == null) ? 0 : recordSchemaValidator.hashCode());
            result = prime * result + ((source == null) ? 0 : source.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AgentKey other = (AgentKey) obj;
            if (destination == null) {
                if (other.destination != null)
                    return false;
            } else if (!destination.equals(other.destination))
                return false;
            if (mapper == null) {
                if (other.mapper != null)
                    return false;
            } else if (!mapper.equals(other.mapper))
                return false;
            if (messageTracker == null) {
                if (other.messageTracker != null)
                    return false;
            } else if (!messageTracker.equals(other.messageTracker))
                return false;
            if (metricReporter == null) {
                if (other.metricReporter != null)
                    return false;
            } else if (!metricReporter.equals(other.metricReporter))
                return false;
            if (recordSchemaValidator == null) {
                if (other.recordSchemaValidator != null)
                    return false;
            } else if (!recordSchemaValidator.equals(other.recordSchemaValidator))
                return false;
            if (source == null) {
                if (other.source != null)
                    return false;
            } else if (!source.equals(other.source))
                return false;
            return true;
        }

        

        
    }
}
