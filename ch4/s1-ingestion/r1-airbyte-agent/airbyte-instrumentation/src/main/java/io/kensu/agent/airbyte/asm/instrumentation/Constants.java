package io.kensu.agent.airbyte.asm.instrumentation;

public class Constants {
    public static String StandardSyncInputInternalName = "io/airbyte/config/StandardSyncInput";
    public static String StandardSyncInputDescriptor = "Lio/airbyte/config/StandardSyncInput;";

    public static String AirbyteMessageInternalName = "io/airbyte/protocol/models/AirbyteMessage";
    public static String AirbyteMessageDescriptor = "Lio/airbyte/protocol/models/AirbyteMessage;";

    public static String RecordSchemaValidatorInternalName = "io/airbyte/workers/RecordSchemaValidator";
    public static String RecordSchemaValidatorDescriptor = "Lio/airbyte/workers/RecordSchemaValidator;";
    public static String WorkerMetricReporterInternalName = "io/airbyte/workers/WorkerMetricReporter";
    public static String WorkerMetricReporterDescriptor = "Lio/airbyte/workers/WorkerMetricReporter;";

    public static String DefaultReplicationWorkerInternalName = "io/airbyte/workers/general/DefaultReplicationWorker";
    public static String DefaultReplicationWorkerDescriptor = "Lio/airbyte/workers/general/DefaultReplicationWorker;";

    public static String AirbyteSourceInternalName = "io/airbyte/workers/internal/AirbyteSource";
    public static String AirbyteSourceDescriptor = "Lio/airbyte/workers/internal/AirbyteSource;";
    public static String AirbyteDestinationInternalName = "io/airbyte/workers/internal/AirbyteDestination";
    public static String AirbyteDestinationDescriptor = "Lio/airbyte/workers/internal/AirbyteDestination;";
    public static String AirbyteMapperInternalName = "io/airbyte/workers/internal/AirbyteMapper";
    public static String AirbyteMapperDescriptor = "Lio/airbyte/workers/internal/AirbyteMapper;";    
    public static String MessageTrackerInternalName = "io/airbyte/workers/internal/MessageTracker";
    public static String MessageTrackerDescriptor = "Lio/airbyte/workers/internal/MessageTracker;";

    public static String KensuAgentInternalName = "io/kensu/agent/airbyte/KensuAgent";
    public static String KensuAgentDescriptor = "Lio/kensu/agent/airbyte/KensuAgent;";
    public static String KensuAgentFactoryInternalName = "io/kensu/agent/airbyte/KensuAgentFactory";
    public static String KensuAgentFactoryDescriptor = "Lio/kensu/agent/airbyte/KensuAgentFactory;";
}
