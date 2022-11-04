package io.kensu.agent.airbyte;

import io.airbyte.config.StandardSyncInput;

public interface KensuProcessor {
    public boolean matches(String imageName);
    public void process(StandardSyncInput syncInput, KensuAgent agent);
}
