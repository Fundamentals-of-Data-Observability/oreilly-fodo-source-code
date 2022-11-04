package io.kensu.agent.airbyte.asm.instrumentation;

import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.*;
import static org.objectweb.asm.Opcodes.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.commons.Method;

public class AirbyteDefaultReplicationWorkerAdapter extends ClassVisitor {

    public String staticLambdaFunctionName;
    
    /**
     * getReplicationRunnable returns a Runnable created as a `lambda`: `() -> {}`
     * that implements the `run` method of Runnable
     * This lambda is turned into a static method of the owner class:
     * `DefaultReplicationWorker`
     * Because everything happens in this `run` method, we need to update this one,
     * and not getReplicationRunnable
     * We therefore need its name, which we collect while visiting
     * getReplicationRunnable, which itself call visitInvokeDynamicInsn
     * visitInvokeDynamicInsn has a bootstrap arguments which contains the Handler
     * (the callsite that must be executed) => this is what we need
     */

    public AirbyteDefaultReplicationWorkerAdapter(ClassVisitor cv) {
        super(ASM9, cv);
    }

    // @Override
    public MethodVisitor visitMethod​(int access, String name, String descriptor, String signature,
            String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, descriptor, signature, exceptions);
        if (name.equals("run") && descriptor.endsWith("ReplicationOutput;")) {
            mv = new MethodVisitors.MethodVisitorRun(descriptor, mv);
        } else if  (name.equals("getReplicationRunnable")) {
            mv = new MethodVisitors.MethodVisitorReturnRunnable(mv, this);
        } else if (name.equals(this.staticLambdaFunctionName)) {
            MethodVisitors.MethodVisitorRunnableRunLambda mvr = new MethodVisitors.MethodVisitorRunnableRunLambda(descriptor, mv);
            LocalVariablesSorter lvs = new LocalVariablesSorter(access, descriptor, mvr);
            mvr.lvs = lvs;
            mv = lvs;
        }
        return mv;
    }

    public static interface MethodVisitors {
        // add the instructions to call the AgentFactory with Fields of the DefaultReplicationWorker
        public static void generateGetAgentCallInDefaultReplicationWorker(MethodVisitor mv) {
            // LOAD fields
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "source", Constants.AirbyteSourceDescriptor);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "destination", Constants.AirbyteDestinationDescriptor);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "mapper", Constants.AirbyteMapperDescriptor);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "messageTracker", Constants.MessageTrackerDescriptor);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "recordSchemaValidator", Constants.RecordSchemaValidatorDescriptor);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, Constants.DefaultReplicationWorkerInternalName, "metricReporter", Constants.WorkerMetricReporterDescriptor);

            // Use the factory to get agent using the fields
            generateGetAgentCall(mv);
        }

        // add the instructions to call the AgentFactory with variable indices in the lambda builder
        public static void generateGetAgentCallInLambda(MethodVisitor mv, Integer sourceIndex, Integer destinationIndex, 
                                                        Integer mapperIndex, Integer messageTrackerIndex, 
                                                        Integer recordSchemaValidatorIndex, Integer metricReporterIndex) {
            // LOAD local vars
            mv.visitVarInsn(ALOAD, sourceIndex);
            mv.visitVarInsn(ALOAD, destinationIndex);
            mv.visitVarInsn(ALOAD, mapperIndex);
            mv.visitVarInsn(ALOAD, messageTrackerIndex);
            mv.visitVarInsn(ALOAD, recordSchemaValidatorIndex);
            mv.visitVarInsn(ALOAD, metricReporterIndex);

            // Use the factory to get agent using the vars
            generateGetAgentCall(mv);
        }
 
        public static void generateGetAgentCall(MethodVisitor mv) {
            // Use the factory to get agent using the fields
            mv.visitMethodInsn(INVOKESTATIC, Constants.KensuAgentFactoryInternalName, "getAgent",
                    "(" + Constants.AirbyteSourceDescriptor + Constants.AirbyteDestinationDescriptor + Constants.AirbyteMapperDescriptor + 
                        Constants.MessageTrackerDescriptor + Constants.RecordSchemaValidatorDescriptor + Constants.WorkerMetricReporterDescriptor
                    + ")" + Constants.KensuAgentDescriptor, false);
        }

        public static class MethodVisitorRun extends MethodVisitor {

            public int argOfTypeStandardSyncInputIndex = -1;

            public MethodVisitorRun(String desc, MethodVisitor mv) {
                super(ASM9, mv);
                Method thisMethod = new Method("whocares_0", desc);
                int index = 1; // 0 is this
                for (Type at : thisMethod.getArgumentTypes()) {
                    String atDesc = at.getDescriptor();
                    if (atDesc.equals(Constants.StandardSyncInputDescriptor)) {
                        // Retain source index on the stack for future usage
                        this.argOfTypeStandardSyncInputIndex = index;
                    }
                    index++;
                }
            }

            public void addCode() {
                // Get the agent
                generateGetAgentCallInDefaultReplicationWorker(mv);
                // visit StandardSyncInput
                mv.visitVarInsn(ALOAD, argOfTypeStandardSyncInputIndex);
                // call init with StandardSyncInput
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "init",
                        "("+Constants.StandardSyncInputDescriptor+")V", false);
            }

            @Override
            public void visitCode() {
                mv.visitCode();
                addCode();
            }

            @Override
            public void visitMaxs(int maxStack, int maxLocals) {
                // we don't compute them, because we let the class writer do it
                mv.visitMaxs(maxStack, maxLocals);
            }
        }

        public static class MethodVisitorReturnRunnable extends MethodVisitor {
            public AirbyteDefaultReplicationWorkerAdapter ca;

            public MethodVisitorReturnRunnable(MethodVisitor mv, AirbyteDefaultReplicationWorkerAdapter ca) {
                super(ASM9, mv);
                this.ca = ca;
            }

            @Override
            public void visitInvokeDynamicInsn​(String name, String descriptor, Handle bootstrapMethodHandle,
                    Object... bootstrapMethodArguments) {
                Object[] args = bootstrapMethodArguments;
                if (name.equals("run") && descriptor.endsWith("Ljava/lang/Runnable;")) {
                    // get the handle to the code that must be executed => the Runnable.run lambda
                    String staticLambdaFunctionName = Stream.of(args)
                            .filter(a -> a instanceof Handle
                                    && ((Handle) a).getOwner().equals(Constants.DefaultReplicationWorkerInternalName))
                            .map(a -> ((Handle) a).getName())
                            .findFirst()
                            .orElse(null);
                    if (staticLambdaFunctionName != null) {
                        this.ca.staticLambdaFunctionName = staticLambdaFunctionName;
                    }
                }
                this.mv.visitInvokeDynamicInsn​(name, descriptor, bootstrapMethodHandle, bootstrapMethodArguments);
            }
        }

        // TODO FIXME => handle cases where `attemptRead`, `mapMessage`, and `accept` are throwing exceptions
        public static class MethodVisitorRunnableRunLambda extends MethodVisitor /* to add local variables */ {
            public Map<String, Integer> argIndexMap = new HashMap<>(Map.ofEntries(
                Map.entry(Constants.AirbyteSourceDescriptor, -1),
                Map.entry(Constants.AirbyteDestinationDescriptor, -1),
                Map.entry(Constants.AirbyteMapperDescriptor, -1),
                Map.entry(Constants.MessageTrackerDescriptor, -1),
                Map.entry(Constants.RecordSchemaValidatorDescriptor, -1),
                Map.entry(Constants.WorkerMetricReporterDescriptor, -1)
            ));

            private boolean justVisitedAttemptRead = false;
            private boolean justVisitedMapMessage = false;
            private int indexOfMappedMessage = -1;

            public LocalVariablesSorter lvs;

            public MethodVisitorRunnableRunLambda(String desc, MethodVisitor mv) {
                super(ASM9, mv);
                Method thisMethod = new Method("whocares_1", desc);
                int index = 0;
                for (Type at : thisMethod.getArgumentTypes()) {
                    String atDesc = at.getDescriptor();
                    this.argIndexMap.replace(atDesc, -1, index);
                    index++;
                }
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                // TODO use owner and descriptor to ensure... this is the right method on the right owner
                if (name.equals("attemptRead")) {
                    justVisitedAttemptRead = true;
                } else if (name.equals("mapMessage")) {
                    justVisitedMapMessage = true;
                } else if (name.equals("accept")) {
                    // We add the code here because the return of accept is not stored... 
                    // So we need to load the message from the stack, this is why we kept its index after the mapping
                    addCodeAfterAccept();
                } else if (name.equals("notifyEndOfInput")) { // on destination... finalizing the copy
                    addCodeAfterNotifyEndOfInput();
                }
            }

            public void generateGetAgent() {
                // Call Get Agent
                generateGetAgentCallInLambda(mv, 
                    this.argIndexMap.get(Constants.AirbyteSourceDescriptor),
                    this.argIndexMap.get(Constants.AirbyteDestinationDescriptor), 
                    this.argIndexMap.get(Constants.AirbyteMapperDescriptor),
                    this.argIndexMap.get(Constants.MessageTrackerDescriptor), 
                    this.argIndexMap.get(Constants.RecordSchemaValidatorDescriptor),
                    this.argIndexMap.get(Constants.WorkerMetricReporterDescriptor));
            }

            public void addCodeAfterAttemptRead(int varIndex) {
                // Load agent
                generateGetAgent();

                // load the message back on
                mv.visitVarInsn(ALOAD, varIndex);

                // call handleMessageRead on the agent with the (Optional) message
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "handleMessageRead", 
                                    "("+"Ljava/util/Optional;"+")V", false);
            }

            public void addCodeAfterMapMessage(int varIndex) {
                // Load agent
                generateGetAgent();

                // load the message back on
                mv.visitVarInsn(ALOAD, varIndex);

                // We need this index to handle the `accept` where there is no result stored
                indexOfMappedMessage = varIndex;

                // call handleMessageMapped on the agent with the mapped message
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "handleMessageMapped", 
                                    "("+Constants.AirbyteMessageDescriptor+")V", false);
            }

            public void addCodeAfterAccept() {
                // Load agent
                generateGetAgent();

                // load the mapped message back on
                mv.visitVarInsn(ALOAD, this.indexOfMappedMessage);

                // call handleMessageMapped on the agent with the mapped message
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "handleMessageCopied", 
                                    "("+Constants.AirbyteMessageDescriptor+")V", false);
            }

            public void addCodeAfterNotifyEndOfInput() {
                // Load agent
                generateGetAgent();

                // call handleMessageMapped on the agent with the mapped message
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "finishCopy", 
                                    "()V", false);
            }

            @Override
            public void visitVarInsn(int opcode, int varIndex) {
                super.visitVarInsn(opcode, varIndex);
                if (justVisitedAttemptRead && opcode == ASTORE) {
                    // we're storing the message in the local variable as a next operation after executing the attemptRead
                    // we reset the flag to avoid potential duplicates
                    justVisitedAttemptRead = false;
                    addCodeAfterAttemptRead(varIndex);
                } else if (justVisitedMapMessage && opcode == ASTORE) {
                    // we're storing the message after it has been mapped
                    // we reset the flag to avoid potential duplicates
                    justVisitedMapMessage = false;
                    addCodeAfterMapMessage(varIndex);
                }
            }

            @Override
            public void visitMaxs(int maxStack, int maxLocals) {
                // we don't compute them, because we let the class writer do it
                mv.visitMaxs(maxStack, maxLocals);
            }
        }
    }
}