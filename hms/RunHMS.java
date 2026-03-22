package dewberry;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;

public class RunHMS {
    private static final Logger logger = Logger.getLogger(RunHMS.class.getName());

    static {
        // Configure logger with timestamp format
        logger.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        logger.addHandler(handler);
        logger.setLevel(java.util.logging.Level.INFO);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            logger.severe("Error: You must provide both `hmsFilePath` and `simulationName` as command-line arguments.");
            System.exit(1);
        }

        String hmsFilePath = args[0];
        String simulationName = args[1];

        logger.info("Starting HMS simulation: model='" + hmsFilePath + "', simulation='" + simulationName + "'");

        try {
            // Try multiple API versions in order of preference

            // Try 4.14+ API first (static factory method)
            int result = tryStaticOpen(hmsFilePath, simulationName);
            if (result == 0) {
                // Success
                System.exit(0);
            } else if (result == 1) {
                // Errors detected in simulation
                System.exit(1);
            }
            // result == 2 means API not available, try next

            // Fall back to 4.12-4.13 API (constructor pattern)
            result = tryConstructorOpen(hmsFilePath, simulationName);
            if (result == 0) {
                // Success
                System.exit(0);
            } else if (result == 1) {
                // Errors detected in simulation
                System.exit(1);
            }
            // result == 2 means API not available

            // If we get here, no compatible API was found
            throw new Exception("No compatible HMS API found in classpath");

        } catch (Exception e) {
            logger.severe("Error running HMS: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Print available runs in the project for debugging
     */
    private static void printAvailableRuns(Class<?> projectClass, Object project) {
        try {
            // Try various method names to get list of runs
            String[] methodNames = {"getRunNames", "getRuns", "getSimulations", "getComputeRuns",
                                   "getComputeRunManager", "getRunManager", "getSimulationManager"};

            // First, try to get runs directly
            for (String methodName : methodNames) {
                try {
                    Method method = projectClass.getMethod(methodName);
                    Object result = method.invoke(project);

                    if (result != null) {
                        System.err.println("Available runs in project:");
                        if (result instanceof java.util.Collection) {
                            java.util.Collection<?> collection = (java.util.Collection<?>) result;
                            if (collection.isEmpty()) {
                                System.err.println("  (none - project has no runs)");
                            } else {
                                for (Object run : collection) {
                                    System.err.println("  - " + run);
                                }
                            }
                            return;
                        } else if (result.getClass().isArray()) {
                            Object[] array = (Object[]) result;
                            if (array.length == 0) {
                                System.err.println("  (none - project has no runs)");
                            } else {
                                for (Object run : array) {
                                    System.err.println("  - " + run);
                                }
                            }
                            return;
                        } else {
                            // Try to get runs from the manager object
                            System.err.println("Attempting to get runs from manager...");
                            printAvailableRunsFromManager(result.getClass(), result);
                            return;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    // Try next method
                    continue;
                }
            }

            System.err.println("Could not retrieve available runs - method not found");
            System.err.println("  Available methods on Project:");
            // List all available methods to help with debugging
            for (Method m : projectClass.getMethods()) {
                if (m.getName().toLowerCase().contains("run") ||
                    m.getName().toLowerCase().contains("simul") ||
                    m.getName().toLowerCase().contains("compute")) {
                    System.err.println("    - " + m.getName());
                }
            }
        } catch (Exception e) {
            System.err.println("Error while trying to list available runs: " + e.getMessage());
        }
    }

    /**
     * Try to get runs from a manager object
     */
    private static void printAvailableRunsFromManager(Class<?> managerClass, Object manager) {
        try {
            System.err.println("Manager class: " + managerClass.getName());
            String[] methodNames = {"getRunNames", "getNames", "getAll", "getAllRuns", "listAll", "values",
                                   "getRuns", "getSimulations", "getComputeRuns", "elements", "toArray"};

            boolean foundMethod = false;
            for (String methodName : methodNames) {
                try {
                    Method method = managerClass.getMethod(methodName);
                    foundMethod = true;
                    Object result = method.invoke(manager);

                    if (result != null) {
                        System.err.println("Available runs in project (via " + methodName + "):");
                        if (result instanceof java.util.Collection) {
                            java.util.Collection<?> collection = (java.util.Collection<?>) result;
                            if (collection.isEmpty()) {
                                System.err.println("  (none)");
                            } else {
                                for (Object run : collection) {
                                    System.err.println("  - " + run);
                                }
                            }
                            return;
                        } else if (result.getClass().isArray()) {
                            Object[] array = (Object[]) result;
                            if (array.length == 0) {
                                System.err.println("  (none)");
                            } else {
                                for (Object run : array) {
                                    System.err.println("  - " + run);
                                }
                            }
                            return;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    // Method doesn't exist, try next
                    continue;
                } catch (Exception e) {
                    System.err.println("Error calling " + methodName + ": " + e.getMessage());
                }
            }

            // Fallback: list all available methods on the manager
            if (!foundMethod) {
                System.err.println("None of the standard run-listing methods were found.");
            }
            System.err.println("Available methods on manager (" + managerClass.getName() + "):");
            Method[] allMethods = managerClass.getMethods();
            int count = 0;
            for (Method m : allMethods) {
                String methodName = m.getName();
                if (!methodName.startsWith("wait") && !methodName.equals("getClass") && !methodName.equals("hashCode") &&
                    !methodName.equals("equals") && !methodName.equals("toString") && !methodName.equals("notify") &&
                    !methodName.equals("notifyAll")) {
                    System.err.println("  - " + methodName);
                    count++;
                }
            }
            if (count == 0) {
                System.err.println("  (no public methods found)");
            }
        } catch (Exception e) {
            System.err.println("Exception in printAvailableRunsFromManager: " + e.getMessage());
        }
    }

    /**
     * Additional error checking via reflection (fallback if stderr capture misses errors)
     */
    private static boolean hasProjectErrors(Class<?> projectClass, Object project) {
        try {
            // Try common error checking methods on the project
            String[] errorMethodNames = {"getErrorCount", "hasErrors", "getErrors", "getMessageCount", "getMessages"};

            for (String methodName : errorMethodNames) {
                try {
                    Method method = projectClass.getMethod(methodName);
                    Object result = method.invoke(project);

                    if (result instanceof Integer) {
                        int count = (Integer) result;
                        if (count > 0) {
                            System.err.println("Project has " + count + " errors");
                            return true;
                        }
                    } else if (result instanceof Boolean) {
                        boolean hasErrors = (Boolean) result;
                        if (hasErrors) {
                            System.err.println("Project reports errors");
                            return true;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    // Method doesn't exist, try next one
                    continue;
                }
            }

            return false;
        } catch (Exception e) {
            // If we can't check for errors via API, stderr capture should have caught them
            return false;
        }
    }

    /**
     * Try HMS 4.14+ API using static Project.open(String) method
     * Returns: 0=success, 1=errors detected, 2=API not available
     */
    private static int tryStaticOpen(String hmsFilePath, String simulationName) {
        Class<?> projectClass = null;
        Object project = null;

        try {
            projectClass = Class.forName("hms.model.Project");
            Method openMethod = projectClass.getMethod("open", String.class);
            Method computeRunMethod = projectClass.getMethod("computeRun", String.class);
            Method closeMethod = projectClass.getMethod("close");


            project = openMethod.invoke(null, hmsFilePath);

            // Capture both stdout and stderr to check for HMS errors
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
            ByteArrayOutputStream capturedErr = new ByteArrayOutputStream();
            PrintStream outStream = new PrintStream(capturedOut);
            PrintStream errStream = new PrintStream(capturedErr);
            System.setOut(outStream);
            System.setErr(errStream);

            boolean hasErrors = false;
            Object result = null;

            try {
                result = computeRunMethod.invoke(project, simulationName);
            } finally {
                // Always restore stdout and stderr
                System.setOut(originalOut);
                System.setErr(originalErr);
                outStream.flush();
                errStream.flush();
                String stdoutOutput = capturedOut.toString();
                String stderrOutput = capturedErr.toString();

                // Print all captured output
                if (!stdoutOutput.isEmpty()) {
                    System.out.print(stdoutOutput);
                }
                if (!stderrOutput.isEmpty()) {
                    System.err.print(stderrOutput);
                }

                // Check for HMS error patterns in either output
                String allOutput = stdoutOutput + stderrOutput;
                if (allOutput.contains("ERROR") && (allOutput.contains("ERROR 2") || allOutput.contains("Could not find") || allOutput.contains("does not exist"))) {
                    hasErrors = true;
                }
            }

            // If HMS errors were detected, fail
            if (hasErrors) {
                logger.warning("HMS simulation failed - errors detected in output");
                closeMethod.invoke(project);
                return 1;
            }

            // Check if computeRun returned a status code (usually 0 for success)
            if (result instanceof Integer) {
                int status = (Integer) result;
                if (status != 0) {
                    logger.warning("HMS simulation failed with status code: " + status);
                    closeMethod.invoke(project);
                    return 1;
                }
            }

            // Check for errors in the project after simulation
            if (hasProjectErrors(projectClass, project)) {
                logger.warning("HMS simulation completed but with errors");
                closeMethod.invoke(project);
                return 1;
            }

            closeMethod.invoke(project);

            logger.info("Completed HMS simulation: model='" + hmsFilePath + "', simulation='" + simulationName + "'");
            return 0;

        } catch (NoSuchMethodException e) {
            logger.severe("4.14+ API not available: method not found - " + e.getMessage());
            return 2;
        } catch (ClassNotFoundException e) {
            logger.severe("4.14+ API not available: class not found - " + e.getMessage());
            return 2;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            String errorMsg = cause != null ? cause.getMessage() : e.getMessage();
            logger.severe("4.14+ API method invocation failed: " + errorMsg);
            if (cause != null) {
            } else {
            }
            // "Invalid run" and similar errors are simulation failures, not API incompatibility
            if (errorMsg != null && (errorMsg.contains("Invalid run") || errorMsg.contains("not found") ||
                                     errorMsg.contains("does not exist") || errorMsg.contains("Could not") ||
                                     errorMsg.contains("error") || errorMsg.toLowerCase().contains("failed"))) {
                // Provide debugging info for invalid run
                if (errorMsg.contains("Invalid run")) {
                    logger.severe("ERROR: Invalid run name: '" + simulationName + "'");
                    logger.info("Hint: The run name '" + simulationName + "' was not found in the HMS project.");
                    logger.info("Verify that:");
                    logger.info("  1. The run exists in the .hms file");
                    logger.info("  2. The spelling/capitalization matches exactly");
                    logger.info("  3. The file path '" + hmsFilePath + "' is correct");
                } else if (errorMsg.contains("does not exist")) {
                    logger.severe("ERROR: HMS model file not found: '" + hmsFilePath + "'");
                    logger.info("Hint: The HMS project file could not be opened.");
                    logger.info("Verify that:");
                    logger.info("  1. The file path is correct: '" + hmsFilePath + "'");
                    logger.info("  2. The file exists and is readable");
                    logger.info("  3. If using Docker, the volume mount is correct: -v LOCAL_PATH:CONTAINER_PATH");
                    logger.info("  4. Ensure LOCAL_MODEL_DIR exists and contains the .hms file");
                }
                return 1;
            }
            return 2;
        } catch (Exception e) {
            logger.severe("4.14+ API failed: " + e.getMessage());
            return 2;
        }
    }

    /**
     * Try HMS 4.12-4.13 API using Project constructor
     * Returns: 0=success, 1=errors detected, 2=API not available
     */
    private static int tryConstructorOpen(String hmsFilePath, String simulationName) {
        Class<?> projectClass = null;
        Object project = null;

        try {
            projectClass = Class.forName("hms.model.Project");
            Method computeRunMethod = projectClass.getMethod("computeRun", String.class);
            Method closeMethod = projectClass.getMethod("close");


            project = projectClass.getConstructor(String.class).newInstance(hmsFilePath);

            // Capture both stdout and stderr to check for HMS errors
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
            ByteArrayOutputStream capturedErr = new ByteArrayOutputStream();
            PrintStream outStream = new PrintStream(capturedOut);
            PrintStream errStream = new PrintStream(capturedErr);
            System.setOut(outStream);
            System.setErr(errStream);

            boolean hasErrors = false;
            Object result = null;

            try {
                result = computeRunMethod.invoke(project, simulationName);
            } finally {
                // Always restore stdout and stderr
                System.setOut(originalOut);
                System.setErr(originalErr);
                outStream.flush();
                errStream.flush();
                String stdoutOutput = capturedOut.toString();
                String stderrOutput = capturedErr.toString();

                // Print all captured output
                if (!stdoutOutput.isEmpty()) {
                    System.out.print(stdoutOutput);
                }
                if (!stderrOutput.isEmpty()) {
                    System.err.print(stderrOutput);
                }

                // Check for HMS error patterns in either output
                String allOutput = stdoutOutput + stderrOutput;
                if (allOutput.contains("ERROR") && (allOutput.contains("ERROR 2") || allOutput.contains("Could not find") || allOutput.contains("does not exist"))) {
                    hasErrors = true;
                }
            }

            // If HMS errors were detected, fail
            if (hasErrors) {
                logger.warning("HMS simulation failed - errors detected in output");
                closeMethod.invoke(project);
                return 1;
            }

            // Check for errors in the project after simulation
            if (hasProjectErrors(projectClass, project)) {
                logger.warning("HMS simulation completed but with errors");
                closeMethod.invoke(project);
                return 1;
            }

            closeMethod.invoke(project);

            logger.info("Completed HMS simulation: model='" + hmsFilePath + "', simulation='" + simulationName + "'");
            return 0;

        } catch (NoSuchMethodException e) {
            logger.severe("4.12-4.13 API not available: method not found - " + e.getMessage());
            return 2;
        } catch (ClassNotFoundException e) {
            logger.severe("4.12-4.13 API not available: class not found - " + e.getMessage());
            return 2;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            String errorMsg = cause != null ? cause.getMessage() : e.getMessage();
            logger.severe("4.12-4.13 API method invocation failed: " + errorMsg);
            if (cause != null) {
            } else {
            }
            // "Invalid run" and similar errors are simulation failures, not API incompatibility
            if (errorMsg != null && (errorMsg.contains("Invalid run") || errorMsg.contains("not found") ||
                                     errorMsg.contains("does not exist") || errorMsg.contains("Could not") ||
                                     errorMsg.contains("error") || errorMsg.toLowerCase().contains("failed"))) {
                // Provide debugging info for invalid run
                if (errorMsg.contains("Invalid run")) {
                    logger.severe("ERROR: Invalid run name: '" + simulationName + "'");
                    logger.info("Hint: The run name '" + simulationName + "' was not found in the HMS project.");
                    logger.info("Verify that:");
                    logger.info("  1. The run exists in the .hms file");
                    logger.info("  2. The spelling/capitalization matches exactly");
                    logger.info("  3. The file path '" + hmsFilePath + "' is correct");
                } else if (errorMsg.contains("does not exist")) {
                    logger.severe("ERROR: HMS model file not found: '" + hmsFilePath + "'");
                    logger.info("Hint: The HMS project file could not be opened.");
                    logger.info("Verify that:");
                    logger.info("  1. The file path is correct: '" + hmsFilePath + "'");
                    logger.info("  2. The file exists and is readable");
                    logger.info("  3. If using Docker, the volume mount is correct: -v LOCAL_PATH:CONTAINER_PATH");
                    logger.info("  4. Ensure LOCAL_MODEL_DIR exists and contains the .hms file");
                }
                return 1;
            }
            return 2;
        } catch (Exception e) {
            logger.severe("4.12-4.13 API failed: " + e.getMessage());
            return 2;
        }
    }
}
