# auto-fpt-hms

Docker container for running HEC-HMS hydrological simulations.

## Image Size

~1.88GB - Primarily HMS binaries, libraries, and bundled Java runtime. Image size cannot be significantly reduced without removing essential HMS components.

## Supported Versions

**Stable Release (Tested & Working):**
- HMS 4.13 (default)

**Previous Stable Release:**
- HMS 4.12

**Beta Versions (Tested & Working):**
- HMS 4.13-beta.6
- HMS 4.14-beta.1

**Legacy (Not Tested):**
- HMS 4.11, 4.9, 4.10 (available but not actively supported)

## Usage

Run an HMS simulation by passing the HMS project file path and simulation name as arguments:

```bash
docker run -v /path/to/models:/mnt/model hms-docker /mnt/model/project.hms simulation_name
```

### Arguments

- `filepath`: Path to the HMS project file (`.hms`)
- `simname`: Name of the simulation to run within the project

### Volume Mounts

Mount your HMS model files at `/mnt/model` in the container. The container expects to find `.hms` project files at this location.

## Building

Build the Docker image with a specific HMS version:

```bash
# Build with default version (4.13)
docker build -t hms-docker .

# Build with specific versions
docker build --build-arg HMS_VERSION=4.12 -t hms-docker:4.12 .
docker build --build-arg HMS_VERSION=4.13 -t hms-docker:4.13 .
docker build --build-arg HMS_VERSION=4.13-beta.6 -t hms-docker:4.13-beta .
docker build --build-arg HMS_VERSION=4.14-beta.1 -t hms-docker:4.14-beta .
```

The build process:
1. Compiles the Java HMS runner application using reflection-based API detection
2. Downloads HEC-HMS binaries and dependencies for the specified version (excludes samples/docs)
3. Creates a minimal production image with all required libraries and non-root user execution
4. Single `RunHMS` class supports all versions through runtime API detection