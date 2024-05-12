# Tadaflow

Tadaflow is a configureable dataflow engine. It manages the flow of outputs of some node in a dataflow graph to the inputs of another node.

A **node** is a program that is part of the dataflow. It has **inputs** and **outputs**. An output of one node can be connected to an input of another node using a **Connection**. Upon executing a node will produce a new **resource** for each output, which will be used by the inputs connected to each output.

# Configuration

The engine has the following options:
```
Usage: tadaflow [OPTIONS]

Options:
  -c, --config-dir <CONFIG_DIR>                [default: /etc/tadaflow]
  -s, --state-dir <STATE_DIR>                  [default: /run/tadaflow]
  -l, --logfile <LOGFILE>                      
  -m, --management-socket <MANAGEMENT_SOCKET>  [default: /var/run/tadaflow]
  -h, --help                                   Print help
```

The configuration directory should contain the [configured dataflow graph](#dataflow-graph-configuration). The state directory is going to be used by the engine to store its state for later resumption of work upon termination. It also contains all the resources for the duration of their use.

If no log file is specified, then the engine will log to stdout.

## Dataflow graph configuration

The dataflow graph is given as a collection of yaml files with the following structure:

```yaml
nodes:
- node_id: <some-id>
  program: <command-to-execute>
  arguments:
  - list
  - of 
  - program
  - args
  inputs:
    <input-name>: <input-config>
  outputs:
    <output-name>: <output-config>
  typ: <node-type-configuration>
  error_handling:
    handling: <error-handling>
    error_log: <error-log-config>

connections:
- from_id: <source-node-id>
  output: <output-name>
  to_id: <destination-node-id>
  input: <input-name>
```

`<input-config>` configures how the resource is given to the program. It is either `Stdin` or `!FilenameArg`, which has a field `placeholder` of type string. Upon executing the node, all instances of the placeholder string in the argument list will be replaced with the path to the resource. An example of this can be seen in the example configuration below.

`<output-config>` configures how the resource is collected from the program. It is either `Stdout`, `Stderr` or `!File`, which has the field `path` that should contain the path to the file that is produced by the program.
Example:
```yaml
nodes:
- ...
  outputs:
    my-file: !File
      path: /path/to/file
```

The `<error_handling>` determines what the dataflow engine should do when the program returns a non-successful status code. Current options are `Fail` or `!Retry`, which retries after a delay of a numer of milliseconds specified by the field `delay_ms`. An example can be seen in the node `fail` in the example configuration below.

The `<error-log-config>` tells the dataflow engine where it can gather the error to log it. It allows `Stdout`, `Stderr` or `!Logfile` with the path to the log specified in the field `path`. Example for logfile:
```yaml
nodes:
- ...
  error_handling:
    error_log: !Logfile
      path: path/to/file
```

## Example configuration
This configuration connects the output of echo and consumes it:
```yaml
nodes:
- node_id: testnode
  program: echo
  arguments:
  - world
  inputs: {}
  outputs:
    stdout: Stdout
  typ: !Scheduled
    schedule: !Startup
      delay_ms: 0
  error_handling:
    handling: Fail
    error_source: Stderr
- node_id: consume
  program: mv
  arguments:
  - '%input'
  - '%input'
  inputs:
    input: !FilenameArg
      placeholder: '%input'
  outputs: {}
  typ: OnInput
  error_handling:
    handling: !Retry
      delay_ms: 1000
    error_source: Stderr

connections:
- from_id: testnode
  output: stdout
  to_id: consume
  input: input
```

