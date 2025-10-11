# Streamlet 🌊

Streamlet is a simple to configure data pipeline built on [Celery](https://docs.celeryq.dev/en/stable/).
It can run periodic tasks that fetch metrics, process them and finally send it to other storages.

## Version Changes

Excluding small features and bugfixes, the following shows the version history:

- `v2.3.0`: Pattern keys, nested Metrics and Transform priority
- `v2.2.0`: Added EOS support, test cases and fixes
- `v2.1.0`: Latest CERN certificate, Schema unions and fixes
- `v2.0.0`: Core rewrite, improved validation, settings and task templates
- `v1.8.0`: Template grouping and extending (Reworked in v2!)
- `v1.7.0`: Disable tasks and new Processors
- `v1.6.0`: Nested access Data class (Reworked in v2!)
- `v1.5.0`: Custom processors and task routing
- `v1.4.0`: Automatic retries on failure
- `v1.3.0`: Streamlet metric collection
- `v1.2.0`: OpenTelemetry static exporter
- `v1.1.0`: Introduced Templating support (Reworked in v2!)
- `v1.0.0`: Initial full release

## Design and Terminology

Streamlet is designed to run many heavy IO bound tasks isolated in parallel.

### Flow and Modules

The central instance is the `Flow`, which is a Celery application.
The Flow can have multiple Modules, which are either Inputs, Transforms and Outputs.
It can be configured using YAML files, which is explained in detail [here](#configuration).

The `Input` is responsible for fetching data from a source.
They do so by running `Tasks` on a schedule.
Some modules are PostgreSQL, OpenSearch, and HTTP inputs.

The `Transform` receives a MetricFrame and can manipulate it.

The `Output` is responsible for sending the data to an external storage.
In this final stage, the individual metrics cannot be manipulated anymore.

### Tasks and Metrics

Each Input can have multiple `Tasks`.
A Task is a periodic function, which triggers the fetching of values by the Input.
After creating a MetricFrame from the result, the Task sends it to other Modules along the `Task Chain`.

The Task produces a `MetricFrame`, which is a list of common `Metrics`.
All Metrics in a MetricFrame share a timestamp and a common name.
The Metric is the basic Data unit and is structured close to OpenTelemetry metrics.
It has one single metric value and can have multiple attributes.

## Configuring Streamlet

Streamlet is configured with one or multiple files, cmd arguments, and environment variables.

### Configuration File

The configuration file should be written in YAML and has the following base structure
(with items in brackets being optional):

```yaml
flow:
  version: v1

(env): ...

inputs: [ ... ]

(transforms): [ ... ]

outputs: [ ... ]
```

Check the `examples/` folder after reading this guide.
It contains files for various scenarios with detailed explanations.

### Validation

One of Streamlet's core features is strong validation engine.
Each configuration gets validated for required, optional and extra fields, as well as for their types.

The following example is to demonstrate its capabilities.
Imagine an overly simplified configuration as follows:

```yaml
input:
  - ...
  - ...
  - type: PostgreSQL
    tasks:
      - ...
      - name: some_task
        cronn: "0 0 * * *"
        params: ...
```

In this scenario, we want to run some SQL query (not shown here) at midnight.
The configured Input is the third in the list with the incorrect task being the second.
Unfortunately, the field name of the schedule has been mistyped, as it should be `cron:` and not `cronn:`.
Streamlet will print an error on start up, as it checks for required fields and fields that it does not need.
The following message will be printed:

```log
[2025-07-24 15:41:10,457: 🟢 INFO /flow] Starting Version 2.0.0.
[2025-07-24 15:41:10,570: 🟢 INFO /mods] Successfully initialized 15 modules.
[Invalid] ╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┬🠆 Field: [<>][input][2][tasks][1] > cronn
                         └🠆 Error: extra keys not allowed. Did you mean: `cron: 4 23 * * *`?
[2025-07-24 15:41:10,580: 🔥 CRIT /flow] Please check your configuration or documentation for more details.
```

The printed error can be read following this guide:

```log
[Exact type of thrown Error] ┬🠆 Exact field in your Flow configuration that raised the exception.
                             └🠆 More detailed information on the raised exception with an optional suggestion.
```

To find the exact place of the error, the path in the third line has to be followed,
i.e., the third input's second task on field `cronn:`.
Streamlet does not recognize this field, so it raises an error.
On printing, it will check other expected fields on that path for similarity.
On finding a similarly named field, it will print out a suggestion of what the intended configuration might have been.

Tip: Pass the `--only-validate` flag as an argument to only validate the configuration.

### Streamlet Settings

Streamlet offers various settings for configuration.
These can be configured in multiple ways, as explained in the following:

`CMD Arguments`:
Pass settings to streamlet directly via the cmd, using `--setting-name <value>`.
These are persistent arguments, which means that other settings will not overwrite these.
Pass `--help` to see all available arguments.
Boolean arguments are `false` by default and can be set as above or as `true` by only passing the name as a flag.

`Flow-Settings`:
Inside the flow configuration in `flow: settings: setting_name: ...` you can set the same settings to a value.
This checks if the setting is available and the passed value is of correct type.
However, Modules are loaded before the configuration, which means that some settings cannot be set in the file,
for example, the configuration path.

`Environent variables`:
You can set environment variables as `STREAMLET_SETTING_NAME` and they will be loaded.
These are loaded on startup before Modules, but can also be added later by using the configuration field `env: ...`.

Check [this file](docs/SETTINGS.md) for all available settings and their types.

After reading this guide, you can check [Modules](docs/MODULES.md) for detailed information on the available modules and
their specific configuration options.

Streamlet is configured using `YAML` files and environment variables.
To get started, you have to follow this basic schema:

### Timezone Support (v2.2.5)

To adjust the timestamps printed in logs, set the `TZ` environment variable to your preferred timezone 
(see list [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)).

**Note**: The following Setting is experimental and not fully tested.

To set the timezone of celery tasks and metric timestamps, set the `timezone` variable.
By default, the UTC timezone is used, as this works well with other infrastructure.

### Modules

All modules share the following settings:

- `type`: Type of the module
- `name?`: Add an optional unique name for identification
- `enabled?`: Enable or disable the module, enabled by default
- `connection?`: Connection parameters dependent on the defined type

### Parameters

Transforms and Outputs have the field `params`, which holds Module specific parameters.
Check [Modules](docs/MODULES.md) for all possible options.

### Tasks

Inputs have another field called `tasks`.
This is a list of defined tasks, with the following fields:

- `name`: A global task name that acts as a unique identifier
- `cron`: Pass a [crontab style](https://crontab.guru/) schedule for the task
- `result?`:
    - `metrics`: Fetched data fields that are metrics
    - `attributes`: Fetched data fields that are attributes
- `static_attributes?`: A dictionary of items to be added as attributes
- `max_retries?`: Number how often a task should be retries, default is 2
- `retry_delay?`: Delay after which a task should be retried, default is 10s
- `repeat_for?`: A dict of variables holding a list of values, disabled by default

#### Metrics and Attributes

The `result` fields specify, how the fetched Data by an input is processed into a MetricFrame.
By default, the field named `metric` will be set as the metrics field and the rest as attributes.
On defining multiple metric fields, there will be a metric for each defined field,
with the attributes being all fields from the result that are not specified in the metrics.
Setting the `attributes` field will only extract certain fields from the result instead of all.
The following example should explain it further:

In the example, the fields are set as following:
`metrics: ["a", "b"]` and `attributes: ["c", "d"]`, 
with the MetricFrame being `{"a": 4, "b": 9, "c": 3, "d": 6, "e": 1}`.
This will return in the generation of two Metrics as:
`Metric1[metric: 4, attributes: {"c": 3, "d": 6}]` and `Metric2[metric: 9, attributes: {"c": 3, "d": 6}]`
The original field's name can be accessed later via `metric_field_name`.

Following OpenTelemetry's specifications, the `metric` has to be an integer, float, boolean or complex number.
It is possible to leave the metric field empty, by setting `metrics:` to `None`.
This will map all values as attributes, which can be used to read or process raw data.
However, this requires the setting `allow_none_metric` to be `True`, otherwise an exception will be raised.

**New in v2.3**: For fetching multiple metrics, it is possible to define a unix like pattern.
This will extract all fields matching the pattern as metrics. 
For fetching all fields as metrics, use the wildcard `*`.

In addition, access nested fields with the seperator defined in the setting `nested_attr_seperator` (`.` by default).
This works with patterns as well, i.e. defining `_source.*` would extract all nested fields from `_source`.

Check `examples/1_basic.yaml` for a full configuration example.

#### Task Routing

Inputs and Tasks have a unique name, by which it is possible to add routing via filters on Transforms and Outputs.
The following settings can be used: `include_tasks`, `include_inputs`, `exclude_tasks` and `exclude_inputs`.
Logically, it is not possible to use include and exclude from the same type at once.
It is possible to combine different types, i.e., use `exclude_tasks` with `include_inputs` which will filter for both.

The fields hold one single string, which is the name or a list of multiple strings.
It is possible to use unix-like patterns, i.e., filtering for multiple tasks using `*` as wildcards.
The defined names do not get checked weather they exist or match any configured modules.
It is recommended to examine the printed Task routes with the `only_validate` option for checking.

Check `examples/4_with_routing.yaml` for a full configuration example.

#### Task Templates

A more advanced capability of Streamlet is to define a task template.
This template is being repeated and rendered with a list of provided values.

Enabled it by setting `repeat_for` to a dictionary of variable names to lists of values.
This could look as following:

```yaml
tasks:
  - name: some_task_$i
    cron: "$minute * * * *"
    repeat_for:
      table: a, b, c
      minute: 0, 20, 40
    params:
      query: SELECT COUNT(*) FROM $table
```

The field `repeat_for` holds keys, which correspond to the variable's names in the configuration.
For each pair, the task gets repeated with the specified values.
Here, there will be three tasks running with an offset of 20 minutes reading from different tables.
As all Tasks have to have a unique name, there has to be one variable inside the Task's name.
Apart from the defined variables, `i` can be accessed in the template and holds the loop's index, i.e. `0, 1, 2, ...`.

Check `examples/6_with_generator.yaml` for a full configuration example.

### MetricFrame Modifiers

Inputs and Outputs can modify the set Timestamp for all Metrics.
By default, the Timestamp is set to the value of the Task being started.
That value can be modified by setting fields in `modifiers`.
The following can be set:

- `time_offset`: Offset the timestamp by a timeframe.
  Integers are interpreted as seconds, while time periods as `1d` are also supported.
- `time_modulus`: Round the timestamp down to the next value in mod timestamp.
  By setting it to `1h` it would round down to the current hour.

Check `examples/7_with_modifiers.yaml` for a full configuration example.

### Flow Templates

It is possible to split one configuration into multiple files using extensions.
In the main configuration, inside the header, set `extends:` to a list of files.
Each file gets loaded on top of the base configuration, in reversed order.
The configuration gets expanded with the additional values.

By setting names for Modules and Tasks, it is possible to overwrite tasks.
The base configuration does not have to be fully complete, as it only gets lightly checked.
After the merge, the result gets fully validated before continuing.

**New in v2.3:** It is possible to order Transforms by setting the `priority` key.
This can also be used in combination with overwriting predefined Transforms by their name.
By default, all Transforms have the priority `0` and can be set to a number between `-256` and `256`.
A higher priority number is prioritized in order, while lower numbers are the last ones to be executed. 
Use a negative priority to ensure a Transform is executed last in a Task's chain.

Check `examples/2_with_template.yaml` for a full configuration example.

## Running

This section will guide you through running and deploying Streamlet.

### Locally

The easiest way of running Streamlet locally us using `uv`.
Create a virtual environment and install Streamlet with `uv sync --dev` and `uv pip install -e .`.

For Celery to work properly, you need to have a broker running.
By default, Streamlet uses [Redis](https://redis.io/open-source/) as its broker and backend.
This can be changed in the settings.
Get the broker easily running with Docker using the following command:

```shell 
docker run -it --rm --name streamlet_broker -p 6379:6379 redis:8-alpine
```

To get started, run Streamlet with the provided demo configuration.
This will print random numbers to the console every full minute.

```shell
uv run src/main.py --config "$PWD/examples/0_demo.yaml"
```

Finally, create a valid configuration (as described [here](#configuring-streamlet))
and optionally define an `.env` file for secrets.
Start Streamlet with the following command (remove env if not needed):

```shell
uv run [--env-file .env] src/main.py --config "$PWD/configuration/path.yaml"
```

For developing and debugging, check the [debugging section](#debugging).

### Using Proxies

If you need to use a Proxy for running Streamlet,
you can use the [Proxychains](https://github.com/haad/proxychains) program with the included `proxychain.conf` file.
For development, you can run your broker locally, while other connections are routed through your local proxy.
The file is configured to connect to `localhost:8080`, change that accordingly if needed.
Use the following command to run Streamlet:

```bash
proxychains -f proxychain.conf uv run ...
```

### OpenShift

Streamlet can easily be integrated into running with your OpenShift project.
Redis has a low memory usage of about 3-8MB, but you can re-use any broker in your project.

For using the provided with deployment in `openshift.yaml`, define the following:

- Define your configuration in the provided `streamlet-config` ConfigMap.
- Define environment variable secrets as `streamlet-env-secret`.

Finally, you can deploy Streamlet using the `oc` or `helm` command in your project.

## Developing

Streamlet's core code can be found in `src/core/` and modules in `src/modules/`

### Debugging

Recommended settings for developing and debugging the core or new modules are listed in the following.
Enable them by your required use case:

- `log_level`: Set the logging level to *10* (=Debug) for more verbose logging.
- `only_validate`: Validate the configuration, print all task chains and exit after.
- `run_once`: Run all tasks one single time, right after startup.
- `print_config`: Print the raw validated configuration.
- `print_traceback`: Print exception tracebacks on startup and when executing tasks.
- `disable_outputs`: Disable calling of any outputs by Tasks.
- `disable_default`: Disable all modules and tasks by default, having to enable each manually.
- `celery_pool`: Set the pool to *solo* for executing tasks one after another. This disables auto exit with `run_once`.
- `disable_readiness_probe`: Do not start the readiness probe endpoint.
- `skip_disabled_validation`: Do not validate Modules or Tasks that are disabled.

Furthermore, you can use a Debugger to get deeper insights on the Core and Modules.

### Versions

On pushing a commit to any branch, it will build an image accordingly and push it to
[harbor](https://registry.cern.ch/harbor/projects/3791/repositories/streamlet/artifacts-tab).
The tag it gets pushed to depends on the version specified in `pyproject.toml`.

By default, all builds will be pushed to `:latest`.
For builds where the field `version` in `pyproject.toml` is a full version, the build will be pushed to
`:<main-version-number>`, i.e. `2.1.3` will be pushed to `:2`.
This acts as the stable builds to be used in production environments.
For development builds, specify the `version` to include a dev tag, i.e. `2.0.0a1` or `2.1.0b1`.
These will only be pushed as `:latest`, but not stable versions.

### Adding custom Modules

Integrating custom Modules is easily doable.
Create a new file in `modules/` for implementing it in there.
Depending on the type of Module (Input, Transform, Output), import the `Abstract<Module>` from `core.modules`.

Next, create a class, where the name of the class is the referred for its `type`.
This can be overwritten later by setting the function `module_name(cls, ...)`.
The class should inherit from one of the Abstract Modules, i.e., for an Input it inherits from `AbstractInput`.
Check `abstract.py` for all available functions either to be implemented or optionally settable.

After fully implementing the Modules, decorate the class with `Importable()`, which is found in `core.modules`.
This sets a Flag which makes the class be importable as a module.
It is not possible to inherit from Importable modules, as this can lead to Modules breaking.
You can circumvent this by creating another class, inheriting from a not importable module and adding the decorator to
the empty class.

Furthermore, use `self.logger` for logging messages with the preconfigured logger.

#### Implementing \_\_call__()

Each module needs to be callable, which is where the Data is created or processed.
On loading a Module, the function gets inspected for correct fields and types.

Inputs take a variable `params`, which are the parameters from the Task.
The returned result should be either a dictionary (one metric) or a list of dictionaries.

Transforms and Outputs take a MetricFrame `data` as parameter.
Neither of the Modules return anything.
While Transforms can manipulate Metrics (Items of a MetricFrame), Outputs cannot change them.

Connection settings can be accessed via `.connection_config` once `super()` has been called.
For Transforms and Outputs, the parameters are accessible via `.settings`.

**Note**: The `__call__` method is not thread-safe from the execution side! 
Multiple Tasks can call it at the same time. 
It is up to the developer to add synchronization where necessary (i.e. modifying stateful Module variables).

#### Configuration Schemas

For defining required variables, each Module can define schemas for connection and (Task-) parameters.
These schemas must be a dictionary or a Union of dictionaries.
They can contain `voluptuous` objects, like `Optional()` for optional keys.
By default, all keys are marked as required.
Check implementations for Modules in `modules/` for examples.

For connection parameters, define the function `connection_schema()`.
Inputs have a function `task_params_schema()`, for Task parameters.
Transforms and Outputs implement `params_schema()` for their parameter field.
When all keys are Optional, defining `connection:` or `params:` is not required anymore.
It will automatically fill the values with the defined defaults.
In the case of using Union, the first item where all keys are optional will be set as the default values.

After setting the schema, it is not required to adjust the [Modules](docs/MODULES.md) documentation.
That is generated automatically from the provided Schema and docstrings on the CI/CD Pipeline.
For the Schemas, set the docstring to be in the format of: `:KEY: DESCRIPTION`.
The generator will fetch automatically, weather the key is Optional or not from the defined Schema.
This is not required for Module docstrings, set them to be a short explanation of what the Module does.

### Hooks

Modules have three different hooks that are run on startup and shutdown.
These should be used to initiate outside connections (i.e., connecting to a db) or cleaning up.
The following can be implemented:

- `on_connect`: Run a function after before Tasks being run
- `on_pre_shutdown`: Run when shutdown signal has reached but the instance is still running
  This can be used to run Celery Tasks and Streamlet Tasks
- `on_shutdown`: Triggered directly after `pre_shutdown` and should fully close the module.
