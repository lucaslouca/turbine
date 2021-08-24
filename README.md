# Connector
:alien: A generic framework that quickly enables a user to implement a multithreaded Poller/Parser/Sender connector.

## Table of Contents
- [Connector](#connector)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Architecture](#architecture)
  - [Setup IDE](#setup-ide)
  - [Configuration](#configuration)
  - [How to run](#how-to-run)
  - [File Connector](#file-connector)
  - [Client](#client)

## Features
* **Configurabe**: The connector is fully customizable and configurable. Connector name, poller/parser/sender implementation can be configured using a `.ini` file.
* **Logging**: Build in logging that can be configured using a `.ini` file. Any exceptions are automatically caught and properly logged.
* **Multithreading**: For maximum performance you can define how many pollers, parsers and senders the connector should spawn. It autimatically optimizes based on available CPUs.
* **Transaction Handler**: You want to move successfully proccessed files (See [File Connector](#file-connector)) to an _archive_ directory and failed-to-proccess files to an _error_ folder? No problem. Just implement your custom Transaction Handler to get notified when a poll just finished or failed processing which you can then handle as you see fit.
* **Monitoring**: The connector can be configured to listen on a specified port. Using the build in client a user can remotely connect to the connector and run basic queries or kill the connector.
  
## Architecture
The main workers of the framework are the _Poller_, _Parser_ and the _Sender_.

**Poller**: Poller threads connect to the source and poll for data. The source can be a database, file system, Web API, etc. The data returned by the Poller is then passed on to the Parser for further processing.

**Parser**: Parsers get the data polled by the Pollers. They implement the parsing logic such as extracting specific data or generating JSON objects. The results are then passed on to the Senders.

**Sender**: Sender threads are responsible for persisting or passing the processed data on to a different service. For example Senders can persist the data into a relational database or publish to a Kafka topic.

![System Landscape](documentation/System_Landscape.png)

## Setup IDE
Here is an example on how to setup VSCode for use with this project. You can use any IDE you like.

```shell
$ git clone https://github.com/lucaslouca/pps-connector
$ cd pps-connector

# Create and activate virtual environment
$ python3 -m venv env
$ source env/bin/activate
```

In order to run the provided sample connectors you have to install the following libraries:
```shell
# Used by Directory Watcher
(env) $ pip install watchdog

# For AWS S3 Sender
(env) $ pip install boto3
```

Finally open the project in VSCode:
```shell
(env) $ code .
```

Make sure VSCode also opens the project in the correct virtual environment.

Create a new launch configuration. Below is a run configuration to launch the `File Connector` using VSCode:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Directory Connector",
            "type": "python",
            "request": "launch",
            "program": "${workspaceRoot}/main.py",
            "console": "integratedTerminal",
            "args": [
                "-c",
                "config/file_connector.ini"
            ]
        }
    ]
}
```

## Configuration
Below is a sample configuration for the [File Connector](#file-connector):

```ini
[CONNECTOR]
Name=File Connector
Host=127.0.0.1
Port=35813

[POLLER]
Class=implementation.poller.Poller
Args={"file_dir":"in"}
MinThreads=2
MaxThreads=2

[PARSER]
Class=implementation.parser.FileParser
Args={}
MinThreads=2
MaxThreads=2

[SENDER]
Class=implementation.sender.FileSQLiteSender
Args={"db":"database.db"}
MinThreads=2
MaxThreads=2

# Optional
[TRANSACTION_HANDLER]
Class=implementation.file_transaction_handler.FileTransactionHandler
Args={"archive_dir":"out/archive", "error_dir":"out/error"}
```

## How to run
```shell
$ source env/bin/activate
(env) $ python main.py

CONNECTORS
-------------------------------------------------------------
[0] - XBRL Index Connector
[1] - XBRL Download Connector
[2] - File Connector
Connector to start> 2
...
```

To start the connector in the background just run it using `screen`:
```shell
(env) $ screen -S "connector" -dm python main.py
```

You can see that the framework automatically picks up any configuration files found in the `config` directory and lists them as a run option.

## File Connector
The project comes with a few pre-implemented connectors. One of them is the File Connector.

This is a connector that polls files from the file system, parses the files based on their file type using custom extractors and then persists the extracted data (e.g.: e-mail address) into a SQLite database.

**Create Custom Extractors**

You can easily create your custom extractors by subclassing the `AbstractExtractor` class and place the file into the `extractors` folder. The connector will pick them up automatically on next restart.

> **Important**: Your extractor class name must be the same as the file name of the Python file that contains it. For example, if your custom extractor is called `YourCustomExtractor`, then it must be placed in a Python file called `YourCustomExtractor.py` under the `extractors` directory.

Here is an example of an extractor that does regex matching for each line in a `.TXT` file in order to return any contained E-Mails:

```Python
from implementation.abstract_extractor import AbstractExtractor
from implementation.model.data_extraction_request import DataExtractionRequest
from connarchitecture.decorators import overrides
import re


class FactExtractor(AbstractExtractor):
    @overrides(AbstractExtractor)
    def supports_input(self, file: DataExtractionRequest):
        return file.extension and file.extension.upper() == ".TXT"

    @overrides(AbstractExtractor)
    def extract(self, file: DataExtractionRequest):
        self.log(f"Proccessing '{file.file_path}'")
        pattern = re.compile("[\w\.-]+@[\w\.-]+")

        emails = []
        for i, line in enumerate(open(file.file_path)):
            emails += pattern.findall(line)

        if emails:
            return {'emails': emails}
        else:
            return {}
```
In the above example the method `supports_input(...)` simply tells the framework that this extractor can only be used for text files.

The `extract(...)` method should return a dictionary that holds the extracted type (e.g. `emails`) as key and the extracted values as a list.

Notice also how the extractor comes with logging capabilities as well using `self.log(...)`.

## Client
The framework comes also with a monitoring client. You can use this client to connect to your running connector and view its status or kill it.

```shell
(env) $ python client.py 127.0.0.1 -p 35813
=============================================================

          _____                       __          
         / ___/__  ___  ___  ___ ____/ /____  ____
        / /__/ _ \/ _ \/ _ \/ -_) __/ __/ _ \/ __/
        \___/\___/_//_/_//_/\__/\__/\__/\___/_/
        
=============================================================
File Connector
-------------------------------------------------------------
   Started: 2021-07-03 20:00:17.066630+02:00
    Poller: Poller (2)
    Parser: FileParser (2)
    Sender: FileSQLiteSender (2)
-------------------------------------------------------------
Connector commands:
-------------------------------------------------------------
      stop: stop connector
     stats: show statistics
      head: show the first items in the queues
=============================================================
Client commands:
-------------------------------------------------------------
      quit: quit client
=============================================================

command>stats
         Polled: 5
         Parsed: 5
      Completed: 2
         Errors: 0

command>stop
(env) $ 
```
