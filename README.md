# buzzline-06-rogers

# Project Overview
This project generates random streaming movie reviews to a Kafka Topic.
A consumer then reads those reviews and enters them into an SQLITE table.
Following that the results are visualized into a graph.

# Data
The SQLite data can be found in the data folder under movie reviews.


## VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- SQLite Viewer by Florian Klampfer
- WSL by Microsoft (on Windows Machines)

## Task 1. Create and activate a .venv

In order to run this project a .venv environment will need to be created and active.
1. Open a terminal
2. Enter the following code in the terminal
    1. python -m venv .venv (this creates the virtual environment)
    2. .venv\scripts\activate (activates the environment)
3. Install the packages into the environment using the requirements,txt file.
    1. pip install -r requirements.txt


## Task 2. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

These will need to be started in using after changing the system to wsl.

Please follow the steps in the "SETUP-KAFKA.md" if there is any doubt on how to do this.


1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 3. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 



### Producer (Terminal 3) 

Start the producer to generate the messages. 
The existing producer writes messages to a live data file in the data folder.
If Zookeeper and Kafka services are running, it will try to write them to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_rogers
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_rogers
```

The producer will still work if Kafka is not available.

### Consumer (Terminal 4) 

Start an associated consumer. 
You have two options. 
1. Start the consumer that reads from the live data file.
2. OR Start the consumer that reads from the Kafka topic.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.kafka_consumer_rogers
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_case
```

---


### Custom Consumer
The custom consumer for this project was a lof of fun to build. 

The consumer the reads and processes these messages and stores them in an SQLite database
Finally, the consumer access the data stored in the database and develops visuals to display the data. 

To create this consumer I forked the consumer from buzzline-05-rogers.
I then removed all of the informtion that did not directly involve this project.
Following that I updated the files and where they access the code. This is where a utils file really comes in handy. If used correctly the utils files make it easy to update and change centralized variables.

Finally, new visual were constructed and launched from that data that was stored in the database.

 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
