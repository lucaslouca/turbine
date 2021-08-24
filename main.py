"""
HOW TO RUN:
Step 1:
Intall 'screen':
https://unix.stackexchange.com/questions/449356/how-do-i-install-screen-on-rhel-7-2

Step 2:
$ screen -S "xbrl" -dm python main.py

To see the process again, use in terminal:
$ screen -ls
$ screen -d -r 103820.xbrl
"""

from connarchitecture.connector import Connector
from connarchitecture.constants import Constants
from os import listdir
from os.path import isfile, join
import configparser
import sys
import argparse


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', action='store', type=str,  help='configuration to use')
    args = arg_parser.parse_args()
    connector_config = args.c

    if not connector_config:
        connector_map = {}
        connector_names = []
        config_parser = configparser.ConfigParser()
        config_dir = 'config'
        config_files = [f for f in listdir(config_dir) if isfile(join(config_dir, f))]
        for config in config_files:
            config_path = f"{config_dir}/{config}"
            config_parser = configparser.ConfigParser()
            config_parser.read(config_path)
            if config_parser.has_section(Constants.CONFIG_SECTION_CONNECTOR):
                connector_name = config_parser.get(Constants.CONFIG_SECTION_CONNECTOR, Constants.CONFIG_CONNECTOR_NAME)
                connector_map[connector_name] = config_path
                connector_names.append(connector_name)

        if not connector_names:
            print("No connectors found")
            sys.exit(1)

        print("CONNECTORS")
        print("-------------------------------------------------------------")
        for index, connector_name in enumerate(connector_names):
            print(f"[{index}] - {connector_name}")

        try:
            i = int(input("Connector to start>"))
            while i < 0 or i > len(connector_names):
                i = int(input("Connector to start>"))

            connector_config = connector_map[connector_names[i]]
        except Exception as e:
            sys.exit(1)

    connector = Connector(connector_config)
    connector.connect()


if __name__ == "__main__":
    main()
