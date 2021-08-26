from connarchitecture.logging_component import LoggingComponent
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import json


class FileDirWatcher(LoggingComponent):
    def __init__(self, dir_to_watch, queue):
        LoggingComponent.__init__(self, self.component_name())
        self._dir_to_watch = dir_to_watch
        self._queue = queue
        self.observer = Observer()

    def run(self):
        self.log(f"Watching {self._dir_to_watch}")
        event_handler = Handler(name=self.component_name(), queue=self._queue, dir_to_watch=self._dir_to_watch)
        self.observer.schedule(event_handler, self._dir_to_watch, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()

    def component_name(self):
        return "FileDirWatcher"


class Handler(FileSystemEventHandler, LoggingComponent):
    def __init__(self, name, queue, dir_to_watch):
        FileSystemEventHandler.__init__(self)
        LoggingComponent.__init__(self, name)
        self._dir_to_watch = dir_to_watch
        self._queue = queue
        self._load_files(dir=self._dir_to_watch)

    def _file_topic(self, file: str) -> str:
        with open(file, 'r') as json_file:
            try:
                json_data = json.load(json_file)
                topic = json_data['topic']
                return topic
            except Exception as e:
                self.log_exception(message=f'Unable to load json {file}', exception=e)

    def _handle_file(self, file: str):
        file_name = os.path.splitext(os.path.basename(file))[0]
        if not file_name.startswith("."):
            tuples = []
            topic = self._file_topic(file=file)
            if topic == 'concept':
                tuples = self._generate_concept_tuples(file=file)
            elif topic == 'price':
                tuples = self._generate_price_tuples(file=file)
            for t in tuples:
                topic = t[0]
                self.log(f"Adding '{t}' to queue")
                self._queue.put_topic(topic, t[1:])

    def _load_files(self, dir):
        for f in os.listdir(dir):
            path = os.path.join(dir, f)
            if os.path.isfile(path):
                self._handle_file(file=path)

    def _generate_concept_tuples(self, file: str) -> list[(str, int, str)]:
        result = []
        with open(file, 'r') as json_file:
            try:
                json_data = json.load(json_file)
                topic = json_data['topic']
                for resource in json_data['resources']:
                    for ticker in resource['tickers']:
                        concepts = resource['concepts']
                        for concept in concepts:
                            concept_name = concept['name']
                            year = concept['year']
                            result.append((topic, ticker, concept_name, int(year)))

            except Exception as e:
                self.log_exception(message=f'Unable to load json {file}', exception=e)
        return result

    def _generate_price_tuples(self, file: str) -> list[(str, int, str)]:
        result = []
        with open(file, 'r') as json_file:
            try:
                json_data = json.load(json_file)
                topic = json_data['topic']
                for resource in json_data['resources']:
                    for ticker in resource['tickers']:
                        years = resource['years']
                        result.append((topic, ticker, [int(y) for y in years]))

            except Exception as e:
                self.log_exception(message=f'Unable to load json {file}', exception=e)
        return result

    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type == 'created':
            self._handle_file(file=event.src_path)
