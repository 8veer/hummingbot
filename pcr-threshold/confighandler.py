from watchdog.events import FileSystemEventHandler
import json

class ConfigHandler(FileSystemEventHandler):
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.read_config()

    def on_modified(self, event):
        if event.src_path == self.config_file:
            self.config = self.read_config()
            print("Configuration updated:")
            print(self.config)

    def read_config(self):
        with open(self.config_file, 'r') as file:
            return json.load(file)