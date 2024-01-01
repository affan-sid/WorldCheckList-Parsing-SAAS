import os

def load_env_file(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            key, value = line.strip().split('=')
            os.environ[key] = value