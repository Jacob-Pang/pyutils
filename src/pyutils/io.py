import colorama
import os
import sys

colorama.init()

class IOStreamConfig:
    @staticmethod
    def mute_output():
        return IOStreamConfig(stdout_dest=os.devnull)

    def __init__(self, stdout_dest = sys.stdout, stderr_dest = sys.stderr, stdin_dest = sys.stdin):
        self.stdout_dest = stdout_dest
        self.stderr_dest = stderr_dest
        self.stdin_dest  = stdin_dest

    def __enter__(self) -> None:
        self.origin_stdout = sys.stdout
        self.origin_stderr = sys.stderr
        self.origin_stdin  = sys.stdin

        sys.stdout = self.stdout_dest
        sys.stderr = self.stderr_dest
        sys.stdin  = self.stdin_dest

    def __exit__(self, *args, **kwargs) -> None:
        sys.stdout = self.origin_stdout
        sys.stderr = self.origin_stderr
        sys.stdin  = self.origin_stdin

def erase_stdout(stdout_lines: int) -> None:
    for _ in range(stdout_lines):
        # move up cursor and delete whole line
        print("\x1b[1A\x1b[2K", end='\r')

if __name__ == "__main__":
    pass