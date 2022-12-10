import os
import json
import requests

from collections import defaultdict
from subprocess import Popen, PIPE
from telethon import TelegramClient, events
from threading import Thread

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> None:
    for _ in range(99):
        try:
            return requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": message, "parse_mode": "html"})
        except:
            pass
    
    requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "html"})

def make_bot_from_config(config_json: str) -> "CommandBotBase":
    with open(config_json) as json_file:
        config = json.load(json_file)

    name = config.get("name") if "name" in config else "CommandBot"

    client = TelegramClient(name, int(config.get("api_id")), config.get("api_hash"))
    return CommandBotBase(client, config.get("bot_token"), name=name)

def run_command_bot(command_bot: "CommandBotBase") -> None:
    @command_bot.client.on(events.NewMessage(pattern="/(?i)"))
    async def run_event_handler(event):
        await command_bot.event_handler(event)
    
    command_bot.start()
    command_bot.client.run_until_disconnected()


class CommandBotBase:
    class ProcessArgsParser:
        set_alias_flag = "-alias"
        set_cwd_flag = "-cwd"

        def __init__(self) -> None:
            self.args = []
            self.encapsulator = None
            self.setter_flag = None

            self.cwd = os.getcwd()
            self.process_alias = None
        
        def parse_arg(self, arg: str) -> None:
            if arg in [self.set_alias_flag, self.set_cwd_flag]:
                assert not self.setter_flag # No active setter_flag
                self.setter_flag = arg
            elif self.setter_flag:
                if self.setter_flag == self.set_alias_flag:
                    self.process_alias = arg.strip()
                elif self.setter_flag == self.set_cwd_flag:
                    self.cwd = arg.strip()
                
                self.setter_flag = None
            else:
                self.args.append(arg)

                if arg == "py":
                    self.args.append("-u")

        def parse_args(self, args_text: str) -> None:
            arg = ""

            for char in args_text:
                if not self.encapsulator and (char == "'" or char == '"'):
                    self.encapsulator = char
                elif char == self.encapsulator:
                    self.encapsulator = None
                elif char != ' ' or self.encapsulator:
                    arg += char
                elif arg: # Argument found
                    self.parse_arg(arg)
                    arg = "" # Reset
            
            if arg:
                self.parse_arg(arg)

    def __init__(self, client: TelegramClient, bot_token: str, name: str = "CommandBot"):
        self.client = client
        self.bot_token = bot_token
        self.name = name

        # Current command vars
        self.cmd_sender_id = None
        self.cmd_chat_id = None

        # Tracks mapping of process_alias to (process, output_chat_id)
        self.processes = dict[str, tuple[Popen, int]]()

        # Tracks mapping of sender_id to process_alias of active (tracked) processes
        self.active_processes = defaultdict(list[str])

    # Initializer
    def start(self):
        self.client.start(bot_token=self.bot_token)

    # Process handlers
    def pipe_process_outputs(self, process_alias: str):
        process, output_chat_id = self.processes[process_alias]

        for output in process.stdout:
            try:    message = f"<b>Process [{process_alias}]</b>:\n{output.decode('utf-8')}"
            except: message = f"<b>Process [{process_alias}]</b>:\n(undecoded) {output}"
            send_telegram_message(self.bot_token, output_chat_id, message)

        process.wait()
        self.kill_process(process_alias) # Does not invoke Popen.terminate
    
    def pipe_process_errors(self, process_alias: str):
        process, output_chat_id = self.processes[process_alias]

        for output in process.stderr:
            try:    message = f"<b>Process [{process_alias}] Error</b>:\n{output.decode('utf-8')}"
            except: message = f"<b>Process [{process_alias}] Error</b>:\n(undecoded) {output}"

            send_telegram_message(self.bot_token, output_chat_id, message)

    def kill_process(self, process_alias: str):
        if process_alias not in self.processes:
            return

        process, output_chat_id = self.processes[process_alias]
        status = process.poll()

        if status is None: # Busy process
            process.terminate()

        self.processes.pop(process_alias)

        # Remove process_alias from tracked active processes
        for user in self.active_processes.keys():
            if process_alias in self.active_processes[user]:
                self.active_processes[user].remove(process_alias)

        send_telegram_message(
            self.bot_token, output_chat_id,
            f"<b>Process [{process_alias}]</b> closed."
        )

    # Event handlers
    async def event_handler(self, event):
        _, command = str(event.raw_text).split('/', maxsplit=1)
        command = command.strip()
        command_args = None

        sender = await event.get_sender()
        chat = await event.get_chat()
        self.cmd_sender_id = sender.id
        self.cmd_chat_id = chat.id
        
        if ' ' in command:
            # command has args
            command_name, command_args = command.split(' ', maxsplit=1)
            command_name = command_name.strip()
            command_args = command_args.strip()
        else:
            command_name = command

        try: # Execute command wrapper
            if not hasattr(self, command_name):
                await self.echo(f"<b>Error:</b> command [{command_name}] not recognized.")
            elif command_args:
                await getattr(self, command_name)(command_args)
            else:
                await getattr(self, command_name)()
        except Exception as exception:
            await self.echo(f"<b>Exception encountered:</b>\n{exception}")

    async def disconnect(self):
        await self.echo(f"{self.name} disconnected.")
        await self.client.disconnect()

    async def echo(self, message: str) -> None:
        await self.client.send_message(self.cmd_sender_id, message, parse_mode="HTML")

    async def cmd(self, args_text: str = None) -> None:
        if args_text:
            await self.execute(f"cmd.exe {args_text}")
        else:
            await self.execute("cmd.exe")

    async def execute(self, args_text: str) -> None:
        args_parser = CommandBotBase.ProcessArgsParser()
        args_parser.parse_args(args_text)

        process = Popen(args_parser.args, stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=args_parser.cwd)
        process_alias = args_parser.process_alias if args_parser.process_alias else str(process.pid)
        self.processes[process_alias] = (process, self.cmd_chat_id)

        await self.echo(f"<b>Process [{process_alias}]</b> started and active.")

        Thread(target=self.pipe_process_outputs, args=(process_alias,)).start()
        Thread(target=self.pipe_process_errors, args=(process_alias,)).start()

        self.active_processes[self.cmd_sender_id].append(process_alias)

    async def close(self, process_alias: str = None) -> None:
        active_process_alias = self.active_processes[self.cmd_chat_id][-1] \
            if self.active_processes[self.cmd_chat_id] else None

        if not process_alias: # Assign to active process
            process_alias = active_process_alias
        
        if not process_alias:
            await self.echo(f"<b>Error:</b> No active process found.")
            return

        self.kill_process(process_alias)

    async def activate(self, process_alias: str) -> None:
        # Activates the process (sets to current active process)
        if process_alias not in self.processes:
            await self.echo(f"<b>Error:</b> process alias [{process_alias}] not recognized.")
            return
        
        if process_alias in self.active_processes[self.cmd_sender_id]:
            self.active_processes[self.cmd_sender_id].remove(process_alias)

        self.active_processes[self.cmd_sender_id].append(process_alias)

    async def input(self, pipe_input: str) -> None:
        # Pipes the input to the active process
        if not self.active_processes[self.cmd_sender_id]:
            await self.echo(f"<b>Error:</b> No active process found.")
        else:
            process_alias = self.active_processes[self.cmd_sender_id][-1]
            process, _ = self.processes[process_alias]
            process.stdin.write(f"{pipe_input}\r\n".encode("utf-8"))
            process.stdin.flush()

if __name__ == "__main__":
    pass