import json
import requests

from subprocess import Popen, PIPE
from telethon import TelegramClient, events
from threading import Thread

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> None:
    for _ in range(99):
        try:
            return requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": message})
        except:
            pass
    
    requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message})

def manage_process_output(command_bot: "CommandBotBase", process_pid: int) -> None:
    process, chat_id = command_bot.active_processes[process_pid]

    for output in process.stdout:
        send_telegram_message(command_bot.bot_token, chat_id, f"Process [{process_pid}]:\n"
                + output.decode("utf-8"))

    for output in process.stderr:
        send_telegram_message(command_bot.bot_token, chat_id, f"Process [{process_pid}] encountered exception:\n"
                + output.decode("utf-8"))
    
    process.wait()
    command_bot._stop(process_pid)

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
    def __init__(self, client: TelegramClient, bot_token: str, name: str = "CommandBot"):
        self.client = client
        self.bot_token = bot_token
        self.name = name

        # Runtime vars
        self.sender_id = None
        self.chat_id = None
        self.active_processes = dict[int, Popen]()

    def start(self):
        self.client.start(bot_token=self.bot_token)

    async def event_handler(self, event):
        _, command = str(event.raw_text).split('/', maxsplit=1)
        command = command.strip()
        command_args = None

        sender = await event.get_sender()
        chat = await event.get_chat()
        self.sender_id = sender.id
        self.chat_id = chat.id
        
        if ' ' in command:
            # command has args
            command_name, command_args = command.split(' ', maxsplit=1)
            command_name = command_name.strip()
            command_args = command_args.strip()
        else:
            command_name = command

        try: # Execute command wrapper
            if not hasattr(self, command_name):
                await self.echo(f"Command [{command_name}] not recognized.")
            elif command_args:
                await getattr(self, command_name)(command_args)
            else:
                await getattr(self, command_name)()
        except Exception as exception:
            await self.echo(f"Exception encountered:\n{exception}")

    async def disconnect(self):
        await self.echo(f"{self.name} Disconnected.")
        await self.client.disconnect()

    async def echo(self, message: str) -> None:
        await self.client.send_message(self.sender_id, message, parse_mode="HTML")

    # Process methods
    def _stop(self, process_pid: int = None) -> None:
        if not process_pid:
            try:    process_pid = list(self.active_processes.keys())[-1]
            except: return
        
        process_pid = int(process_pid)

        if process_pid not in self.active_processes:
            return
        
        process, chat_id = self.active_processes[process_pid]
        exit_code = process.poll()

        if exit_code is None:
            process.terminate()
            send_telegram_message(self.bot_token, chat_id, f"Process [{process_pid}] terminated.")
        else:
            send_telegram_message(self.bot_token, chat_id, f"Process [{process_pid}] exited with status {exit_code}.")

        self.active_processes.pop(process_pid)

    async def run(self, command_args: str) -> None:
        args, command_arg, encapsulator = [], "", None
        
        for char in command_args:
            if not encapsulator and (char == "'" or char == '"'):
                encapsulator = char
            elif char == encapsulator:
                encapsulator = None
            elif char != ' ' or encapsulator:
                command_arg += char
            elif command_arg:
                args.append(command_arg)
                command_arg = "" # Reset
        
        if command_arg:
            args.append(command_arg)
        
        if args[0] == "py": # Python executable
            args.insert(1, "-u") # Flush prints

        process = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.active_processes[process.pid] = (process, self.chat_id)

        await self.echo(f"Running on subprocess [{process.pid}].")
        Thread(target=manage_process_output, args=(self, process.pid)).start()

    async def stop(self, process_pid: str = None) -> None:
        if not process_pid:
            try:    process_pid = list(self.active_processes.keys())[-1]
            except: return

        self._stop(int(process_pid))

    async def input(self, command_args: str) -> None:
        # command_args format
        #   relay_input subproc_pid (opt)
        if ' ' in command_args:
            relay_input, process_pid = command_args.split(' ', maxsplit=1)

            relay_input = relay_input.strip()
            process_pid = int(process_pid.strip())
        else:
            relay_input = command_args.strip()

            try:    process_pid = list(self.active_processes.keys())[-1]
            except: process_pid = None # Triggers error message

        if process_pid not in self.active_processes:
            await self.echo(f"No active subprocess [{process_pid}] found.")
        else:
            await self.echo(f"Relaying {relay_input} to subprocess [{process_pid}].")
            process, _ = self.active_processes[process_pid]
            process.stdin.write(f"{relay_input}\n".encode("utf-8"))
            process.stdin.flush()

if __name__ == "__main__":
    pass